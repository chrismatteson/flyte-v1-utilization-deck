"""Flyte v1 usage metering with an interactive HTML report.

The v1 (flytekit / FlyteAdmin) counterpart of flyte-sdk's
examples/remote_management/utilization_report.py. It uses only the stock
FlyteAdmin API through flytekit's client, so it works on open-source Flyte v1
and on Union v1 alike.

Sweeps every execution in the window across the selected project/domains,
walks the full node tree of each (subworkflows, branches, dynamic workflows,
map-task subtasks), records requested CPU/memory/GPU and container run time
for every task attempt, and writes a self-contained HTML report in the same
format as the v2 report, so v1 history and v2 runs can be compared directly:

  - filters: date range, project, domain, user, task type
  - group-by: project / domain / user / execution / task / task type, bucketed
    by day / month / year
  - drill-down: group -> executions -> full node tree
  - fanout aggregation: identical leaf nodes under one parent (e.g. map-task
    subtasks) collapse into a single counted row; metrics stay exact

How v1 maps onto the v2 report's terms:
  - run    = a workflow (or single-task) execution. Child launch-plan
             executions are their own runs and are not double counted inside
             their parent.
  - action = a node execution, plus one action per map-task subtask. This is
             the closest v1 equivalent of what v2 counts as an action.

Requested resources are resolved the way flytepropeller does at launch:
node-level overrides (with_overrides) > task template requests > task
template limits (k8s uses the limit as the request when none is set) >
platform TASK_RESOURCE defaults for the project/domain. Pod-template tasks
sum the requests of every container in the pod spec.

Run it (any flytekit config works, including Union v1 configs):
    pip install flytekit
    python v1_utilization_report.py --config ~/.flyte/config.yaml --all
        [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--project P] [--domain D]
        [--out usage_report.html] [--workers 16]

Add --publish to also launch a tiny task on the cluster that shows the report
as a Deck in the console (e.g. to share it); --from-json <rows.json> publishes
an earlier sweep without re-running it.

Default window is the last 12 full months plus the current month. Progress is
checkpointed next to the output file (<out>.checkpoint.json); re-running the
same command resumes from it. Pass --fresh to discard it.
"""

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from flytekit import Deck, Resources, task
from flytekit.types.file import FlyteFile
from google.protobuf.json_format import MessageToDict

EXEC_PAGE = 500  # executions per list page
NODE_PAGE = 500  # node executions per list page
CHECKPOINT_SAVE_INTERVAL = 30.0  # seconds between checkpoint rewrites
RPC_RETRIES = 4

# Task types whose real footprint lives in plugin config (worker groups,
# executors) or outside the cluster. Their container request (if any) is
# still metered; the rest is flagged in the report.
PLUGIN_TASK_TYPES = {
    "spark", "ray", "dask", "mpi", "pytorch", "tensorflow", "databricks",
    "sagemaker_training_job_task", "sagemaker_custom_training_job_task",
}

_MEM_UNITS = {
    "": 1, "k": 10**3, "M": 10**6, "G": 10**9, "T": 10**12, "P": 10**15,
    "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "Pi": 2**50,
}
_EPOCH = "1970-01-01T00:00:00Z"


def parse_cpu(v) -> float:
    if v in (None, ""):
        return 0.0
    v = str(v).strip()
    if v.endswith("m"):
        return float(v[:-1]) / 1000
    return float(v)


def parse_mem_gib(v) -> float:
    if v in (None, ""):
        return 0.0
    m = re.fullmatch(r"([0-9.eE+-]+)\s*([A-Za-z]*)", str(v).strip())
    if not m:
        return 0.0
    return float(m.group(1)) * _MEM_UNITS.get(m.group(2), 1) / 2**30


def parse_secs(d) -> float:
    """protobuf Duration as MessageToDict renders it: '12.5s'."""
    if not d:
        return 0.0
    try:
        return round(float(str(d).rstrip("s")), 3)
    except ValueError:
        return 0.0


def ts_diff(a: str, b: str) -> float:
    try:
        ta = datetime.fromisoformat(a.replace("Z", "+00:00"))
        tb = datetime.fromisoformat(b.replace("Z", "+00:00"))
        return max(0.0, round((tb - ta).total_seconds(), 3))
    except Exception:
        return 0.0


def real_ts(s: str) -> str:
    return "" if not s or s.startswith("1970-") else s


def resolve_window(start: str = "", end: str = ""):
    """Parse YYYY-MM-DD bounds; default = last 12 full months + current month."""
    now = datetime.now(timezone.utc)
    if start:
        start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    else:
        y, m = now.year, now.month - 12
        if m <= 0:
            y, m = y - 1, m + 12
        start_dt = datetime(y, m, 1, tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc) if end else None
    if end_dt is not None and len(end) <= 10:
        end_dt = end_dt.replace(hour=23, minute=59, second=59)
    return start_dt, end_dt


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# resource extraction
# ---------------------------------------------------------------------------

def _resource_list(entries) -> dict:
    """[{name: CPU, value: '1'}, ...] -> {'CPU': '1', ...}"""
    return {e.get("name", ""): e.get("value", "") for e in entries or []}


def _from_k8s_quantities(q: dict) -> dict:
    out = {}
    if "cpu" in q:
        out["CPU"] = q["cpu"]
    if "memory" in q:
        out["MEMORY"] = q["memory"]
    for k, v in q.items():
        # k8s extended name ("nvidia.com/gpu", MIG profiles), and the bare
        # "gpu" that flytekit writes into a pod spec for plugin tasks
        kl = k.lower()
        if kl == "gpu" or kl.endswith("/gpu") or kl.startswith("nvidia.com/mig-"):
            out["GPU"] = v
    return out


def template_resources(tmpl: dict) -> dict:
    """Requested cpu/mem/gpu declared by a compiled task template.

    cpu/mem are None when nothing was declared (the platform default is filled
    in later). `pod` is True when the task runs a pod we can meter.
    """
    out = {"cpu": None, "mem": None, "gpu": 0.0, "gd": "", "pod": False}
    container = tmpl.get("container")
    if tmpl.get("_unregistered"):
        out["pod"] = True
    pod_spec = (tmpl.get("k8sPod") or {}).get("podSpec")
    if container:
        out["pod"] = True
        res = container.get("resources") or {}
        req, lim = _resource_list(res.get("requests")), _resource_list(res.get("limits"))
        _apply(out, req, lim)
    elif pod_spec:
        out["pod"] = True
        tot_cpu = tot_mem = tot_gpu = 0.0
        any_cpu = any_mem = False
        for c in pod_spec.get("containers") or []:
            res = c.get("resources") or {}
            req = _from_k8s_quantities(res.get("requests") or {})
            lim = _from_k8s_quantities(res.get("limits") or {})
            cpu = req.get("CPU", lim.get("CPU"))
            mem = req.get("MEMORY", lim.get("MEMORY"))
            if cpu not in (None, ""):
                tot_cpu += parse_cpu(cpu)
                any_cpu = True
            if mem not in (None, ""):
                tot_mem += parse_mem_gib(mem)
                any_mem = True
            tot_gpu += float(req.get("GPU") or lim.get("GPU") or 0)
        out["cpu"] = tot_cpu if any_cpu else None
        out["mem"] = tot_mem if any_mem else None
        out["gpu"] = tot_gpu
    acc = (tmpl.get("extendedResources") or {}).get("gpuAccelerator") or {}
    out["gd"] = acc.get("device", "")
    return out


def _apply(out: dict, req: dict, lim: dict) -> None:
    """Merge requests (falling back to limits) into out, overriding what is set."""
    cpu = req.get("CPU") or lim.get("CPU")
    mem = req.get("MEMORY") or lim.get("MEMORY")
    gpu = req.get("GPU") or lim.get("GPU")
    if cpu:
        out["cpu"] = parse_cpu(cpu)
    if mem:
        out["mem"] = parse_mem_gib(mem)
    if gpu:
        out["gpu"] = float(gpu)


def apply_overrides(res: dict, overrides: dict | None) -> dict:
    """Layer a TaskNode's with_overrides() resources on top of the template's."""
    if not overrides:
        return res
    res = dict(res)
    r = overrides.get("resources") or {}
    if r:
        _apply(res, _resource_list(r.get("requests")), _resource_list(r.get("limits")))
    acc = (overrides.get("extendedResources") or {}).get("gpuAccelerator") or {}
    if acc.get("device"):
        res["gd"] = acc["device"]
    return res


# ---------------------------------------------------------------------------
# admin client with retries + caches
# ---------------------------------------------------------------------------

class Admin:
    def __init__(self, remote):
        from flytekit.models.core.identifier import Identifier, ResourceType

        self.c = remote.client
        self._Identifier, self._RT = Identifier, ResourceType
        self.tasks: dict[str, dict] = {}
        self.workflows: dict[str, dict | None] = {}
        self.launch_plans: dict[str, object] = {}
        self.lock = threading.Lock()
        self.rpcs = 0

    def call(self, fn, *args, **kwargs):
        from flytekit.exceptions.user import FlyteEntityNotExistException

        delay = 1.0
        for attempt in range(RPC_RETRIES):
            try:
                self.rpcs += 1
                return fn(*args, **kwargs)
            except FlyteEntityNotExistException:
                raise
            except Exception as e:
                if attempt == RPC_RETRIES - 1:
                    raise
                print(f"  ~ retrying {fn.__name__} after {type(e).__name__}", file=sys.stderr)
                time.sleep(delay)
                delay *= 2

    @staticmethod
    def _key(i) -> str:
        return f"{i.project}/{i.domain}/{i.name}/{i.version}"

    def task_template(self, task_id) -> dict:
        k = self._key(task_id)
        if k not in self.tasks:
            try:
                t = self.call(self.c.get_task, task_id)
                d = MessageToDict(t.to_flyte_idl())
                tmpl = d.get("closure", {}).get("compiledTask", {}).get("template", {})
            except Exception:
                # tasks created inside a dynamic workflow are compiled at run
                # time and never registered: meter them as plain container
                # tasks whose request is unknown (platform default / assumed)
                tmpl = {"type": "python-task", "container": {}, "_unregistered": True}
            self.tasks[k] = tmpl
        return self.tasks[k]

    def workflow_nodes(self, launch_plan_id) -> dict:
        """{template id -> {node id -> node spec}} for a launch plan's compiled
        workflow, including subworkflows; {} for single-task executions."""
        if launch_plan_id.resource_type != self._RT.LAUNCH_PLAN:
            return {}
        k = self._key(launch_plan_id)
        if k in self.workflows:
            return self.workflows[k] or {}
        out: dict = {}
        try:
            lp = self.call(self.c.get_launch_plan, launch_plan_id)
            wf = self.call(self.c.get_workflow, lp.spec.workflow_id)
            cw = MessageToDict(wf.to_flyte_idl()).get("closure", {}).get("compiledWorkflow", {})
            for t in cw.get("tasks") or []:
                tmpl = t.get("template") or {}
                tid = tmpl.get("id") or {}
                tk = f"{tid.get('project')}/{tid.get('domain')}/{tid.get('name')}/{tid.get('version')}"
                self.tasks.setdefault(tk, tmpl)
            primary = (cw.get("primary") or {}).get("template") or {}
            out["__primary__"] = _flatten_nodes(primary.get("nodes"))
            for sw in cw.get("subWorkflows") or []:
                tmpl = sw.get("template") or {}
                sid = tmpl.get("id") or {}
                out[f"{sid.get('project')}/{sid.get('domain')}/{sid.get('name')}/{sid.get('version')}"] = (
                    _flatten_nodes(tmpl.get("nodes"))
                )
        except Exception as e:
            print(f"  ! workflow for {k}: {type(e).__name__}", file=sys.stderr)
        self.workflows[k] = out
        return out


def _flatten_nodes(nodes) -> dict:
    """node id -> node spec, including nodes nested in branch cases."""
    out: dict = {}

    def add(n):
        if not n:
            return
        out[n.get("id", "")] = n
        b = (n.get("branchNode") or {}).get("ifElse") or {}
        add((b.get("case") or {}).get("thenNode"))
        for o in b.get("other") or []:
            add(o.get("thenNode"))
        add(b.get("elseNode"))

    for n in nodes or []:
        add(n)
    return out


def _ref_key(r: dict) -> str:
    return f"{r.get('project')}/{r.get('domain')}/{r.get('name')}/{r.get('version')}"


# ---------------------------------------------------------------------------
# listing
# ---------------------------------------------------------------------------

def load_scopes(admin: Admin, project: str = "", domain: str = "") -> list[list[str]]:
    # raw proto call: flytekit's Project model drops each project's domain list
    from flyteidl.admin.project_pb2 import ProjectListRequest

    scopes, token = [], ""
    while True:
        resp = admin.call(admin.c.list_projects, ProjectListRequest(limit=200, token=token))
        for p in resp.projects:
            if project and p.id != project:
                continue
            for d in p.domains:
                if domain and d.id != domain:
                    continue
                scopes.append([p.id, d.id])
        token = resp.token
        if not token:
            break
    return scopes


def platform_defaults(admin: Admin, proj: str, dom: str) -> dict | None:
    """TASK_RESOURCE defaults flytepropeller applies to tasks with no request."""
    from flytekit.models.matchable_resource import MatchableResource

    try:
        a = admin.call(admin.c.get_project_domain_attributes, proj, dom, MatchableResource.TASK_RESOURCE)
        d = MessageToDict(a.to_flyte_idl() if hasattr(a, "to_flyte_idl") else a)
        defaults = (
            d.get("attributes", {}).get("matchingAttributes", {}).get("taskResourceAttributes", {}).get("defaults")
            or {}
        )
    except Exception:
        return None
    if not defaults:
        return None
    return {
        "cpu": parse_cpu(defaults["cpu"]) if defaults.get("cpu") else None,
        "mem": parse_mem_gib(defaults["memory"]) if defaults.get("memory") else None,
    }


def list_executions(admin: Admin, proj: str, dom: str, start_dt, end_dt) -> list[list[str]]:
    from flytekit.models import filters
    from flytekit.models.admin.common import Sort

    f = [filters.GreaterThanOrEqual("created_at", _iso(start_dt))]
    if end_dt is not None:
        f.append(filters.LessThanOrEqual("created_at", _iso(end_dt)))
    out, token, seen = [], None, set()
    while True:
        try:
            execs, token = admin.call(
                admin.c.list_executions_paginated, proj, dom, limit=EXEC_PAGE, token=token, filters=f,
                sort_by=Sort("created_at", Sort.Direction.DESCENDING),
            )
        except Exception as e:
            print(f"  ! listing executions {proj}/{dom}: {type(e).__name__}: {e}", file=sys.stderr)
            break
        for e in execs:
            if e.id.name in seen:
                continue
            seen.add(e.id.name)
            lp = e.spec.launch_plan
            md = e.spec.metadata
            user = (md.principal if md else "") or "unknown"
            parent = ""
            if md and md.parent_node_execution and md.parent_node_execution.execution_id.name:
                parent = md.parent_node_execution.execution_id.name
            out.append([proj, dom, e.id.name, user, int(lp.resource_type), lp.project, lp.domain, lp.name,
                        lp.version, parent])
        if not token:
            break
    return out


# ---------------------------------------------------------------------------
# sweeping one execution
# ---------------------------------------------------------------------------

def sweep_execution(admin: Admin, ex: list, defaults: dict | None) -> list[dict]:
    from flytekit.models.core.identifier import (
        Identifier, NodeExecutionIdentifier, WorkflowExecutionIdentifier,
    )

    proj, dom, name, user, lp_rt, lp_p, lp_d, lp_n, lp_v, _parent = ex
    wid = WorkflowExecutionIdentifier(proj, dom, name)
    spec = admin.workflow_nodes(Identifier(lp_rt, lp_p, lp_d, lp_n, lp_v))
    rows: list[dict] = []

    def base(nid: str, parent: str, st: str, phase: str) -> dict:
        return {
            "pj": proj, "dm": dom, "rn": name, "an": nid, "pa": parent, "at": "TASK", "tt": "", "tn": "",
            "ph": phase, "att": 0, "st": st, "cs": False, "qs": 0.0, "ins": 0.0, "rs": 0.0, "ts": 0.0,
            "cpu": None, "mem": None, "gpu": 0.0, "gd": "", "hc": False, "us": user,
            "dd": False, "ed": False, "pf": False,
        }

    def finish_resources(row: dict, res: dict, tt: str) -> None:
        row["cpu"], row["mem"], row["gpu"], row["gd"] = res["cpu"], res["mem"], res["gpu"], res["gd"]
        if defaults and (row["cpu"] is None or row["mem"] is None):
            if row["cpu"] is None and defaults.get("cpu") is not None:
                row["cpu"], row["dd"] = defaults["cpu"], True
            if row["mem"] is None and defaults.get("mem") is not None:
                row["mem"], row["dd"] = defaults["mem"], True
        row["pf"] = tt in PLUGIN_TASK_TYPES

    def list_nodes(parent_id: str | None) -> list:
        out, token = [], None
        while True:
            kw = {"limit": NODE_PAGE, "token": token}
            if parent_id:
                kw["unique_parent_id"] = parent_id
            nodes, token = admin.call(admin.c.list_node_executions, wid, **kw)
            out.extend(nodes)
            if not token:
                return out

    def task_execs(node_id: str) -> list[dict]:
        out, token = [], None
        while True:
            tes, token = admin.call(
                admin.c.list_task_executions_paginated, NodeExecutionIdentifier(node_id, wid),
                limit=100, token=token,
            )
            out.extend(MessageToDict(t.to_flyte_idl()) for t in tes)
            if not token:
                return out

    def task_id_of(te: dict):
        t = te["id"]["taskId"]
        return Identifier(1, t["project"], t["domain"], t["name"], t["version"])

    def walk(parent_id: str | None, node_specs: dict):
        for n in list_nodes(parent_id):
            d = MessageToDict(n.to_flyte_idl())
            nid = d["id"]["nodeId"]
            md, cl = d.get("metadata") or {}, d.get("closure") or {}
            sid = md.get("specNodeId", nid)
            if sid in ("start-node", "end-node"):
                continue
            ns = node_specs.get(sid) or {}
            phase = str(cl.get("phase", "UNDEFINED"))
            st = real_ts(cl.get("startedAt", "")) or cl.get("createdAt", "")
            row = base(nid, parent_id or "", st, phase)
            row["ts"] = parse_secs(cl.get("duration"))
            tnm = cl.get("taskNodeMetadata") or {}
            row["cs"] = tnm.get("cacheStatus") == "CACHE_HIT"
            row["tn"] = (ns.get("metadata") or {}).get("name", "")
            wnm = cl.get("workflowNodeMetadata") or {}

            if md.get("isArray"):
                rows.extend(array_rows(nid, parent_id or "", row, ns))
                continue
            if wnm.get("executionId"):
                # child launch plan: its own execution, swept on its own
                row.update(at="WORKFLOW", tt="core-launchplan", tn=row["tn"] or wnm["executionId"].get("name", ""))
                rows.append(row)
                continue
            if md.get("isParentNode") and not md.get("isDynamic"):
                if ns.get("branchNode"):
                    row.update(at="CONDITION", tt="core-branch")
                    child_specs = node_specs
                else:
                    row.update(at="WORKFLOW", tt="core-subworkflow")
                    ref = (ns.get("workflowNode") or {}).get("subWorkflowRef")
                    child_specs = spec.get(_ref_key(ref), {}) if ref else {}
                rows.append(row)
                walk(nid, child_specs)
                continue
            if ns.get("gateNode"):
                row.update(at="TASK", tt="core-gate")
                rows.append(row)
                continue

            # a task node (dynamic parents run their own pod too)
            tes = task_execs(nid)
            if not tes:
                row["tt"] = row["tt"] or ("cache" if row["cs"] else "core-node")
                rows.append(row)
                if md.get("isDynamic"):
                    walk(nid, {})
                continue
            tmpl = admin.task_template(task_id_of(tes[0]))
            tt = str(tmpl.get("type", "")) or "task"
            res = apply_overrides(template_resources(tmpl), (ns.get("taskNode") or {}).get("overrides"))
            row["tt"] = tt
            row["tn"] = tes[0]["id"]["taskId"]["name"]
            row["att"] = len(tes)
            row["rs"] = round(sum(parse_secs(te.get("closure", {}).get("duration"))
                                  for te in tes if real_ts(te.get("closure", {}).get("startedAt", ""))), 3)
            row["hc"] = res["pod"] and not row["cs"]
            finish_resources(row, res, tt)
            rows.append(row)
            if md.get("isDynamic"):
                walk(nid, {})

    def array_rows(nid: str, parent: str, row: dict, ns: dict) -> list[dict]:
        """The array node itself, plus one action per subtask attempt."""
        row.update(at="TASK", tt="core-array")
        out = [row]
        tes = task_execs(nid)
        if not tes:
            return out
        inner = (ns.get("arrayNode") or {}).get("node") or {}
        tmpl = admin.task_template(task_id_of(tes[0]))
        tt = str(tmpl.get("type", "")) or "task"
        res = apply_overrides(template_resources(tmpl), (inner.get("taskNode") or {}).get("overrides"))
        tn = tes[0]["id"]["taskId"]["name"].removesuffix("-arraynode")
        row["tn"] = tn
        for te in tes:
            tcl = te.get("closure") or {}
            whole = parse_secs(tcl.get("duration"))
            for er in (tcl.get("metadata") or {}).get("externalResources") or []:
                idx, retry = er.get("index", 0), er.get("retryAttempt", 0)
                sub = base(f"{nid}[{idx}]" + (f".{retry}" if retry else ""), nid,
                           real_ts(tcl.get("startedAt", "")) or row["st"], str(er.get("phase", "UNDEFINED")))
                sub.update(tt=tt, tn=tn, att=retry + 1)
                sub["cs"] = er.get("cacheStatus") == "CACHE_HIT"
                secs, est, first = 0.0, True, ""
                for pod in (er.get("logContext") or {}).get("pods") or []:
                    for c in pod.get("containers") or []:
                        p = c.get("process") or {}
                        s, e = p.get("containerStartTime"), p.get("containerEndTime")
                        if s and e:
                            secs += ts_diff(s, e)
                            est = False
                            first = min(first or s, s)
                if est and not sub["cs"] and er.get("phase") not in ("SKIPPED", "UNDEFINED", "ABORTED"):
                    # no per-subtask container times recorded: bound it by the
                    # whole array node's duration and flag the estimate
                    secs = whole
                sub["rs"] = sub["ts"] = round(secs, 3)
                sub["ed"] = est and secs > 0
                if first:
                    sub["st"] = first
                sub["hc"] = res["pod"] and not sub["cs"]
                finish_resources(sub, res, tt)
                out.append(sub)
        return out

    walk(None, spec.get("__primary__", {}))
    return rows


def aggregate_fanout(out: list[dict]) -> list[dict]:
    """Collapse identical leaf nodes under one parent into a counted row
    (exact metrics, bounded report size). Same rule as the v2 report."""
    parents = {r["pa"] for r in out if r["pa"]}
    kept: list[dict] = []
    groups: dict[tuple, dict] = {}
    for r in out:
        if r["an"] in parents:
            kept.append(r)
            continue
        key = (
            r["pa"], r["at"], r["tt"], r["tn"], r["ph"], r["cs"], r["us"], r["cpu"], r["mem"], r["gpu"],
            r["gd"], r["hc"], r["dd"], r["ed"], r["pf"], r["rs"] > 0, (r["st"] or "")[:10],
        )
        g = groups.get(key)
        if g is None:
            groups[key] = {**r, "cnt": 1}
        else:
            g["cnt"] += 1
            for f in ("qs", "ins", "rs", "ts"):
                g[f] = round(g[f] + r[f], 3)
            g["att"] = max(g["att"], r["att"])
            if r["st"] and (not g["st"] or r["st"] < g["st"]):
                g["st"] = r["st"]
    return kept + list(groups.values())


# ---------------------------------------------------------------------------
# driver
# ---------------------------------------------------------------------------

def collect(admin: Admin, args, checkpoint: str) -> dict:
    start_dt, end_dt = resolve_window(args.start, args.end)
    state: dict = {}
    if checkpoint and os.path.exists(checkpoint) and not args.fresh:
        with open(checkpoint, encoding="utf-8") as fh:
            state = json.load(fh)
        print(f"checkpoint restored: {len(state.get('done', {}))}/{len(state.get('execs', []))} executions done")

    if "execs" not in state:
        scopes = load_scopes(admin, args.project, args.domain)
        print(f"scopes: {len(scopes)} project/domain pairs", flush=True)
        execs, defaults = [], {}
        with ThreadPoolExecutor(args.workers) as pool:
            futs = {pool.submit(list_executions, admin, p, d, start_dt, end_dt): (p, d) for p, d in scopes}
            dfuts = {pool.submit(platform_defaults, admin, p, d): (p, d) for p, d in scopes}
            for f in as_completed(futs):
                p, d = futs[f]
                got = f.result()
                if got:
                    print(f"  {p}/{d}: {len(got)} executions", flush=True)
                execs.extend(got)
            for f in as_completed(dfuts):
                p, d = dfuts[f]
                defaults[f"{p}/{d}"] = f.result()
        state = {"execs": execs, "defaults": defaults, "done": {},
                 "window": [start_dt.date().isoformat(), end_dt.date().isoformat() if end_dt else ""]}
        _save(checkpoint, state)

    execs, done = state["execs"], state["done"]
    todo = [e for e in execs if f"{e[0]}/{e[1]}/{e[2]}" not in done]
    print(f"executions to sweep: {len(todo)} (of {len(execs)})", flush=True)
    lock = threading.Lock()
    last = {"t": time.monotonic(), "n": 0}

    def one(e):
        try:
            rows = aggregate_fanout(sweep_execution(admin, e, state["defaults"].get(f"{e[0]}/{e[1]}")))
        except Exception as err:
            print(f"  ! {e[0]}/{e[1]}/{e[2]}: {type(err).__name__}: {err}", file=sys.stderr)
            return
        with lock:
            done[f"{e[0]}/{e[1]}/{e[2]}"] = rows
            last["n"] += 1
            if last["n"] % 200 == 0:
                print(f"  swept {len(done)}/{len(execs)} executions ({admin.rpcs} RPCs)", flush=True)
            if time.monotonic() - last["t"] >= CHECKPOINT_SAVE_INTERVAL:
                _save(checkpoint, state)
                last["t"] = time.monotonic()

    with ThreadPoolExecutor(args.workers) as pool:
        list(pool.map(one, todo))
    _save(checkpoint, state)

    rows = [r for rs in done.values() for r in rs]
    return {
        "rows": rows,
        "users": {},
        "org": args.org_label,
        "window": state["window"],
        "scope": f"{args.project or 'all projects'}/{args.domain or 'all domains'}",
        "defaults": state["defaults"],
    }


def _save(path: str, state: dict) -> None:
    if not path:
        return
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, separators=(",", ":"))
    os.replace(tmp, path)


def summarize(data: dict, def_cpu: float, def_mem: float) -> None:
    from collections import defaultdict

    tot = defaultdict(float)
    by_month: dict = defaultdict(lambda: defaultdict(float))
    for r in data["rows"]:
        n = r.get("cnt", 1)
        tot["actions"] += n
        m = (r["st"] or "")[:7] or "unknown"
        by_month[m]["actions"] += n
        if not r["hc"] or r["rs"] <= 0:
            continue
        h = r["rs"] / 3600
        cpu = (def_cpu if r["cpu"] is None else r["cpu"]) * h
        mem = (def_mem if r["mem"] is None else r["mem"]) * h
        gpu = (r["gpu"] or 0) * h
        for k, v in (("cpu", cpu), ("mem", mem), ("gpu", gpu), ("hours", h)):
            tot[k] += v
            by_month[m][k] += v
        if r["cpu"] is None or r["mem"] is None:
            tot["assumed_hours"] += h
        if r.get("ed"):
            tot["est_hours"] += h
        if r.get("pf"):
            tot["plugin_hours"] += h
    runs = len({(r["pj"], r["dm"], r["rn"]) for r in data["rows"]})
    print()
    print(f"{'month':<9}{'actions':>12}{'container h':>14}{'CPU core-h':>14}{'mem GiB-h':>14}{'GPU-h':>10}")
    for m in sorted(by_month):
        b = by_month[m]
        print(f"{m:<9}{int(b['actions']):>12,}{b['hours']:>14,.1f}{b['cpu']:>14,.1f}{b['mem']:>14,.1f}{b['gpu']:>10,.1f}")
    print(f"{'TOTAL':<9}{int(tot['actions']):>12,}{tot['hours']:>14,.1f}{tot['cpu']:>14,.1f}{tot['mem']:>14,.1f}{tot['gpu']:>10,.1f}")
    print(f"\n{runs:,} executions. Container hours with assumed requests: {tot['assumed_hours']:,.1f}; "
          f"estimated map-subtask hours: {tot['est_hours']:,.1f}; plugin-task hours (partial): "
          f"{tot['plugin_hours']:,.1f}")


def build_fragment(data: dict) -> str:
    payload = json.dumps(data, separators=(",", ":"))
    return _TEMPLATE.replace("__PAYLOAD__", payload.replace("</", "<\\/"))


def build_html(data: dict) -> str:
    body = build_fragment(data)
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        "<title>Flyte v1 Usage</title>"
        "<style>html,body{margin:0;background:#f9f9f7}"
        "@media (prefers-color-scheme: dark){html,body{background:#0d0d0d}}</style>"
        f"</head><body>{body}</body></html>"
    )


@task(enable_deck=True, deck_fields=(), requests=Resources(cpu="1", mem="2Gi"))
def show_usage_report(rows: FlyteFile) -> str:
    """Render a sweep's rows JSON as a Deck on a Flyte execution.

    The sweep itself runs wherever this script is invoked (it needs admin
    credentials); --publish ships its rows to this task so the report can be
    viewed and shared from the Flyte/Union console.
    """
    with open(rows.download(), encoding="utf-8") as fh:
        data = json.load(fh)
    Deck("Usage report", build_fragment(data))
    n = sum(r.get("cnt", 1) for r in data["rows"])
    runs = len({(r["pj"], r["dm"], r["rn"]) for r in data["rows"]})
    return f"{n} actions across {runs} executions on {data.get('org', '')}, window {data.get('window')}"


def publish(remote, args, rows_path: str) -> None:
    """Fast-register show_usage_report and launch it with the rows JSON."""
    from flytekit.configuration import ImageConfig

    project = args.publish_project or remote.default_project
    domain = args.publish_domain or remote.default_domain
    if not project or not domain:
        sys.exit("--publish needs --publish-project and --publish-domain")
    here = os.path.dirname(os.path.abspath(__file__))
    image = ImageConfig.auto(img_name=args.image) if args.image else ImageConfig.auto_default_image()
    entity = remote.register_script(
        show_usage_report, image_config=image, project=project, domain=domain,
        source_path=here, module_name=os.path.splitext(os.path.basename(__file__))[0],
    )
    ex = remote.execute(entity, inputs={"rows": FlyteFile(rows_path)}, project=project, domain=domain)
    url = remote.generate_console_url(ex)
    print(f"\npublished: {url}\n(the report is the 'Usage report' deck on the show_usage_report node)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--config", default="", help="flytekit config file (default: flytekit's own lookup)")
    ap.add_argument("--start", default="", help="YYYY-MM-DD (default: 12 full months ago)")
    ap.add_argument("--end", default="", help="YYYY-MM-DD (default: now)")
    ap.add_argument("--project", default="")
    ap.add_argument("--domain", default="")
    ap.add_argument("--all", action="store_true", help="sweep every project/domain")
    ap.add_argument("--out", default="v1_usage_report.html")
    ap.add_argument("--workers", type=int, default=16, help="executions swept concurrently")
    ap.add_argument("--console", default="", help="console base URL for execution links (default: admin endpoint)")
    ap.add_argument("--default-cpu", type=float, default=1.0,
                    help="assumed cores when neither task nor platform declares a request")
    ap.add_argument("--default-mem", type=float, default=2.0, help="assumed GiB, same rule")
    ap.add_argument("--fresh", action="store_true", help="ignore any existing checkpoint")
    ap.add_argument("--publish", action="store_true",
                    help="also launch a small Flyte task that shows the report as a Deck in the console")
    ap.add_argument("--publish-project", default="", help="project to launch the --publish task in")
    ap.add_argument("--publish-domain", default="", help="domain to launch the --publish task in")
    ap.add_argument("--image", default="", help="container image for --publish (default: flytekit's default image)")
    ap.add_argument("--from-json", default="", help="skip the sweep; publish/render an existing rows JSON")
    args = ap.parse_args()
    if not args.all and not args.project and not args.from_json:
        ap.error("pass --project (optionally --domain), or --all to sweep every project")

    from flytekit.configuration import Config
    from flytekit.remote import FlyteRemote

    cfg = Config.auto(args.config) if args.config else Config.auto()
    remote = FlyteRemote(config=cfg, default_project=args.publish_project or None,
                         default_domain=args.publish_domain or None)
    if args.from_json:
        if args.publish:
            publish(remote, args, args.from_json)
        return
    endpoint = cfg.platform.endpoint.replace("dns:///", "")
    args.org_label = endpoint
    console = args.console or (("http://" if cfg.platform.insecure else "https://") + endpoint)

    admin = Admin(remote)
    t0 = time.monotonic()
    checkpoint = args.out + ".checkpoint.json"
    data = collect(admin, args, checkpoint)
    data["console"] = console.rstrip("/")
    data["assume"] = [args.default_cpu, args.default_mem]

    base = os.path.splitext(args.out)[0]
    with open(base + ".json", "w", encoding="utf-8") as fh:
        json.dump(data, fh, separators=(",", ":"))
    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write(build_html(data))
    summarize(data, args.default_cpu, args.default_mem)
    print(f"\n{len(data['rows']):,} report rows, {admin.rpcs:,} RPCs, {time.monotonic() - t0:,.0f}s")
    print(f"report: {os.path.abspath(args.out)}\nrows:   {os.path.abspath(base + '.json')}")
    if args.publish:
        publish(remote, args, base + ".json")


_TEMPLATE = r"""
<div id="fup-root">
<style>
#fup-root {
  color-scheme: light;
  --surface: #fcfcfb; --page: #f9f9f7;
  --ink: #0b0b0b; --ink-2: #52514e; --muted: #898781;
  --grid: #e1e0d9; --axis: #c3c2b7; --border: rgba(11,11,11,0.10);
  --s1:#2a78d6; --s2:#eb6834; --s3:#1baf7a; --s4:#eda100;
  --s5:#e87ba4; --s6:#008300; --s7:#4a3aa7; --s8:#e34948;
  --other:#898781;
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
  color: var(--ink); background: var(--page);
  display: block; padding: 20px; box-sizing: border-box;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) #fup-root {
    color-scheme: dark;
    --surface:#1a1a19; --page:#0d0d0d;
    --ink:#ffffff; --ink-2:#c3c2b7; --muted:#898781;
    --grid:#2c2c2a; --axis:#383835; --border: rgba(255,255,255,0.10);
    --s1:#3987e5; --s2:#d95926; --s3:#199e70; --s4:#c98500;
    --s5:#d55181; --s6:#008300; --s7:#9085e9; --s8:#e66767;
  }
}
:root[data-theme="dark"] #fup-root {
  color-scheme: dark;
  --surface:#1a1a19; --page:#0d0d0d;
  --ink:#ffffff; --ink-2:#c3c2b7; --muted:#898781;
  --grid:#2c2c2a; --axis:#383835; --border: rgba(255,255,255,0.10);
  --s1:#3987e5; --s2:#d95926; --s3:#199e70; --s4:#c98500;
  --s5:#d55181; --s6:#008300; --s7:#9085e9; --s8:#e66767;
}
#fup-root * { box-sizing: border-box; }
#fup-root h1 { font-size: 20px; font-weight: 600; margin: 0 0 2px; }
#fup-root .sub { color: var(--ink-2); font-size: 13px; margin-bottom: 16px; }
#fup-root .card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: 10px; padding: 14px 16px; margin-bottom: 14px;
}
#fup-root .rowflex { display: flex; flex-wrap: wrap; gap: 10px 18px; align-items: flex-end; }
#fup-root label.ctl { display: flex; flex-direction: column; gap: 3px; font-size: 12px; color: var(--ink-2); }
#fup-root select, #fup-root input[type=number] {
  font: inherit; font-size: 13px; color: var(--ink);
  background: var(--surface); border: 1px solid var(--axis);
  border-radius: 6px; padding: 4px 8px; min-width: 90px;
}
#fup-root input[type=number] { width: 90px; }
#fup-root .chk { display: flex; align-items: center; gap: 6px; font-size: 12px;
  color: var(--ink-2); padding-bottom: 6px; }
#fup-root .tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 12px; margin-bottom: 14px; }
#fup-root .tile { background: var(--surface); border: 1px solid var(--border);
  border-radius: 10px; padding: 12px 14px; }
#fup-root .tile.click { cursor: pointer; }
#fup-root .tile.click:hover:not(.sel) { border-color: var(--axis); }
#fup-root .tile.sel { border-color: var(--s1); }
#fup-root .tile .lbl { font-size: 12px; color: var(--ink-2); margin-bottom: 4px; }
#fup-root .tile .val { font-size: 26px; font-weight: 600; }
#fup-root .tile .note { font-size: 11px; color: var(--muted); margin-top: 3px; }
#fup-root .cardhead { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 8px; }
#fup-root .cardhead h2 { font-size: 14px; font-weight: 600; margin: 0; }
#fup-root .toggle { font-size: 12px; color: var(--ink-2); background: none;
  border: 1px solid var(--axis); border-radius: 6px; padding: 3px 10px; cursor: pointer; }
#fup-root .legend { display: flex; flex-wrap: wrap; gap: 6px 14px; margin-top: 8px;
  font-size: 12px; color: var(--ink-2); }
#fup-root .legend .it { display: flex; align-items: center; gap: 5px; }
#fup-root .legend .sw { width: 10px; height: 10px; border-radius: 2px; display: inline-block; }
#fup-root table { border-collapse: collapse; width: 100%; font-size: 13px; }
#fup-root th { text-align: right; color: var(--ink-2); font-weight: 500;
  padding: 6px 10px; border-bottom: 1px solid var(--grid); }
#fup-root th:first-child, #fup-root td:first-child { text-align: left; }
#fup-root td { padding: 5px 10px; border-bottom: 1px solid var(--grid);
  text-align: right; font-variant-numeric: tabular-nums; }
#fup-root tr.grp td:first-child { cursor: pointer; }
#fup-root tr.sub td { color: var(--ink-2); font-size: 12.5px; }
#fup-root .caret { display: inline-block; width: 14px; color: var(--muted); }
#fup-root .gname { cursor: pointer; }
#fup-root .gname:hover { text-decoration: underline; }
#fup-root .indent { color: var(--muted); }
#fup-root .badge { font-size: 10.5px; border: 1px solid var(--axis); border-radius: 4px;
  padding: 0 4px; margin-left: 6px; color: var(--muted); }
#fup-root #tooltip {
  position: fixed; pointer-events: none; z-index: 10; display: none;
  background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
  padding: 8px 10px; font-size: 12px; box-shadow: 0 2px 10px rgba(0,0,0,0.18); max-width: 320px;
}
#fup-root #tooltip .tt-title { color: var(--ink-2); margin-bottom: 4px; }
#fup-root #tooltip .tt-row { display: flex; align-items: center; gap: 6px; margin-top: 2px; }
#fup-root #tooltip .tt-key { width: 12px; height: 0; border-top: 3px solid; border-radius: 2px; }
#fup-root #tooltip .tt-val { font-weight: 600; }
#fup-root #tooltip .tt-name { color: var(--ink-2); }
#fup-root svg text { font-family: inherit; }
#fup-root .warn { font-size: 12px; color: var(--ink-2); margin-top: 8px; }
</style>

<div style="display:flex; justify-content:space-between; align-items:flex-start; gap:12px;">
  <div>
    <h1>Flyte v1 usage metering report</h1>
    <div class="sub" id="meta"></div>
  </div>
  <div style="display:flex; gap:8px; flex-shrink:0;">
    <button class="toggle" id="csv-view"
      title="Export the chart and breakdown tables as currently filtered and grouped">Export view CSV</button>
    <button class="toggle" id="csv-full"
      title="Export every collected action row (ignores filters)">Export full CSV</button>
  </div>
</div>

<div class="card">
  <div class="rowflex" id="filters">
    <label class="ctl">Date range <select id="f-date">
      <option value="all">All time</option><option value="7">Last 7 days</option>
      <option value="30">Last 30 days</option><option value="90">Last 90 days</option>
      <option value="mtd">Month to date</option><option value="ytd">Year to date</option>
    </select></label>
    <label class="ctl">From <input type="date" id="f-from"></label>
    <label class="ctl">To <input type="date" id="f-to"></label>
    <label class="ctl">Project <select id="f-project"></select></label>
    <label class="ctl">Domain <select id="f-domain"></select></label>
    <label class="ctl">User <select id="f-user"></select></label>
    <label class="ctl">Action type <select id="f-tt"></select></label>
    <label class="ctl">Group by <select id="v-group">
      <option value="pj">Project</option><option value="dm">Domain</option>
      <option value="us">User</option><option value="rn">Run</option>
      <option value="tn">Task</option><option value="tt">Action type</option>
    </select></label>
    <label class="ctl">Time period <select id="v-bucket">
      <option value="day">Day</option><option value="month" selected>Month</option>
      <option value="year">Year</option>
    </select></label>
    <label class="ctl">Break down by <select id="v-stack">
      <option value="none">Aggregate</option>
      <option value="pj">Project</option><option value="dm">Domain</option>
      <option value="us">User</option><option value="rn">Run</option>
      <option value="tn">Task</option><option value="tt" selected>Action type</option>
      <option value="kind">Execution kind</option><option value="gd">GPU type</option>
    </select></label>
    <label class="ctl">Chart metric <select id="v-metric">
      <option value="cpu">CPU core-hours</option>
      <option value="mem">Memory GiB-hours</option><option value="gpu">GPU-hours</option>
      <option value="act" selected>Actions</option><option value="sec">Container run time</option>
    </select></label>
  </div>
</div>

<div class="card">
  <div class="cardhead"><h2>Assumptions (editable — recompute live)</h2></div>
  <div class="rowflex">
    <label class="ctl">Default CPU (cores)* <input type="number" id="a-cpu" value="1" min="0" step="0.1"></label>
    <label class="ctl">Default memory (GiB)* <input type="number" id="a-mem" value="2" min="0" step="0.25"></label>
  </div>
  <div class="warn">* Applied only to tasks that declared no request <em>and</em> whose
  project/domain has no platform TASK_RESOURCE default (tasks that picked up a platform
  default are badged “platform default” and use it). An action is a node execution, plus one
  per map-task subtask; child launch-plan executions are counted as their own runs.
  Compute metrics accrue only for time a container actually ran (summed across retries).
  Cached results and orchestration nodes (e.g. <span id="np-types"></span>) carry no cpu/mem/gpu.
  Map subtasks without recorded container times are bounded by the whole map node's
  duration and badged “est. runtime”. Plugin task types (spark, ray, dask, …) are metered
  by their container request only and badged “plugin”; executor/worker resources are not included.</div>
</div>

<div class="tiles" id="tiles"></div>

<div class="card">
  <div class="cardhead"><h2 id="chart-title">Usage over time</h2>
    <span style="flex:1; margin-left:10px; font-size:12px; color:var(--muted); align-self:center;">
      click a column to zoom in · click a legend entry to filter or open</span>
    <button class="toggle" id="chart-table-toggle">Table view</button></div>
  <div id="chart"></div>
  <div id="chart-table" style="display:none; overflow-x:auto;"></div>
  <div class="legend" id="legend"></div>
</div>

<div class="card">
  <div class="cardhead"><h2 id="table-title">Breakdown</h2></div>
  <div style="overflow-x:auto"><table id="breakdown"></table></div>
  <div class="warn">Click a row to expand runs; click a run to expand its full
  action tree (sub-actions and traces).</div>
</div>

<div id="tooltip"></div>

<script>
(function(){
const DATA = __PAYLOAD__;
const rows = DATA.rows;
const userNames = DATA.users || {};
const $ = (id) => document.getElementById(id);
if (DATA.assume) { $('a-cpu').value = DATA.assume[0]; $('a-mem').value = DATA.assume[1]; }
const SERIES = ['--s1','--s2','--s3','--s4','--s5','--s6','--s7','--s8'];
const css = (v) => getComputedStyle($('fup-root')).getPropertyValue(v).trim();

const NONPOD = {};
rows.forEach(r => { if (!r.hc) NONPOD[r.tt || r.at.toLowerCase()] = 1; });
$('np-types').textContent = Object.keys(NONPOD).slice(0,4).join(', ') || 'none seen';

const uname = (s) => userNames[s] ? userNames[s] : (s.length > 14 ? s.slice(0,12) + '…' : s);
const GROUP_LABELS = {pj:'Project', dm:'Domain', us:'User', rn:'Run', tn:'Task', tt:'Action type'};
const FILTER_FOR = {pj:'f-project', dm:'f-domain', us:'f-user', tt:'f-tt'};
// last rendered chart/breakdown data, for the "Export view CSV" button
const viewState = {chart: null, breakdown: null};
const gval = (r, dim) => {
  const v = r[dim] || '(unknown)';
  return dim === 'us' ? uname(v) : v;
};

function consoleBase() {
  // the report iframe may be served from a storage origin (e.g. S3), so never
  // use the page's own URL — prefer the embedded console URL, then the
  // embedding page's origin
  if (DATA.console) return DATA.console.replace(/\/$/, '');
  try {
    const ao = window.location.ancestorOrigins;
    if (ao && ao.length) return ao[ao.length - 1];
  } catch (e) {}
  try { if (document.referrer) return new URL(document.referrer).origin; } catch (e) {}
  return '';
}
function runUrl(pj, dm, rn) {
  const path = '/console/projects/' + encodeURIComponent(pj) + '/domains/' +
    encodeURIComponent(dm) + '/executions/' + encodeURIComponent(rn);
  const base = consoleBase();
  return base ? base + path : path;
}

function fillSelect(id, values, all) {
  const sel = $(id); sel.textContent = '';
  const o = document.createElement('option'); o.value = ''; o.textContent = all; sel.appendChild(o);
  values.forEach(v => {
    const op = document.createElement('option'); op.value = v.v; op.textContent = v.t; sel.appendChild(op);
  });
}
const uniq = (dim) => [...new Set(rows.map(r => r[dim] || ''))].filter(Boolean).sort();
fillSelect('f-project', uniq('pj').map(v => ({v, t:v})), 'All projects');
fillSelect('f-domain', uniq('dm').map(v => ({v, t:v})), 'All domains');
fillSelect('f-user', uniq('us').map(v => ({v, t: uname(v)})), 'All users');
fillSelect('f-tt', uniq('tt').map(v => ({v, t:v})), 'All action types');

function assumptions() {
  return {defCpu: +$('a-cpu').value || 0, defMem: +$('a-mem').value || 0};
}

function metrics(r, a) {
  // every action counts toward usage; compute accrues only for container run
  // time. Aggregated fanout rows carry cnt actions and pre-summed durations,
  // so requested-resources x summed seconds stays exact.
  const cnt = r.cnt || 1;
  if (!r.hc || r.rs <= 0) return {sec:0, cpu:0, mem:0, gpu:0, act:cnt, assumed:false};
  const sec = r.rs;
  const cpu = (r.cpu == null ? a.defCpu : r.cpu) * sec / 3600;
  const mem = (r.mem == null ? a.defMem : r.mem) * sec / 3600;
  const gpu = (r.gpu || 0) * sec / 3600;
  return {sec, cpu, mem, gpu, act:cnt, assumed: r.cpu == null || r.mem == null};
}

// action kind, for the Actions chart breakdown
const kindOf = (r) =>
  r.cs ? 'cache hit'
  : r.at === 'TRACE' ? 'trace'
  : r.at === 'CONDITION' ? 'condition'
  : r.tt && r.tt.startsWith('core-') ? 'orchestration'
  : r.rs > 0 ? 'ran container'
  : 'no runtime';

function filtered() {
  const p = $('f-project').value, d = $('f-domain').value,
        u = $('f-user').value, t = $('f-tt').value, dr = $('f-date').value;
  // explicit From/To take precedence over the preset
  let lo = $('f-from').value ? new Date($('f-from').value + 'T00:00:00Z') : null;
  const hi = $('f-to').value ? new Date($('f-to').value + 'T23:59:59.999Z') : null;
  if (!lo && !hi) {
    const now = new Date();
    if (dr === 'mtd') lo = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
    else if (dr === 'ytd') lo = new Date(Date.UTC(now.getUTCFullYear(), 0, 1));
    else if (dr !== 'all') lo = new Date(now.getTime() - (+dr) * 86400e3);
  }
  return rows.filter(r =>
    (!p || r.pj === p) && (!d || r.dm === d) && (!u || r.us === u) &&
    (!t || r.tt === t) &&
    (!lo || (r.st && new Date(r.st) >= lo)) &&
    (!hi || (r.st && new Date(r.st) <= hi)));
}

// click-a-column drill-down: zoom the window to that bucket, one period finer
function zoomTo(x, bucket) {
  let from, to, nb;
  if (bucket === 'year') { from = x + '-01-01'; to = x + '-12-31'; nb = 'month'; }
  else if (bucket === 'month') {
    const [y, m] = x.split('-').map(Number);
    const last = new Date(Date.UTC(y, m, 0)).getUTCDate();
    from = x + '-01'; to = x + '-' + String(last).padStart(2, '0'); nb = 'day';
  } else { from = x; to = x; nb = 'day'; }
  $('f-from').value = from; $('f-to').value = to; $('v-bucket').value = nb;
  render();
}

const fmtH = (v) => v >= 100 ? v.toFixed(0) : v >= 1 ? v.toFixed(2) : v.toFixed(3);
const fmtDur = (s) => s >= 3600 ? (s/3600).toFixed(1) + ' h' : s >= 60 ? (s/60).toFixed(1) + ' m' : s.toFixed(1) + ' s';

function bucketOf(st, mode) {
  if (!st) return '(no start)';
  if (mode === 'day') return st.slice(0, 10);
  if (mode === 'month') return st.slice(0, 7);
  return st.slice(0, 4);
}

function render() {
  const a = assumptions();
  const rs = filtered();
  const dim = $('v-group').value, bucket = $('v-bucket').value, metric = $('v-metric').value;

  // totals + tiles
  let tot = {cpu:0, mem:0, gpu:0, sec:0, act:0, compute:0, assumed:0};
  rs.forEach(r => {
    const m = metrics(r, a);
    tot.cpu += m.cpu; tot.mem += m.mem; tot.gpu += m.gpu; tot.sec += m.sec; tot.act += m.act;
    if (m.sec > 0) { tot.compute += m.act; if (m.assumed) tot.assumed += m.act; }
  });
  const tiles = [
    {l:'Actions', v: tot.act.toLocaleString(), m:'act',
     n: tot.compute + ' with compute · ' + (tot.act - tot.compute) + ' without compute'},
    {l:'CPU core-hours', v: fmtH(tot.cpu), m:'cpu', n: 'requested cores \u00d7 run time'},
    {l:'Memory GiB-hours', v: fmtH(tot.mem), m:'mem', n: 'requested GiB \u00d7 run time'},
    {l:'GPU-hours', v: fmtH(tot.gpu), m:'gpu', n: 'requested GPUs \u00d7 run time'},
    {l:'Container run time', v: fmtDur(tot.sec), m:'sec', n: tot.assumed + ' actions using assumed defaults'},
  ];
  const tl = $('tiles'); tl.textContent = '';
  tiles.forEach(t => {
    const d = document.createElement('div');
    d.className = 'tile' + (t.m ? ' click' : '') + (t.m === metric ? ' sel' : '');
    const l = document.createElement('div'); l.className = 'lbl'; l.textContent = t.l;
    const v = document.createElement('div'); v.className = 'val'; v.textContent = t.v;
    d.appendChild(l); d.appendChild(v);
    if (t.n) { const n = document.createElement('div'); n.className = 'note'; n.textContent = t.n; d.appendChild(n); }
    if (t.m) d.addEventListener('click', () => {
      $('v-metric').value = t.m; render();
    });
    tl.appendChild(d);
  });

  // grouped aggregates
  const groups = new Map();
  rs.forEach(r => {
    const g = gval(r, dim);
    if (!groups.has(g)) groups.set(g, {cpu:0, mem:0, gpu:0, act:0, n:0, sec:0, rows:[], raw: r[dim] || ''});
    const o = groups.get(g), m = metrics(r, a);
    o.cpu += m.cpu; o.mem += m.mem; o.gpu += m.gpu; o.act += m.act;
    o.sec += m.sec; o.n += m.act; o.rows.push(r);
  });
  const sorted = [...groups.entries()].sort((x, y) => y[1][metric] - x[1][metric]);

  renderChart(rs, sorted, dim, bucket, metric, a);
  renderTable(sorted, dim, metric, a, tot);
}

// ---------- chart ----------
let chartTableMode = false;
$('chart-table-toggle').addEventListener('click', () => {
  chartTableMode = !chartTableMode;
  $('chart').style.display = chartTableMode ? 'none' : '';
  $('chart-table').style.display = chartTableMode ? '' : 'none';
  $('chart-table-toggle').textContent = chartTableMode ? 'Chart view' : 'Table view';
});

const METRIC_LABEL = {cpu:'CPU core-hours', mem:'Memory GiB-hours', gpu:'GPU-hours',
  act:'Actions', sec:'Container run-time hours'};
const STACK_LABELS = {none:'', pj:'project', dm:'domain', us:'user', rn:'run',
  tn:'task', tt:'action type', kind:'execution kind', gd:'GPU type'};

function renderChart(rs, sorted, dim, bucket, metric, a) {
  const stack = $('v-stack').value;
  const stackKey =
    stack === 'none' ? (() => METRIC_LABEL[metric])
    : stack === 'kind' ? kindOf
    : stack === 'gd' ? ((r) => r.gd || (r.gpu > 0 ? 'unspecified' : 'no gpu'))
    : ((r) => gval(r, stack));
  const fmtV = metric === 'act' ? ((v) => Math.round(v).toLocaleString()) : fmtH;
  const valOf = (m) => metric === 'sec' ? m.sec / 3600 : m[metric];

  // series = top stack keys by metric total across the filtered rows
  const keyTotals = new Map();
  rs.forEach(r => {
    const m = metrics(r, a), v = valOf(m);
    if (metric !== 'act' && v <= 0) return;
    const k = stackKey(r);
    keyTotals.set(k, (keyTotals.get(k) || 0) + v);
  });
  const ranked = [...keyTotals.entries()].sort((x, y) => y[1] - x[1]).map(e => e[0]);
  const top = ranked.slice(0, 7);
  const useOther = ranked.length > 7;
  const names = useOther ? top.concat(['Other']) : top;
  const colors = names.map((n, i) => n === 'Other' && useOther ? css('--other') : css(SERIES[i]));

  const buckets = new Map();
  rs.forEach(r => {
    const m = metrics(r, a), v = valOf(m);
    if (metric !== 'act' && v <= 0) return;
    const b = bucketOf(r.st, bucket);
    if (!buckets.has(b)) buckets.set(b, {});
    const g0 = stackKey(r);
    const g = (useOther && !top.includes(g0)) ? 'Other' : g0;
    buckets.get(b)[g] = (buckets.get(b)[g] || 0) + v;
  });
  const xs = [...buckets.keys()].sort();

  $('chart-title').textContent = METRIC_LABEL[metric] +
    (stack === 'none' ? '' : ' by ' + STACK_LABELS[stack]) + ' per ' + bucket;
  viewState.chart = {
    title: $('chart-title').textContent,
    bucketLabel: bucket === 'day' ? 'Day' : bucket === 'month' ? 'Month' : 'Year',
    names, xs, buckets,
  };

  // legend — entries are clickable: runs open in the console, filterable
  // dimensions apply that filter
  const lg = $('legend'); lg.textContent = '';
  if (names.length > 1) names.forEach((n, i) => {
    const it = document.createElement('span'); it.className = 'it';
    const sw = document.createElement('span'); sw.className = 'sw'; sw.style.background = colors[i];
    it.appendChild(sw); it.appendChild(document.createTextNode(n));
    if (n !== 'Other') {
      if (stack === 'rn') {
        const r0 = rs.find(r => r.rn === n);
        if (r0) {
          it.classList.add('gname'); it.title = 'open run in the console';
          it.addEventListener('click', () => window.open(runUrl(r0.pj, r0.dm, r0.rn), '_blank'));
        }
      } else if (FILTER_FOR[stack]) {
        const r0 = rs.find(r => gval(r, stack) === n);
        if (r0 && r0[stack]) {
          it.classList.add('gname'); it.title = 'filter on ' + n;
          it.addEventListener('click', () => { $(FILTER_FOR[stack]).value = r0[stack]; render(); });
        }
      }
    }
    lg.appendChild(it);
  });

  // chart table twin
  const ct = $('chart-table'); ct.textContent = '';
  const t = document.createElement('table');
  const hr = document.createElement('tr');
  [bucket === 'day' ? 'Day' : bucket === 'month' ? 'Month' : 'Year', ...names].forEach(h => {
    const th = document.createElement('th'); th.textContent = h; hr.appendChild(th);
  });
  t.appendChild(hr);
  xs.forEach(x => {
    const tr = document.createElement('tr');
    const td0 = document.createElement('td'); td0.textContent = x; tr.appendChild(td0);
    names.forEach(n => {
      const td = document.createElement('td');
      td.textContent = fmtV(buckets.get(x)[n] || 0);
      tr.appendChild(td);
    });
    t.appendChild(tr);
  });
  ct.appendChild(t);

  // svg
  const host = $('chart'); host.textContent = '';
  const W = Math.max(host.clientWidth || 800, 320), H = 260;
  const M = {t: 10, r: 10, b: 26, l: 56};
  const pw = W - M.l - M.r, ph = H - M.t - M.b;
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('width', W); svg.setAttribute('height', H);

  const totals = xs.map(x => names.reduce((s, n) => s + (buckets.get(x)[n] || 0), 0));
  const maxV = Math.max(...totals, 1e-9);
  // nice ticks
  const step0 = maxV / 4, pow = Math.pow(10, Math.floor(Math.log10(step0)));
  const step = [1, 2, 5, 10].map(m => m * pow).find(s => s >= step0);
  const yMax = Math.ceil(maxV / step) * step;
  const y = (v) => M.t + ph - (v / yMax) * ph;

  for (let v = 0; v <= yMax + 1e-9; v += step) {
    const ln = document.createElementNS(svg.namespaceURI, 'line');
    ln.setAttribute('x1', M.l); ln.setAttribute('x2', W - M.r);
    ln.setAttribute('y1', y(v)); ln.setAttribute('y2', y(v));
    ln.setAttribute('stroke', v === 0 ? css('--axis') : css('--grid'));
    ln.setAttribute('stroke-width', '1'); svg.appendChild(ln);
    const tx = document.createElementNS(svg.namespaceURI, 'text');
    tx.setAttribute('x', M.l - 6); tx.setAttribute('y', y(v) + 4);
    tx.setAttribute('text-anchor', 'end'); tx.setAttribute('font-size', '11');
    tx.setAttribute('fill', css('--muted'));
    tx.textContent = v >= 1 || v === 0 ? (+v.toFixed(2)).toLocaleString() : v.toFixed(3);
    svg.appendChild(tx);
  }

  const band = pw / Math.max(xs.length, 1);
  const bw = Math.min(24, band * 0.6);
  const GAP = 2;

  xs.forEach((x, xi) => {
    const cx = M.l + band * xi + band / 2;
    let acc = 0;
    const segs = [];
    names.forEach((n, si) => {
      const v = buckets.get(x)[n] || 0;
      if (v <= 0) return;
      segs.push({n, si, v, y0: acc, y1: acc + v});
      acc += v;
    });
    segs.forEach((s, i) => {
      const topSeg = i === segs.length - 1;
      let yTop = y(s.y1), yBot = y(s.y0);
      if (i > 0) yBot -= GAP / 2;
      if (!topSeg) yTop += GAP / 2;
      const h = Math.max(yBot - yTop, 1);
      const xL = cx - bw / 2;
      let el;
      if (topSeg && h > 4) {
        el = document.createElementNS(svg.namespaceURI, 'path');
        const r = 4;
        el.setAttribute('d',
          'M' + xL + ',' + (yTop + h) + ' V' + (yTop + r) +
          ' Q' + xL + ',' + yTop + ' ' + (xL + r) + ',' + yTop +
          ' H' + (xL + bw - r) +
          ' Q' + (xL + bw) + ',' + yTop + ' ' + (xL + bw) + ',' + (yTop + r) +
          ' V' + (yTop + h) + ' Z');
      } else {
        el = document.createElementNS(svg.namespaceURI, 'rect');
        el.setAttribute('x', xL); el.setAttribute('y', yTop);
        el.setAttribute('width', bw); el.setAttribute('height', h);
      }
      el.setAttribute('fill', colors[s.si]);
      el.setAttribute('data-col', xi);
      svg.appendChild(el);
    });

    // x labels (thin out when crowded)
    const every = Math.ceil(xs.length / Math.max(Math.floor(pw / 70), 1));
    if (xi % every === 0) {
      const tx = document.createElementNS(svg.namespaceURI, 'text');
      tx.setAttribute('x', cx); tx.setAttribute('y', H - 8);
      tx.setAttribute('text-anchor', 'middle'); tx.setAttribute('font-size', '11');
      tx.setAttribute('fill', css('--muted')); tx.textContent = x;
      svg.appendChild(tx);
    }

    // column hit target (bigger than the marks)
    const hit = document.createElementNS(svg.namespaceURI, 'rect');
    hit.setAttribute('x', M.l + band * xi); hit.setAttribute('y', M.t);
    hit.setAttribute('width', band); hit.setAttribute('height', ph);
    hit.setAttribute('fill', 'transparent');
    hit.addEventListener('pointermove', (ev) => {
      svg.querySelectorAll('[data-col]').forEach(e =>
        e.setAttribute('opacity', e.getAttribute('data-col') == xi ? '1' : '0.55'));
      showTip(ev, x, segs.slice().reverse().map(s =>
        ({name: s.n, color: colors[s.si], val: fmtV(s.v)})));
    });
    hit.addEventListener('pointerleave', () => {
      svg.querySelectorAll('[data-col]').forEach(e => e.setAttribute('opacity', '1'));
      hideTip();
    });
    hit.style.cursor = 'pointer';
    hit.addEventListener('click', () => { hideTip(); zoomTo(x, bucket); });
    svg.appendChild(hit);
  });
  host.appendChild(svg);
}

function showTip(ev, title, items) {
  const tip = $('tooltip'); tip.textContent = '';
  const t = document.createElement('div'); t.className = 'tt-title'; t.textContent = title;
  tip.appendChild(t);
  items.forEach(it => {
    const r = document.createElement('div'); r.className = 'tt-row';
    const k = document.createElement('span'); k.className = 'tt-key'; k.style.borderTopColor = it.color;
    const v = document.createElement('span'); v.className = 'tt-val'; v.textContent = it.val;
    const n = document.createElement('span'); n.className = 'tt-name'; n.textContent = it.name;
    r.appendChild(k); r.appendChild(v); r.appendChild(n); tip.appendChild(r);
  });
  tip.style.display = 'block';
  const pad = 14;
  let x = ev.clientX + pad, ypx = ev.clientY + pad;
  if (x + tip.offsetWidth > window.innerWidth - 8) x = ev.clientX - tip.offsetWidth - pad;
  if (ypx + tip.offsetHeight > window.innerHeight - 8) ypx = ev.clientY - tip.offsetHeight - pad;
  tip.style.left = x + 'px'; tip.style.top = ypx + 'px';
}
function hideTip() { $('tooltip').style.display = 'none'; }

// ---------- breakdown table with drill-down ----------
function cellRow(cells, cls) {
  const tr = document.createElement('tr'); if (cls) tr.className = cls;
  cells.forEach((c, i) => {
    const td = document.createElement('td');
    if (c instanceof Node) td.appendChild(c); else td.textContent = c;
    tr.appendChild(td);
  });
  return tr;
}

function renderTable(sorted, dim, metric, a, tot) {
  viewState.breakdown = {dim, metric, sorted, tot};
  $('table-title').textContent = 'Breakdown by ' + GROUP_LABELS[dim].toLowerCase();
  const tbl = $('breakdown'); tbl.textContent = '';
  const hr = document.createElement('tr');
  [GROUP_LABELS[dim], 'Actions', 'Metered time', 'CPU core-h', 'Mem GiB-h', 'GPU-h',
   '% of ' + METRIC_LABEL[metric]].forEach(h => {
    const th = document.createElement('th'); th.textContent = h; hr.appendChild(th);
  });
  tbl.appendChild(hr);

  sorted.forEach(([g, o]) => {
    const nameCell = document.createElement('span');
    const caret = document.createElement('span'); caret.className = 'caret'; caret.textContent = '▸';
    nameCell.appendChild(caret);
    const nm = document.createElement('span'); nm.textContent = g;
    // clicking the name filters on it (or opens the run in the console UI);
    // clicking anywhere else on the row expands it
    if (dim === 'rn' && o.rows.length) {
      nm.className = 'gname';
      nm.title = 'open run in the console';
      nm.addEventListener('click', (ev) => {
        ev.stopPropagation();
        window.open(runUrl(o.rows[0].pj, o.rows[0].dm, o.rows[0].rn), '_blank');
      });
    } else if (FILTER_FOR[dim] && o.raw) {
      nm.className = 'gname';
      nm.title = 'filter on ' + g;
      nm.addEventListener('click', (ev) => {
        ev.stopPropagation();
        $(FILTER_FOR[dim]).value = o.raw; render();
      });
    }
    nameCell.appendChild(nm);
    const tr = cellRow([nameCell, o.n, fmtDur(o.sec), fmtH(o.cpu), fmtH(o.mem), fmtH(o.gpu),
      (tot[metric] > 0 ? (100 * o[metric] / tot[metric]).toFixed(1) : '0.0') + '%'], 'grp');
    tbl.appendChild(tr);
    let open = false, subRows = [];
    tr.addEventListener('click', () => {
      open = !open; caret.textContent = open ? '▾' : '▸';
      if (!open) { subRows.forEach(r => r.remove()); subRows = []; return; }
      subRows = renderRuns(tbl, tr, o.rows, a, metric);
    });
  });

  const totTr = cellRow(['Total', sorted.reduce((s, e) => s + e[1].n, 0), fmtDur(tot.sec),
    fmtH(tot.cpu), fmtH(tot.mem), fmtH(tot.gpu), '100%']);
  totTr.style.fontWeight = '600';
  tbl.appendChild(totTr);
}

function renderRuns(tbl, afterTr, groupRows, a, metric) {
  const byRun = new Map();
  groupRows.forEach(r => {
    const key = r.pj + '/' + r.dm + '/' + r.rn;
    if (!byRun.has(key)) byRun.set(key, {cpu:0, mem:0, gpu:0, act:0, sec:0, n:0, rows:[], rn:r.rn});
    const o = byRun.get(key), m = metrics(r, a);
    o.cpu += m.cpu; o.mem += m.mem; o.gpu += m.gpu; o.act += m.act; o.sec += m.sec; o.n += m.act; o.rows.push(r);
  });
  const runs = [...byRun.entries()].sort((x, y) => y[1][metric] - x[1][metric]).slice(0, 50);
  const made = [];
  let anchor = afterTr;
  runs.forEach(([key, o]) => {
    const nameCell = document.createElement('span');
    const caret = document.createElement('span'); caret.className = 'caret'; caret.textContent = '▸';
    const ind = document.createElement('span'); ind.className = 'indent'; ind.textContent = '\u00a0\u00a0\u00a0';
    nameCell.appendChild(ind); nameCell.appendChild(caret);
    nameCell.appendChild(document.createTextNode(key + ' '));
    const lk = document.createElement('span'); lk.className = 'gname'; lk.textContent = '↗';
    lk.title = 'open run in the console';
    lk.addEventListener('click', (ev) => {
      ev.stopPropagation();
      const r0 = o.rows[0];
      window.open(runUrl(r0.pj, r0.dm, r0.rn), '_blank');
    });
    nameCell.appendChild(lk);
    const tr = cellRow([nameCell, o.n, fmtDur(o.sec), fmtH(o.cpu), fmtH(o.mem), fmtH(o.gpu), ''], 'sub grp');
    anchor.after(tr); anchor = tr; made.push(tr);
    let open = false, treeRows = [];
    tr.addEventListener('click', (ev) => {
      ev.stopPropagation();
      open = !open; caret.textContent = open ? '▾' : '▸';
      if (!open) { treeRows.forEach(r => r.remove()); treeRows = []; return; }
      treeRows = renderTree(tr, o.rows, a);
      made.push(...treeRows);
    });
  });
  if (byRun.size > 50) {
    const more = '\u00a0\u00a0\u00a0… ' + (byRun.size - 50) + ' more runs (filter to narrow)';
    const tr = cellRow([more, '', '', '', '', '', ''], 'sub');
    anchor.after(tr); made.push(tr);
  }
  return made;
}

function renderTree(afterTr, runRows, a) {
  // parent/child tree: roots have no parent or parent outside this run
  const byName = new Map(runRows.map(r => [r.an, r]));
  const children = new Map();
  const roots = [];
  runRows.forEach(r => {
    if (r.pa && byName.has(r.pa)) {
      if (!children.has(r.pa)) children.set(r.pa, []);
      children.get(r.pa).push(r);
    } else roots.push(r);
  });
  const made = [];
  let anchor = afterTr;
  const emit = (r, depth) => {
    const m = metrics(r, a);
    const nameCell = document.createElement('span');
    const ind = document.createElement('span'); ind.className = 'indent';
    ind.textContent = '\u00a0'.repeat(6 + depth * 3) + (depth >= 0 ? '└ ' : '');
    nameCell.appendChild(ind);
    nameCell.appendChild(document.createTextNode(r.an + (r.tn ? ' · ' + r.tn : '')));
    const badge = document.createElement('span'); badge.className = 'badge';
    badge.textContent = r.at === 'TRACE' ? 'trace' : (r.tt || r.at.toLowerCase());
    nameCell.appendChild(badge);
    if (r.cnt > 1) {
      const bc = document.createElement('span'); bc.className = 'badge';
      bc.textContent = '\u00d7' + r.cnt.toLocaleString();
      bc.title = r.cnt.toLocaleString() + ' identical leaf actions aggregated';
      nameCell.appendChild(bc);
    }
    if (r.hc && (r.cpu == null || r.mem == null)) {
      const b2 = document.createElement('span'); b2.className = 'badge'; b2.textContent = 'assumed req';
      nameCell.appendChild(b2);
    }
    [[r.dd, 'platform default'], [r.ed, 'est. runtime'], [r.pf, 'plugin']].forEach(([on, txt]) => {
      if (!on) return;
      const b3 = document.createElement('span'); b3.className = 'badge'; b3.textContent = txt;
      nameCell.appendChild(b3);
    });
    const tr = cellRow([nameCell, r.att > 1 ? r.att + ' tries' : '', fmtDur(m.sec || r.ts),
      r.hc ? fmtH(m.cpu) : '—', r.hc ? fmtH(m.mem) : '—', r.gpu ? fmtH(m.gpu) : '—',
      r.ph.toLowerCase()], 'sub');
    anchor.after(tr); anchor = tr; made.push(tr);
    (children.get(r.an) || []).sort((x, y) => (x.st || '').localeCompare(y.st || ''))
      .forEach(c => emit(c, depth + 1));
  };
  roots.sort((x, y) => (x.st || '').localeCompare(y.st || '')).forEach(r => emit(r, 0));
  return made;
}

// ---------- CSV export ----------
const csvCell = (v) => {
  const s = String(v == null ? '' : v);
  return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
};
const toCsv = (table) => table.map(r => r.map(csvCell).join(',')).join('\n') + '\n';
function downloadCsv(name, text) {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([text], {type: 'text/csv'}));
  a.download = name;
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

// every collected action row, unfiltered; computed hours use the current
// assumption inputs
$('csv-full').addEventListener('click', () => {
  const a = assumptions();
  const out = [[
    'project','domain','run','action','count','parent','action_type','task_type',
    'task_name','phase','attempts','start_time','cache_hit','execution_kind',
    'user_id','user_name','queued_s','initializing_s','running_s','total_s',
    'requested_cpu','requested_mem_gib','requested_gpu','gpu_device',
    'ran_container','assumed_defaults','cpu_core_hours','mem_gib_hours','gpu_hours',
    'platform_default_request','estimated_runtime','plugin_task',
  ]];
  rows.forEach(r => {
    const m = metrics(r, a);
    out.push([
      r.pj, r.dm, r.rn, r.an, r.cnt || 1, r.pa || '', r.at, r.tt || '', r.tn || '',
      r.ph, r.att, r.st, r.cs ? 1 : 0, kindOf(r),
      r.us, userNames[r.us] || '', r.qs, r.ins, r.rs, r.ts,
      r.cpu == null ? '' : r.cpu, r.mem == null ? '' : r.mem, r.gpu || 0, r.gd || '',
      r.hc ? 1 : 0, m.assumed && m.sec > 0 ? 1 : 0,
      m.cpu.toFixed(6), m.mem.toFixed(6), m.gpu.toFixed(6),
      r.dd ? 1 : 0, r.ed ? 1 : 0, r.pf ? 1 : 0,
    ]);
  });
  downloadCsv('flyte-v1-usage-full.csv', toCsv(out));
});

// the chart pivot and breakdown table exactly as currently rendered
// (filters, grouping, stacking, and assumptions applied)
$('csv-view').addEventListener('click', () => {
  const c = viewState.chart, b = viewState.breakdown;
  if (!c || !b) return;
  const out = [['# ' + c.title]];
  out.push([c.bucketLabel, ...c.names]);
  c.xs.forEach(x => out.push([x, ...c.names.map(n => c.buckets.get(x)[n] || 0)]));
  out.push([]);
  out.push(['# Breakdown by ' + GROUP_LABELS[b.dim].toLowerCase()]);
  out.push([GROUP_LABELS[b.dim], 'actions', 'metered_time_s', 'cpu_core_hours',
    'mem_gib_hours', 'gpu_hours', 'pct_of_' + b.metric]);
  b.sorted.forEach(([g, o]) => out.push([
    g, o.n, o.sec.toFixed(3), o.cpu.toFixed(6), o.mem.toFixed(6), o.gpu.toFixed(6),
    b.tot[b.metric] > 0 ? (100 * o[b.metric] / b.tot[b.metric]).toFixed(1) : '0.0',
  ]));
  out.push(['Total', b.sorted.reduce((s, e) => s + e[1].n, 0), b.tot.sec.toFixed(3),
    b.tot.cpu.toFixed(6), b.tot.mem.toFixed(6), b.tot.gpu.toFixed(6), '100.0']);
  downloadCsv('flyte-v1-usage-view.csv', toCsv(out));
});

// meta line
(function(){
  const n = rows.reduce((s, r) => s + (r.cnt || 1), 0),
        runs = new Set(rows.map(r => r.pj + '/' + r.dm + '/' + r.rn)).size;
  const w = DATA.window || ['', ''];
  $('meta').textContent = DATA.org + ' · scope ' + (DATA.scope || 'all') +
    ' · window ' + (w[0] || 'beginning') + ' → ' + (w[1] || 'now') +
    ' — ' + n + ' actions across ' + runs + ' runs';
})();

['f-date','f-from','f-to','f-project','f-domain','f-user','f-tt',
 'v-group','v-bucket','v-metric','v-stack',
 'a-cpu','a-mem'].forEach(id =>
  $(id).addEventListener(id.startsWith('a-') ? 'input' : 'change', render));
// choosing a preset clears any explicit From/To
$('f-date').addEventListener('change', () => {
  $('f-from').value = ''; $('f-to').value = ''; render();
});

// The console may lay this report out after the script runs (hidden tab,
// late-attached iframe) — render defensively and re-render when the root
// first gets real dimensions or changes width.
function safeRender() { try { render(); } catch (e) { console.error('render:', e); } }
let lastW = -1;
if (typeof ResizeObserver !== 'undefined') {
  new ResizeObserver(() => {
    const w = $('fup-root').clientWidth;
    if (w !== lastW) { lastW = w; safeRender(); }
  }).observe($('fup-root'));
} else {
  window.addEventListener('resize', safeRender);
}
window.addEventListener('load', safeRender);
setTimeout(safeRender, 400);
setTimeout(safeRender, 1500);
safeRender();
})();
</script>
</div>
"""


if __name__ == "__main__":
    main()
