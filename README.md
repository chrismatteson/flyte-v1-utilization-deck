# Flyte v1 utilization report

Measures how much compute a **Flyte v1** deployment used (OSS Flyte or Union v1), to help estimate what moving to Flyte v2 / Union v2 would cost. It is the v1 version of flyte-sdk's v2 [`utilization_report.py`](https://github.com/flyteorg/flyte-sdk/blob/main/examples/remote_management/utilization_report.py). It outputs the same row format and interactive report, so v1 history and v2 runs can be compared directly.

It uses only stock `flytekit` and the FlyteAdmin API. It does not depend on any Union APIs.

## What it measures

For every execution in the window, it walks the full node tree: subworkflows, branches, dynamic workflows and map-task subtasks. For each node it records:

- **Actions**: one per node execution, plus one per map-task subtask. This is the closest v1 equivalent of a v2 action. Child launch-plan executions count as their own runs and are never counted twice.
- **Requested CPU / memory / GPU (and GPU type)**, resolved the way the v1 platform resolves them at launch: `with_overrides` first, then the task's requests, then its limits, then the platform `TASK_RESOURCE` default for the project/domain. Pod-template tasks add up all their containers.
- **Container run time**, summed across retries. Map subtasks use each subtask's own container start and end times when the API has them.

From these it computes CPU core-hours, memory GiB-hours and GPU-hours, broken down by project, domain, user, execution, task and task type, by day, month or year.

## Usage

```bash
pip install -r requirements.txt

# sweep the whole deployment (last 12 full months + current month)
python v1_utilization_report.py --config ~/.flyte/config.yaml --all

# one project, explicit window
python v1_utilization_report.py --config ~/.flyte/config.yaml --project my-proj \
    --start 2026-01-01 --end 2026-06-30
```

Outputs:
- `v1_usage_report.html`: a standalone interactive report with filters, charts, drill-down and CSV export
- `v1_usage_report.json`: the raw rows
- a monthly summary printed to the terminal

The run saves progress to `<out>.checkpoint.json`. If it's interrupted, re-running the same command resumes from there; `--fresh` starts over. On a test deployment it swept about 43k executions in about 16 minutes with `--workers 16`.

### Viewing the report in the Flyte console

`--publish` registers a small display task and launches it with the swept rows, so the report shows up as a **Deck** on that execution in the console:

```bash
python v1_utilization_report.py --config ~/.flyte/config.yaml --all \
    --publish --publish-project my-proj --publish-domain development

# publish an earlier sweep without re-running it
python v1_utilization_report.py --config ~/.flyte/config.yaml \
    --from-json v1_usage_report.json --publish --publish-project my-proj --publish-domain development
```

The display task uses flytekit's default image for your local Python version. Python 3.13 has no default image, so pass `--image` there (or use Python 3.12).

## Limits

- **Plugin tasks** (spark, ray, dask, pytorch, …): only their container request is metered, not executors or workers. These rows are badged `plugin`.
- **Map subtasks with no recorded container times**: runtime is an estimate, capped at the whole map node's duration, and badged `est. runtime`.
- **Tasks created inside dynamic workflows**: they aren't registered, so their requests fall back to the platform default. Their `with_overrides` resources are not seen.
- **Users**: shown as the execution principal (an OIDC subject or email). There's no name lookup.
- **Requests that are defined nowhere** (no task request and no platform default): these use the editable assumptions in the report (`--default-cpu` / `--default-mem`).
