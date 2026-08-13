"""Derives, from a manifest entry, the DAG the factory is expected to generate.

This is the single place the manifest->DAG contract is written down. The rules below are read
off the factory's `_build_dag()` and the manifest schema in
`etp-etl-core/dags/mtm/mtm_manifest.yaml`. When the deployed factory changes, correct this
file and every assertion in test_dag_factory_alignment.py follows.

Contract
--------
enabled: false      no DAG is built at all
no runtime + no dbt the factory logs a warning and skips the entry -- no DAG
dag_id              used verbatim
schedule            passed straight through
tags                DAG tags; the factory defaults to [dsf, domain, job] when omitted

Task topology
-------------
    ingest                      one runtime  -> a single `ingest` task
    ingest.ingest_<runtime>     several      -> an `ingest` TaskGroup, one task per runtime
    verify_tables.verify_<t>    one BigQueryCheckOperator per verify_tables entry
    backfill_gate               only when incremental: true
    dbt_run, dbt_test           only when run_dbt (defaults to bool(dbt_selector))
    wait_for_<dag_id>           one ExternalTaskSensor per prerequisite_dags entry

Ordering is checked as a sequence of stages rather than exact edges, because TaskGroup-to-
TaskGroup dependencies fan out into many individual edges whose precise shape is an Airflow
implementation detail, not something the manifest specifies.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ExpectedDag:
    """What the factory should produce for one manifest entry."""

    dag_id: str
    schedule: str
    required_tags: frozenset[str]
    tasks: frozenset[str]
    #: Task-id groups that must run in order: every task in stages[i] upstream of stages[i+1].
    stages: tuple[frozenset[str], ...] = ()
    #: Sensor tasks that must be upstream of the DAG's first real stage.
    sensors: frozenset[str] = field(default_factory=frozenset)


def _runtimes(entry: dict) -> list[str]:
    """Multi-runtime entries use `runtimes`; single-runtime ones just `runtime`."""
    runtime = entry.get("runtime")
    return entry.get("runtimes") or ([runtime] if runtime else [])


def builds_a_dag(entry: dict) -> bool:
    """Whether the factory produces a DAG for this entry at all."""
    if not entry.get("enabled", True):
        return False
    dbt_selector = entry.get("dbt_selector")
    run_dbt = entry.get("run_dbt", bool(dbt_selector))
    return bool(_runtimes(entry)) or run_dbt


def expected_dag_for(entry: dict) -> ExpectedDag | None:
    """Build the expected DAG spec for one manifest entry, or None if it is skipped."""
    if not builds_a_dag(entry):
        return None

    domain = entry.get("domain") or entry.get("wave")
    job = entry["job"]
    runtimes = _runtimes(entry)
    dbt_selector = entry.get("dbt_selector")
    run_dbt = entry.get("run_dbt", bool(dbt_selector))

    tasks: set[str] = set()
    stages: list[frozenset[str]] = []

    if runtimes:
        if len(runtimes) == 1:
            ingest = {"ingest"}
        else:
            ingest = {f"ingest.ingest_{rt}" for rt in runtimes}
        tasks |= ingest
        stages.append(frozenset(ingest))

        verify = entry.get("verify_tables") or []
        if verify:
            verify_tasks = {f"verify_tables.verify_{v['table']}" for v in verify}
            tasks |= verify_tasks
            stages.append(frozenset(verify_tasks))

    if entry.get("incremental", False):
        tasks.add("backfill_gate")
        stages.append(frozenset({"backfill_gate"}))

    if run_dbt:
        tasks |= {"dbt_run", "dbt_test"}
        stages.append(frozenset({"dbt_run"}))
        stages.append(frozenset({"dbt_test"}))

    sensors = {f"wait_for_{d}" for d in entry.get("prerequisite_dags") or []}
    tasks |= sensors

    return ExpectedDag(
        dag_id=entry["dag_id"],
        schedule=str(entry["schedule"]),
        required_tags=frozenset(entry.get("tags") or {"dsf", domain, job}),
        tasks=frozenset(tasks),
        stages=tuple(stages),
        sensors=frozenset(sensors),
    )


def expected_dags(manifests: dict[str, dict]) -> dict[str, ExpectedDag]:
    """Expected DAGs across every manifest, keyed by dag_id."""
    result: dict[str, ExpectedDag] = {}
    for manifest in manifests.values():
        for entry in manifest.get("dags") or []:
            spec = expected_dag_for(entry)
            if spec is not None:
                result[spec.dag_id] = spec
    return result


def skipped_dag_ids(manifests: dict[str, dict]) -> set[str]:
    """dag_ids the factory should deliberately NOT build (disabled, or no work to do)."""
    return {
        entry["dag_id"]
        for manifest in manifests.values()
        for entry in manifest.get("dags") or []
        if not builds_a_dag(entry)
    }


def downstream_of(dag, task_id: str) -> set[str]:
    """All task ids reachable downstream of ``task_id``."""
    seen: set[str] = set()
    stack = [task_id]
    while stack:
        current = stack.pop()
        for nxt in dag.get_task(current).downstream_task_ids:
            if nxt not in seen:
                seen.add(nxt)
                stack.append(nxt)
    return seen


def normalise_schedule(dag) -> str | None:
    """Read a DAG's schedule across Airflow 2.x / 3.x attribute renames."""
    for attr in ("schedule_interval", "schedule", "timetable"):
        value = getattr(dag, attr, None)
        if value is None:
            continue
        if attr == "timetable":
            summary = getattr(value, "summary", None)
            if summary:
                return str(summary)
            continue
        return str(value)
    return None
