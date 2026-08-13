"""DAG Factory <-> manifest alignment.

Answers the questions the validation exists to answer, against the factory that is actually
deployed in the Composer bucket (CI downloads it into dags/templates/ before running this):

1. Does the factory pick up this repo's manifest?  -> test_factory_produced_any_dags
2. Is a DAG generated for each entry?              -> test_no_missing_dags
3. Is each DAG correct per its entry?              -> schedule / tags / tasks / ordering
4. Are missing, extra or wrong DAGs identified?    -> test_no_extra_dags plus the per-DAG
                                                      diffs in every failure message

These skip when Airflow or the factory is absent, so a plain checkout stays green. CI supplies
both, so CI enforces them.
"""

from __future__ import annotations

import pytest
from dag_expectations import (
    downstream_of,
    expected_dags,
    normalise_schedule,
    skipped_dag_ids,
)

from etl_mtm import iter_manifests

MANIFESTS = iter_manifests()
EXPECTED = expected_dags(MANIFESTS)
SKIPPED = skipped_dag_ids(MANIFESTS)

EXPECTED_ITEMS = sorted(EXPECTED.items())


def _ids(items):
    return [dag_id for dag_id, _ in items]


# --- Coverage ---------------------------------------------------------------------------


def test_manifests_yield_at_least_one_expected_dag():
    """Guard the guard: an empty expectation set would make every check below vacuous."""
    assert EXPECTED, (
        "No expected DAGs derived from dags/manifests/. Either the manifest is empty or it "
        "is missing the top-level `dags:` key the factory requires."
    )


def test_factory_produced_any_dags(generated_dags):
    """The headline check: the deployed factory found and parsed our manifest."""
    assert generated_dags, (
        "The factory produced no DAGs at all. It loaded, but discovered no manifest -- "
        "check that dags/manifests/*_manifest.yaml is where the factory looks, and that it "
        "uses the `dags:` key."
    )


def test_no_missing_dags(generated_dags):
    missing = sorted(set(EXPECTED) - set(generated_dags))
    assert not missing, (
        f"The factory generated no DAG for these manifest entries: {missing}. "
        f"Generated: {sorted(generated_dags)}"
    )


def test_no_extra_dags(generated_dags):
    extra = sorted(set(generated_dags) - set(EXPECTED))
    assert not extra, (
        f"The factory generated DAGs with no backing manifest entry: {extra}. "
        f"Expected: {sorted(EXPECTED)}"
    )


def test_disabled_entries_produce_no_dag(generated_dags):
    """`enabled: false`, or an entry with neither a runtime nor dbt, must build nothing."""
    leaked = sorted(SKIPPED & set(generated_dags))
    assert not leaked, f"These entries should not have produced a DAG: {leaked}"


# --- Per-DAG correctness ----------------------------------------------------------------


@pytest.mark.parametrize(("dag_id", "spec"), EXPECTED_ITEMS, ids=_ids(EXPECTED_ITEMS))
def test_dag_generated_per_entry(generated_dags, dag_id, spec):
    assert dag_id in generated_dags, f"No DAG with dag_id '{dag_id}'"


@pytest.mark.parametrize(("dag_id", "spec"), EXPECTED_ITEMS, ids=_ids(EXPECTED_ITEMS))
def test_schedule_matches_manifest(generated_dags, dag_id, spec):
    dag = generated_dags.get(dag_id)
    if dag is None:
        pytest.skip("covered by test_no_missing_dags")

    actual = normalise_schedule(dag)
    assert actual == spec.schedule, (
        f"{dag_id}: manifest schedule is '{spec.schedule}' but the DAG has '{actual}'"
    )


@pytest.mark.parametrize(("dag_id", "spec"), EXPECTED_ITEMS, ids=_ids(EXPECTED_ITEMS))
def test_tags_match_manifest(generated_dags, dag_id, spec):
    dag = generated_dags.get(dag_id)
    if dag is None:
        pytest.skip("covered by test_no_missing_dags")

    missing = sorted(spec.required_tags - set(dag.tags or []))
    assert not missing, f"{dag_id}: DAG tags {sorted(dag.tags or [])} are missing {missing}"


@pytest.mark.parametrize(("dag_id", "spec"), EXPECTED_ITEMS, ids=_ids(EXPECTED_ITEMS))
def test_tasks_match_manifest(generated_dags, dag_id, spec):
    dag = generated_dags.get(dag_id)
    if dag is None:
        pytest.skip("covered by test_no_missing_dags")

    actual = {task.task_id for task in dag.tasks}
    missing = sorted(spec.tasks - actual)
    extra = sorted(actual - spec.tasks)
    assert not missing and not extra, (
        f"{dag_id}: task mismatch. Missing: {missing or 'none'}. Extra: {extra or 'none'}. "
        f"Expected {sorted(spec.tasks)}, got {sorted(actual)}"
    )


@pytest.mark.parametrize(("dag_id", "spec"), EXPECTED_ITEMS, ids=_ids(EXPECTED_ITEMS))
def test_stages_run_in_order(generated_dags, dag_id, spec):
    """Each stage must be upstream of the next: ingest -> verify -> dbt_run -> dbt_test."""
    dag = generated_dags.get(dag_id)
    if dag is None:
        pytest.skip("covered by test_no_missing_dags")

    task_ids = {task.task_id for task in dag.tasks}

    for upstream_stage, downstream_stage in zip(spec.stages, spec.stages[1:], strict=False):
        for upstream in sorted(upstream_stage):
            if upstream not in task_ids:
                continue  # reported by test_tasks_match_manifest
            reachable = downstream_of(dag, upstream)
            unreachable = sorted(downstream_stage - reachable)
            assert not unreachable, (
                f"{dag_id}: '{upstream}' should run before {unreachable}, but they are not "
                f"downstream of it"
            )


@pytest.mark.parametrize(("dag_id", "spec"), EXPECTED_ITEMS, ids=_ids(EXPECTED_ITEMS))
def test_prerequisite_sensors_gate_the_dag(generated_dags, dag_id, spec):
    """Each prerequisite_dags entry becomes a sensor upstream of the real work."""
    dag = generated_dags.get(dag_id)
    if dag is None or not spec.sensors:
        pytest.skip("no prerequisite_dags on this entry")

    first_stage = spec.stages[0] if spec.stages else frozenset()
    for sensor in sorted(spec.sensors):
        if sensor not in {t.task_id for t in dag.tasks}:
            continue  # reported by test_tasks_match_manifest
        reachable = downstream_of(dag, sensor)
        assert first_stage & reachable, (
            f"{dag_id}: sensor '{sensor}' does not gate {sorted(first_stage)} -- the DAG "
            f"would start before its prerequisite finished"
        )


@pytest.mark.parametrize(("dag_id", "spec"), EXPECTED_ITEMS, ids=_ids(EXPECTED_ITEMS))
def test_dag_has_no_cycles(generated_dags, dag_id, spec):
    """A DAG that parses but cycles fails at scheduling time, not parse time."""
    dag = generated_dags.get(dag_id)
    if dag is None:
        pytest.skip("covered by test_no_missing_dags")

    from airflow.utils.dag_cycle_tester import check_cycle

    check_cycle(dag)
