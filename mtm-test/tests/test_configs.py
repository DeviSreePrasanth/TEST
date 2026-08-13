"""Manifest validation — schema checks that run without Airflow or the factory.

These are the fast checks: they catch a malformed manifest on any PR, before the
factory-alignment job downloads the deployed dag_factory and actually builds the DAGs.

The schema mirrors etp-etl-core/dags/mtm/mtm_manifest.yaml, which is what the factory parses.
"""


import yaml
from dag_expectations import builds_a_dag

from etl_mtm import (
    DATA_DIR,
    DOMAIN,
    MANIFESTS_DIR,
    OWNED_WAVE,
    WAVES,
    configs_dir,
    dbt_dir,
    iter_dag_entries,
    iter_manifests,
)

# Minimum the factory needs from every entry to build anything at all.
REQUIRED_ENTRY_FIELDS = {"dag_id", "job", "schedule"}

# Runtimes seen across the platform. `null` is valid and means dbt-only.
VALID_RUNTIMES = {
    "databricks_mirror",
    "databricks_python",
    "pbi_mirror",
    "s3_mirror",
    "cloud_run_batch",
    None,
}

CRON_FIELDS = 5


def _entries():
    return iter_dag_entries()


def test_manifests_directory_not_empty():
    assert list(MANIFESTS_DIR.glob("*_manifest.yaml")), "No manifests in dags/manifests/"


def test_this_repo_owns_exactly_one_manifest():
    found = iter_manifests()
    assert set(found) == {DOMAIN}, (
        f"etl-mtm should carry exactly the '{DOMAIN}' manifest, found: {sorted(found)}. "
        "Manifests for other domains belong in their own repos."
    )


def test_manifest_uses_the_dags_key():
    """The factory skips any manifest without a top-level `dags:` key, with only a log line."""
    for domain, manifest in iter_manifests().items():
        assert "dags" in manifest, (
            f"{domain}_manifest.yaml has no top-level `dags:` key. The factory logs "
            f"\"missing 'dags' key -- skipping\" and generates nothing. Keys present: "
            f"{sorted(manifest)}"
        )


def test_manifest_declares_at_least_one_entry():
    assert _entries(), f"{DOMAIN}_manifest.yaml declares no DAG entries"


def test_entries_have_required_fields():
    for entry in _entries():
        missing = REQUIRED_ENTRY_FIELDS - entry.keys()
        assert not missing, (
            f"entry '{entry.get('dag_id', '<no dag_id>')}' is missing fields: {missing}"
        )


def test_entries_set_domain_or_wave():
    """The factory raises ValueError when neither is present."""
    for entry in _entries():
        assert entry.get("domain") or entry.get("wave"), (
            f"entry '{entry['dag_id']}' sets neither domain nor wave"
        )


def test_dag_ids_are_unique():
    ids = [e["dag_id"] for e in _entries()]
    duplicates = sorted({i for i in ids if ids.count(i) > 1})
    assert not duplicates, f"duplicate dag_id(s) in the manifest: {duplicates}"


def test_entries_use_valid_runtime():
    for entry in _entries():
        runtimes = entry.get("runtimes") or [entry.get("runtime")]
        for runtime in runtimes:
            assert runtime in VALID_RUNTIMES, (
                f"entry '{entry['dag_id']}' has unknown runtime '{runtime}'; "
                f"expected one of {sorted(str(r) for r in VALID_RUNTIMES)}"
            )


def test_schedules_look_like_cron():
    for entry in _entries():
        schedule = entry["schedule"]
        if schedule is None:
            continue
        assert len(str(schedule).split()) == CRON_FIELDS, (
            f"entry '{entry['dag_id']}' has schedule '{schedule}', which is not a "
            f"{CRON_FIELDS}-field cron expression"
        )


def test_run_dbt_entries_declare_a_selector():
    """The factory raises ValueError for run_dbt=true without dbt_selector."""
    for entry in _entries():
        if entry.get("run_dbt") and not entry.get("dbt_selector"):
            raise AssertionError(
                f"entry '{entry['dag_id']}' sets run_dbt without dbt_selector"
            )


def test_every_entry_produces_work():
    """An entry with no runtime and no dbt is silently skipped by the factory."""
    inert = [e["dag_id"] for e in _entries() if not builds_a_dag(e) and e.get("enabled", True)]
    assert not inert, (
        f"these entries have neither a runtime nor a dbt selector, so the factory skips "
        f"them and no DAG appears: {inert}"
    )


def test_verify_tables_entries_are_well_formed():
    for entry in _entries():
        for table in entry.get("verify_tables") or []:
            missing = {"dataset", "table"} - table.keys()
            assert not missing, (
                f"entry '{entry['dag_id']}' has a verify_tables item missing {missing}"
            )


def test_prerequisite_dags_resolve_within_this_manifest():
    """A prerequisite pointing at a dag_id nobody defines produces a sensor that never clears.

    Cross-domain prerequisites are legitimate, so this only warns for ids outside this
    manifest's own domain prefix.
    """
    known = {e["dag_id"] for e in _entries()}
    for entry in _entries():
        for prereq in entry.get("prerequisite_dags") or []:
            if prereq.startswith(f"dsf_{DOMAIN}_") and prereq not in known:
                raise AssertionError(
                    f"entry '{entry['dag_id']}' waits on '{prereq}', which this manifest "
                    f"does not define. Known: {sorted(known)}"
                )


def _configured_tags(node) -> set[str]:
    """Every value of a `+tags:` key anywhere in the dbt models config tree."""
    tags: set[str] = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "+tags":
                tags |= {value} if isinstance(value, str) else set(value or [])
            else:
                tags |= _configured_tags(value)
    elif isinstance(node, list):
        for item in node:
            tags |= _configured_tags(item)
    return tags


def test_dbt_selectors_match_tags_configured_in_the_dbt_project():
    """`dbt_selector: tag:x` silently runs zero models if nothing carries tag x."""
    project = yaml.safe_load((dbt_dir() / "dbt_project.yml").read_text(encoding="utf-8"))
    configured = _configured_tags(project.get("models", {}))

    for entry in _entries():
        selector = entry.get("dbt_selector")
        if not selector or not selector.startswith("tag:"):
            continue
        tag = selector.split(":", 1)[1]
        assert tag in configured, (
            f"entry '{entry['dag_id']}' selects '{selector}' but no model in "
            f"data/{OWNED_WAVE}/dbt/dbt_project.yml carries tag '{tag}'. dbt would run "
            f"zero models. Tags configured: {sorted(configured)}"
        )


def test_data_dir_contains_only_declared_waves():
    """etl-mtm carries only the wave it deploys into -- currently etp."""
    on_disk = sorted(p.name for p in DATA_DIR.iterdir() if p.is_dir())
    assert on_disk == sorted(WAVES), (
        f"data/ holds {on_disk} but etl_mtm.WAVES declares {sorted(WAVES)}"
    )


def test_coverage_configs_without_manifest_entry(capfd):
    """Non-fatal: warn on source configs no manifest entry references."""
    jobs = {e["job"] for e in _entries()}
    orphaned = sorted(
        p.name for p in configs_dir().glob("*.yaml") if not any(j in p.stem for j in jobs)
    )
    if orphaned:
        print(f"WARNING: configs with no manifest entry (in development?): {orphaned}")
