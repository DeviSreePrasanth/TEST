#!/usr/bin/env python
"""Report how the deployed DAG Factory aligns with this repo's manifests.

Same checks as tests/test_dag_factory_alignment.py, but emits a readable report instead of
pytest output -- intended for CI job summaries and for eyeballing during a factory upgrade.

    python scripts/validate_dag_factory.py
    python scripts/validate_dag_factory.py --factory /path/to/dag_factory.py --json

Exit codes: 0 aligned, 1 drift detected, 2 factory or Airflow unavailable.
"""

from __future__ import annotations

import argparse
import importlib.util
import inspect
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "tests"))

from airflow_env import configure, explain_factory_error  # noqa: E402
from dag_expectations import (  # noqa: E402
    downstream_of,
    expected_dags,
    normalise_schedule,
)

from etl_mtm import MANIFESTS_DIR, ROOT, iter_manifests  # noqa: E402

# Must run before anything imports airflow. Shared with tests/conftest.py -- without it the
# factory's Variable.get() lookups hit a metadata database that does not exist here.
configure()

CANDIDATE_ENTRYPOINTS = (
    "build_dags",
    "generate_dags",
    "create_dags",
    "load_dags",
    "make_dags",
    "build",
)


def load_factory(path: Path):
    if not path.is_file():
        return None
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[path.stem] = module
    spec.loader.exec_module(module)
    return module


def collect_dags(module, entrypoint: str | None):
    from airflow.models import DAG

    def normalise(obj):
        if isinstance(obj, DAG):
            return {obj.dag_id: obj}
        if isinstance(obj, dict):
            return {d.dag_id: d for d in obj.values() if isinstance(d, DAG)}
        if isinstance(obj, (list, tuple, set)):
            return {d.dag_id: d for d in obj if isinstance(d, DAG)}
        return {}

    for name in [entrypoint] if entrypoint else CANDIDATE_ENTRYPOINTS:
        func = getattr(module, name, None)
        if not callable(func):
            continue
        params = inspect.signature(func).parameters
        required = [
            p
            for p in params.values()
            if p.default is inspect.Parameter.empty
            and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
        dags = normalise(func(MANIFESTS_DIR) if required else func())
        if dags:
            return dags, name

    globals_dags = {v.dag_id: v for v in vars(module).values() if isinstance(v, DAG)}
    return globals_dags, "<module globals>"


def diff_dag(spec, dag) -> dict:
    """Return only the fields that disagree with the manifest."""
    problems = {}

    schedule = normalise_schedule(dag)
    if schedule != spec.schedule:
        problems["schedule"] = {"expected": spec.schedule, "actual": schedule}

    missing_tags = sorted(spec.required_tags - set(dag.tags or []))
    if missing_tags:
        problems["tags"] = {"missing": missing_tags, "actual": sorted(dag.tags or [])}

    tasks = {t.task_id for t in dag.tasks}
    if tasks != set(spec.tasks):
        problems["tasks"] = {
            "missing": sorted(set(spec.tasks) - tasks),
            "extra": sorted(tasks - set(spec.tasks)),
        }

    # Stage ordering, rather than exact edges: TaskGroup-to-TaskGroup dependencies fan out
    # into many edges whose precise shape is an Airflow detail the manifest never specifies.
    out_of_order = []
    for upstream_stage, downstream_stage in zip(spec.stages, spec.stages[1:], strict=False):
        for upstream in sorted(upstream_stage):
            if upstream not in tasks:
                continue
            unreachable = sorted(downstream_stage - downstream_of(dag, upstream))
            if unreachable:
                out_of_order.append({"after": upstream, "not_downstream": unreachable})
    if out_of_order:
        problems["ordering"] = out_of_order

    for sensor in sorted(spec.sensors):
        if sensor in tasks and spec.stages and not (spec.stages[0] & downstream_of(dag, sensor)):
            problems.setdefault("sensors", []).append(
                {"sensor": sensor, "does_not_gate": sorted(spec.stages[0])}
            )

    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--factory",
        type=Path,
        default=Path(
            os.environ.get("DAG_FACTORY_PATH", ROOT / "plugins" / "dag_factory.py")
        ),
        help="Path to dag_factory.py (default: plugins/dag_factory.py)",
    )
    parser.add_argument(
        "--entrypoint",
        default=os.environ.get("DAG_FACTORY_ENTRYPOINT"),
        help="Factory callable to invoke (default: probe common names)",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = parser.parse_args()

    try:
        import airflow  # noqa: F401
    except ImportError:
        print("ERROR: Airflow is not installed. Install with: pip install -e '.[validation]'")
        return 2

    try:
        # A globals-style factory builds every DAG during import, so failures surface here.
        module = load_factory(args.factory)
    except Exception as exc:  # noqa: BLE001 - reported with context instead of a traceback
        print(f"ERROR: the factory raised while loading from {args.factory}.\n")
        print(explain_factory_error(exc))
        return 2

    if module is None:
        print(f"ERROR: DAG Factory not found at {args.factory}")
        print("It is deployed by the platform repo's pipeline; pass --factory to override.")
        return 2

    expected = expected_dags(iter_manifests())
    try:
        generated, entrypoint = collect_dags(module, args.entrypoint)
    except Exception as exc:  # noqa: BLE001 - reported with context instead of a traceback
        print(f"ERROR: the factory raised while building DAGs from {MANIFESTS_DIR}.\n")
        print(explain_factory_error(exc))
        return 2

    missing = sorted(set(expected) - set(generated))
    extra = sorted(set(generated) - set(expected))
    incorrect = {
        dag_id: problems
        for dag_id, spec in sorted(expected.items())
        if dag_id in generated and (problems := diff_dag(spec, generated[dag_id]))
    }
    aligned = sorted(
        dag_id for dag_id in expected if dag_id in generated and dag_id not in incorrect
    )

    report = {
        "factory": str(args.factory),
        "entrypoint": entrypoint,
        "manifests_dir": str(MANIFESTS_DIR),
        "expected": sorted(expected),
        "generated": sorted(generated),
        "aligned": aligned,
        "missing": missing,
        "extra": extra,
        "incorrect": incorrect,
    }

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"DAG Factory : {args.factory}")
        print(f"Entry point : {entrypoint}")
        print(f"Manifests   : {MANIFESTS_DIR}")
        print(f"Expected {len(expected)} DAG(s), factory generated {len(generated)}\n")

        for dag_id in aligned:
            print(f"  OK       {dag_id}")
        for dag_id in missing:
            print(f"  MISSING  {dag_id}  (manifest job produced no DAG)")
        for dag_id in extra:
            print(f"  EXTRA    {dag_id}  (DAG has no backing manifest job)")
        for dag_id, problems in incorrect.items():
            print(f"  WRONG    {dag_id}")
            for field, detail in problems.items():
                print(f"             {field}: {detail}")

        verdict = "ALIGNED" if not (missing or extra or incorrect) else "DRIFT DETECTED"
        print(f"\n{verdict}")

    return 0 if not (missing or extra or incorrect) else 1


if __name__ == "__main__":
    raise SystemExit(main())
