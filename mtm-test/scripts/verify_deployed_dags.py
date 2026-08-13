#!/usr/bin/env python
"""Verify what is actually deployed to the Composer environment.

Two halves, because they answer different questions:

  --gcs      Are the *inputs* in the bucket? The factory under plugins/, a loader under
             dags/, and MTM's manifest under dags/manifests/.
  --airflow  Do the *generated* DAGs exist in the environment, with the right schedules?

Why both: with a factory pattern the bucket's dags/ folder never contains one .py per DAG.
DAGs are built in memory at parse time, so listing GCS can only confirm the ingredients are
there -- it cannot tell you a DAG was generated. Only Airflow can, hence --airflow.

    python scripts/verify_deployed_dags.py --gcs
    python scripts/verify_deployed_dags.py --airflow --env composer-udp-env --location us-central1

Requires gcloud/gsutil authenticated against the target project. Exit codes: 0 ok, 1 problems
found, 2 tooling or auth unavailable.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "tests"))

from dag_expectations import expected_dags  # noqa: E402

from etl_mtm import iter_manifests  # noqa: E402

DEFAULT_BUCKET = "elc-composer-udp-env-dev"
DEFAULT_PROJECT = "eo-dev-comp-orch-gl-2540"

#: The manifest this repo is responsible for putting in the bucket.
REQUIRED_MANIFEST = "dags/manifests/mtm_manifest.yaml"

#: Filename the factory is deployed under, wherever it lands.
FACTORY_FILENAME = "dag_factory.py"


def _run(cmd: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _check_airflowignore(bucket: str, factory_paths: list[str]) -> list[str]:
    """Warn if an .airflowignore excludes the folder the factory lives in.

    Only relevant when the factory sits under dags/: teams routinely add helper folders such
    as `templates/` to .airflowignore to stop them being parsed as DAG files. Do that and the
    factory is skipped by the DagBag -- no error, no DAGs, nothing in the UI. A factory in
    plugins/ is unaffected, since plugins/ is never parsed for DAGs in the first place.
    """
    code, out, _ = _run(["gsutil", "cat", f"gs://{bucket}/dags/.airflowignore"])
    if code != 0:
        return []  # no .airflowignore, nothing to exclude anything

    patterns = [
        line.strip()
        for line in out.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    if not patterns:
        return []

    print(f"  NOTE     gs://{bucket}/dags/.airflowignore contains: {patterns}")
    hits = [
        (path, pattern)
        for path in factory_paths
        for pattern in patterns
        if pattern.strip("/") in path
    ]
    for path, pattern in hits:
        print(f"  PROBLEM  '{pattern}' appears to exclude {path} from DAG parsing.")
        print("           Airflow will skip the factory silently -- no error, and zero DAGs.")

    return [f".airflowignore excludes {p}" for p, _ in hits]


def _report_factory_placement(bucket: str, objects: set[str]) -> list[str]:
    """Locate dag_factory.py in the bucket and judge whether it can actually produce DAGs.

    Placement decides behaviour, so this does not assume a folder:

    plugins/...      Expected. Synced to $AIRFLOW_HOME/plugins and importable, but *never
                     scanned for DAGs* -- a loader .py under dags/ must call it, or the
                     factory is inert in Composer. CI validation is unaffected either way,
                     since it imports the factory directly.
    dags/...         Parsed directly by Airflow, so no loader is needed -- provided
                     .airflowignore does not exclude the folder.
    absent           Either installed as a Composer PyPI dependency instead (check with
                     `gcloud composer environments describe`), or genuinely missing.
    """
    problems: list[str] = []
    found = sorted(o for o in objects if o.rsplit("/", 1)[-1] == FACTORY_FILENAME)

    if not found:
        print(f"  MISSING  no {FACTORY_FILENAME} anywhere under gs://{bucket}/")
        print("           If it is installed as a Composer PyPI dependency instead, check:")
        print("             gcloud composer environments describe <env> --location <loc> \\")
        print("               --format='value(config.softwareConfig.pypiPackages)'")
        return [FACTORY_FILENAME]

    for path in found:
        print(f"  OK       gs://{bucket}/{path}")

    in_dags = [p for p in found if p.startswith("dags/")]
    in_plugins = [p for p in found if p.startswith("plugins/")]

    if in_dags:
        print("           under dags/ -- Airflow scans recursively, so this is parsed directly")
        print("           and needs no separate loader. DAGs appear as long as the factory")
        print("           builds them into module globals at import time.")
        problems += _check_airflowignore(bucket, in_dags)
        return problems

    if in_plugins:
        # Any .py under dags/ can be the loader, at any depth (e.g. dags/mtm/loader.py).
        loaders = sorted(o for o in objects if o.startswith("dags/") and o.endswith(".py"))
        if loaders:
            print(f"  OK       DAG loader(s) under dags/: {loaders}")
            print("           Confirm one of these imports the factory and emits its DAGs.")
        else:
            print(f"  PROBLEM  factory is in plugins/ but there is no .py under gs://{bucket}/dags/")
            print("           plugins/ is importable but never scanned for DAGs, so the factory")
            print("           is inert and zero DAGs are generated. A loader under dags/ that")
            print("           calls it is required.")
            problems.append("dags/**/*.py (loader)")

    return problems


def check_gcs(bucket: str) -> int:
    if shutil.which("gsutil") is None:
        print("ERROR: gsutil not on PATH. Install the Google Cloud SDK.")
        return 2

    code, out, err = _run(["gsutil", "ls", "-r", f"gs://{bucket}/**"])
    if code != 0:
        print(f"ERROR: could not list gs://{bucket}/ -- {err}")
        return 2

    objects = {
        line.split(f"gs://{bucket}/", 1)[-1]
        for line in out.splitlines()
        if line.strip() and not line.rstrip().endswith(":")
    }

    problems = _report_factory_placement(bucket, objects)

    status = "OK     " if REQUIRED_MANIFEST in objects else "MISSING"
    print(f"  {status}  gs://{bucket}/{REQUIRED_MANIFEST}")
    if REQUIRED_MANIFEST not in objects:
        problems.append(REQUIRED_MANIFEST)

    expected = expected_dags(iter_manifests())
    print(f"\n  Manifests here declare {len(expected)} DAG(s): {sorted(expected)}")
    print("  Presence in GCS does not prove they were generated -- run with --airflow.")
    print("\n  To validate the deployed factory against these manifests:")
    print(f"    gsutil cp gs://{bucket}/<path>/{FACTORY_FILENAME} /tmp/")
    print(f"    DAG_FACTORY_PATH=/tmp/{FACTORY_FILENAME} python scripts/validate_dag_factory.py")

    return 1 if problems else 0


def check_airflow(env: str, location: str, project: str) -> int:
    if shutil.which("gcloud") is None:
        print("ERROR: gcloud not on PATH. Install the Google Cloud SDK.")
        return 2

    code, out, err = _run(
        [
            "gcloud", "composer", "environments", "run", env,
            "--location", location,
            "--project", project,
            "dags", "list", "--", "--output", "json",
        ]
    )
    if code != 0:
        print(f"ERROR: `gcloud composer environments run` failed -- {err}")
        return 2

    # The wrapper prints connection noise before the payload; take the JSON array.
    start = out.find("[")
    if start == -1:
        print(f"ERROR: could not parse a DAG list from the output:\n{out}")
        return 2

    try:
        listed = json.loads(out[start:])
    except json.JSONDecodeError as exc:
        print(f"ERROR: could not parse DAG list JSON -- {exc}")
        return 2

    deployed = {d["dag_id"]: d for d in listed}
    expected = expected_dags(iter_manifests())

    missing = sorted(set(expected) - set(deployed))
    problems = list(missing)

    for dag_id in sorted(expected):
        if dag_id in missing:
            print(f"  MISSING  {dag_id}  (manifest declares it; Airflow has not generated it)")
            continue

        entry = deployed[dag_id]
        paused = str(entry.get("paused", "")).lower() == "true"
        print(f"  OK       {dag_id}  (paused={paused}, file={entry.get('fileloc', '?')})")
        if paused:
            print("             note: DAG exists but is paused, so it will not run")

    # Other domains' DAGs share this environment, so extras are expected, not errors.
    others = sorted(set(deployed) - set(expected))
    if others:
        print(f"\n  {len(others)} other DAG(s) in this environment (other domains): {others}")

    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gcs", action="store_true", help="Check bucket inputs")
    parser.add_argument("--airflow", action="store_true", help="Check generated DAGs")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--env", help="Composer environment name (required for --airflow)")
    parser.add_argument("--location", default="us-central1")
    args = parser.parse_args()

    if not args.gcs and not args.airflow:
        parser.error("pass --gcs and/or --airflow")

    status = 0
    if args.gcs:
        print(f"== Bucket inputs: gs://{args.bucket} ==")
        status |= check_gcs(args.bucket)
    if args.airflow:
        if not args.env:
            parser.error("--airflow requires --env")
        print(f"\n== Generated DAGs: {args.env} ({args.location}) ==")
        status |= check_airflow(args.env, args.location, args.project)

    return status


if __name__ == "__main__":
    raise SystemExit(main())
