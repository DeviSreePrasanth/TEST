"""Fixtures for DAG Factory alignment validation.

The factory itself is not vendored in this repo -- it is deployed to ``plugins/`` by the
platform repo's own CI/CD pipeline. These fixtures locate it, invoke it against this repo's
manifests, and hand the resulting DAGs to the tests.

When the factory (or Airflow) is absent, every dependent test *skips* rather than fails, so a
domain-only checkout still gets a green run. CI installs both and therefore actually enforces
the checks -- see the ``dag-factory-alignment`` job in validate-dags.yml.

Discovery
---------
The factory module is looked up in this order:

1. ``$DAG_FACTORY_PATH``             -- explicit path to the .py file
2. ``plugins/dag_factory.py``        -- where the platform pipeline deploys it in the bucket
3. ``dags/templates/dag_factory.py`` -- alternative placement, kept as a fallback
4. ``import dag_factory``            -- if installed as a Composer PyPI dependency

CI downloads the deployed factory into ``plugins/`` before running, so the checkout mirrors
the bucket and any path the factory resolves relative to its own file lands where it would
in Composer.

Entry point
-----------
``$DAG_FACTORY_ENTRYPOINT`` names the callable to use. If unset, the adapter probes the
conventional names below, then falls back to scanning module globals for DAG objects (the
shape an Airflow ``dags/`` loader file produces).
"""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import os
import sys
from pathlib import Path

import pytest
from airflow_env import configure, explain_factory_error

from etl_mtm import DAGS_DIR, MANIFESTS_DIR, PLUGINS_DIR

# Must run before anything imports airflow. Shared with scripts/validate_dag_factory.py so
# the two cannot drift -- they did once, and the tests passed while the script crashed on a
# missing metadata database.
configure()

#: Callables tried, in order, when DAG_FACTORY_ENTRYPOINT is not set.
CANDIDATE_ENTRYPOINTS = (
    "build_dags",
    "generate_dags",
    "create_dags",
    "load_dags",
    "make_dags",
    "build",
)

#: Where the platform pipeline deploys the factory: gs://<bucket>/plugins/dag_factory.py.
#: Composer syncs that folder to $AIRFLOW_HOME/plugins and puts it on sys.path.
DEFAULT_FACTORY_PATH = PLUGINS_DIR / "dag_factory.py"

#: Checked second, for deployments that place it under the dags/ tree instead.
FALLBACK_FACTORY_PATH = DAGS_DIR / "templates" / "dag_factory.py"


def _import_factory():
    """Import the dag_factory module, or return None if it cannot be found."""
    explicit = os.environ.get("DAG_FACTORY_PATH")
    candidates = (
        [Path(explicit)] if explicit else [DEFAULT_FACTORY_PATH, FALLBACK_FACTORY_PATH]
    )

    for path in candidates:
        if path.is_file():
            # plugins/ on sys.path mirrors how Airflow exposes the plugins folder, so a
            # factory that imports sibling modules still resolves.
            parent = str(path.parent)
            if parent not in sys.path:
                sys.path.insert(0, parent)
            spec = importlib.util.spec_from_file_location(path.stem, path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[path.stem] = module
            try:
                # A globals-style factory builds every DAG here, at import time, so this is
                # where most real failures surface.
                spec.loader.exec_module(module)
            except Exception as exc:  # noqa: BLE001 - re-raised with context below
                pytest.fail(
                    f"The deployed dag_factory raised while building DAGs from "
                    f"{MANIFESTS_DIR}.\n\n{explain_factory_error(exc)}"
                )
            return module

    try:
        return importlib.import_module("dag_factory")
    except ImportError:
        return None


def _collect_dags(obj):
    """Normalise whatever the factory returned into {dag_id: DAG}."""
    from airflow.models import DAG

    if obj is None:
        return {}
    if isinstance(obj, DAG):
        return {obj.dag_id: obj}
    if isinstance(obj, dict):
        return {d.dag_id: d for d in obj.values() if isinstance(d, DAG)}
    if isinstance(obj, (list, tuple, set)):
        return {d.dag_id: d for d in obj if isinstance(d, DAG)}
    return {}


def _invoke(func):
    """Call the entry point, passing the manifests dir only if it accepts an argument."""
    try:
        params = inspect.signature(func).parameters
    except (TypeError, ValueError):
        params = {}

    required = [
        p
        for p in params.values()
        if p.default is inspect.Parameter.empty
        and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    return func(MANIFESTS_DIR) if required else func()


@pytest.fixture(scope="session")
def dag_factory():
    """The imported dag_factory module. Skips when it is not deployed here."""
    pytest.importorskip("airflow", reason="Airflow not installed; install .[validation]")

    module = _import_factory()
    if module is None:
        pytest.skip(
            f"dag_factory not found at {DEFAULT_FACTORY_PATH} (nor "
            f"{FALLBACK_FACTORY_PATH}) and not importable. It is deployed by the platform "
            "repo's pipeline; pull it from the bucket into plugins/ and re-run:\n"
            "  gsutil cp gs://<bucket>/plugins/dag_factory.py plugins/\n"
            "  pytest tests/test_dag_factory_alignment.py"
        )
    return module


@pytest.fixture(scope="session")
def generated_dags(dag_factory):
    """Every DAG the factory produces from this repo's manifests, keyed by dag_id."""
    from airflow.models import DAG

    named = os.environ.get("DAG_FACTORY_ENTRYPOINT")
    entrypoints = [named] if named else list(CANDIDATE_ENTRYPOINTS)

    for name in entrypoints:
        func = getattr(dag_factory, name, None)
        if callable(func):
            try:
                dags = _collect_dags(_invoke(func))
            except Exception as exc:  # noqa: BLE001 - re-raised with context below
                pytest.fail(
                    f"dag_factory.{name}() raised while building DAGs from "
                    f"{MANIFESTS_DIR}.\n\n{explain_factory_error(exc)}"
                )
            if dags:
                return dags
            if named:
                pytest.fail(
                    f"DAG_FACTORY_ENTRYPOINT='{named}' returned no DAGs for "
                    f"{MANIFESTS_DIR}. Check the factory's manifest discovery path."
                )

    # Fall back to module globals: the shape an Airflow dags/ loader file produces.
    dags = {v.dag_id: v for v in vars(dag_factory).values() if isinstance(v, DAG)}
    if dags:
        return dags

    pytest.fail(
        "Could not obtain DAGs from the factory. Tried entry points "
        f"{entrypoints} and a module-globals scan, all empty. Set "
        "DAG_FACTORY_ENTRYPOINT to the correct callable name."
    )
