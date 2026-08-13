"""Make an Airflow DAG parse work with no metadata database.

Shared by tests/conftest.py and scripts/validate_dag_factory.py. Both execute the deployed
factory, so both need the same environment -- keeping it here stops them drifting apart, which
is exactly what happened once: the tests passed while the script blew up on a missing
`variable` table.

Import and call :func:`configure` BEFORE anything imports airflow.
"""

from __future__ import annotations

import os
import tempfile

#: Airflow Variables the factory reads while building DAGs. Airflow checks AIRFLOW_VAR_*
#: environment variables before the metadata DB, so seeding them here keeps the parse
#: entirely offline. The values are never used -- the checks inspect DAG structure and never
#: submit a run.
PLACEHOLDER_VARIABLES = {
    "AIRFLOW_VAR_DATABRICKS_CLUSTER_ID": "ci-validation-placeholder",
}


def configure() -> None:
    """Seed the environment for a database-free DAG parse. Safe to call more than once."""
    for key, value in PLACEHOLDER_VARIABLES.items():
        os.environ.setdefault(key, value)

    # Keep Airflow's scratch files out of the repo; it otherwise scatters logs/ directories
    # into the working tree, where they look like deployable content.
    os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-validation-"))
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    os.environ.setdefault("AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION", "True")


def explain_factory_error(exc: BaseException) -> str:
    """Turn an opaque Airflow error into something a reader can act on.

    A factory that reaches for a Variable, Connection or provider package that CI does not
    have fails with a message that says nothing about the real cause, and reads like the
    manifest is broken when it is not.
    """
    text = f"{type(exc).__name__}: {exc}"
    hint = ""

    if isinstance(exc, ModuleNotFoundError):
        hint = (
            f"The factory imports '{exc.name}', which is not installed. If it is an Airflow "
            "provider, add it to the [validation] extra in pyproject.toml."
        )
    elif "variable" in text.lower():
        hint = (
            "The factory looked up an Airflow Variable that CI has no database for. Seed it "
            "by adding AIRFLOW_VAR_<NAME_UPPERCASED> to PLACEHOLDER_VARIABLES in "
            "tests/airflow_env.py."
        )
    elif "connection" in text.lower() or "conn_id" in text:
        hint = (
            "The factory resolved an Airflow Connection at parse time. Seed it with "
            "AIRFLOW_CONN_<CONN_ID_UPPERCASED>, or have the factory defer the lookup to "
            "execution time."
        )

    return f"{text}\n\n{hint}" if hint else text
