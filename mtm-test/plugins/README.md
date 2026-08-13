# plugins/ — Composer plugins directory

Mirrors `gs://<composer-bucket>/plugins/`, where **`dag_factory.py` is deployed**.

**Intentionally empty in `etl-mtm`.** The factory is built and deployed by its own CI/CD pipeline
from the dedicated platform repo, and is never committed to a domain repo — a fork here would
silently drift from the manifest schema CI validates against. This directory holds only a
`.gitkeep`, and `deploy.yml` never syncs it.

## How CI validates against it

The `dag-factory-alignment` job in `.github/workflows/validate-dags.yml` downloads the deployed
factory from the bucket **into this directory** on every PR, then runs it against
`dags/manifests/` and asserts the DAGs it builds match the manifest entries.

Placing it here rather than in a temp directory matters: the factory resolves paths relative to
its own file, so mirroring the bucket layout makes that resolution behave in CI exactly as it does
in Composer. A temp copy would find no manifests and the job would pass having proved nothing.

Authentication is Workload Identity Federation — no service-account key is stored anywhere.

## `plugins/` is importable, but never scanned for DAGs

Composer syncs this folder to `$AIRFLOW_HOME/plugins` and puts it on `sys.path`, so
`import dag_factory` works. Airflow does **not** scan it for DAG objects — only files under
`dags/` are parsed. So in the deployed environment something under `dags/` has to import the
factory and emit its DAGs, e.g.:

```python
# dags/mtm_dags.py
from dag_factory import build_dags

for dag_id, dag in build_dags().items():
    globals()[dag_id] = dag
```

`scripts/verify_deployed_dags.py --gcs` checks for such a loader, because without one the factory
is inert in Composer no matter how correct the manifests are. This does not affect CI validation,
which imports the factory directly.

## Running the checks locally

```bash
pip install -e ".[dev,validation]"
gsutil cp gs://<composer-bucket>/plugins/dag_factory.py plugins/

pytest tests/test_dag_factory_alignment.py
python scripts/validate_dag_factory.py
```

Both look here first, then `dags/templates/dag_factory.py`, then a plain `import dag_factory`.
Without the factory they **skip** rather than fail, so a plain checkout stays green.

The manifest→DAG contract being asserted lives in
[`tests/dag_expectations.py`](../tests/dag_expectations.py) — correct it there if the deployed
factory's conventions differ, and every assertion follows.
