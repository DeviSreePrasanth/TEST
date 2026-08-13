# etl-mtm

Domain repository for the **MTM** data pipeline on the Unified Data Platform (UDP).

This repo owns MTM's:
- Domain manifest (`dags/manifests/mtm_manifest.yaml`)
- Source configs (`data/etp/configs/mtm__*__configs.yaml`)
- dbt models within the shared ETP wave project (`data/etp/dbt/`)

Per TRD-002 (*UDP Composer Environments, CI/CD Pipeline, and SCM Architecture*), this repo follows the
**"everything is a wheel"** convention: rather than syncing loose files to Cloud Composer's GCS bucket,
CI packages this repo's DAG configs + dbt project into a versioned Python wheel and publishes it to the
`udp-python` Google Artifact Registry (GAR) repo. Cloud Composer then installs/pins the wheel as a
PyPI package, giving reproducible, rollback-able deploys instead of "whatever's currently in GCS."

## Repository layout

The tree **mirrors the deployed Cloud Composer bucket** described in [`structure.md`](structure.md),
so every path here maps to the same path under `gs://<composer-bucket>/`. That means no mental
translation when debugging a Composer environment: what you see in the bucket is what's in the repo.

```
etl-mtm/
├── dags/                                   -> gs://<bucket>/dags/
│   ├── templates/                          #  EMPTY (mirror only; factory lives in plugins/)
│   └── manifests/
│       └── mtm_manifest.yaml               #  OWNED — MTM's jobs
├── data/                                   -> gs://<bucket>/data/
│   └── etp/                                #  the only wave this repo deploys into
│       ├── dbt/                            #  wave-level dbt project, shared by ETP domains
│       │   ├── dbt_project.yml             #   co-owned
│       │   ├── profiles.yml                #   co-owned
│       │   ├── models/mtm/                 #   OWNED — MTM's models
│       │   ├── macros/                     #   co-owned
│       │   └── tests/                      #   singular dbt tests
│       └── configs/
│           └── mtm__ingestion__configs.yaml  # OWNED — <domain>__<job>__configs.yaml
├── logs/                                   #  Composer runtime dir, contents gitignored
├── plugins/                                #  Composer runtime dir, platform-owned
├── src/etl_mtm/__init__.py                 # path resolution for tests + CI (no pipeline logic)
├── pyproject.toml                          # setuptools-scm versioned wheel build
├── tests/                                  # pytest: manifest/config + DAG Factory alignment
├── scripts/                                # validate_dag_factory.py, verify_deployed_dags.py
└── .github/workflows/
    ├── ci.yml                  # ruff + pytest on every PR
    ├── validate-dags.yml       # schema validation, manifest checks, dbt parse, coverage
    ├── deploy.yml              # push to main → wheel → Artifact Registry + Dev bucket
    └── release.yml             # workflow_dispatch (bump) → tag → wheel → AR (prod, gated)
```

### How a change reaches Composer

Two artifacts travel two independent paths and meet inside the environment:

| | Built from | Route | Cadence |
|---|---|---|---|
| **Config** — manifests + dbt project | this repo | wheel → Artifact Registry → Composer bucket | per merge |
| **Code** — `dag_factory` | `udp_platform_shared` | wheel → Artifact Registry → cluster dependency, applied by Terraform | gated |

This repo therefore ships **one versioned artifact**, not a scatter of loose files: the wheel
carries `dags/manifests/` and `data/etp/` as package data (see the packaging note below), so a
deploy is atomic and rollback is just pinning the previous version. `deploy.yml` fails the build
if the wheel doesn't actually contain the manifest and dbt project — a wheel that ships without
them would deploy cleanly and then generate zero DAGs.

The factory is never deployed from here. That separation is deliberate: one factory serves every
domain, so a bad version breaks all of them at once, which is why it sits behind Terraform while
domain config ships freely.

`etp` is the only wave mirrored here. The bucket hosts others (`dig`, `vch`, …), but they are
deployed by their own repos and carrying empty copies would just invite mistakes —
`tests/test_configs.py` fails the build if a directory appears under `data/` that isn't declared
in `etl_mtm.WAVES`. If MTM ever deploys into a second wave, add it there.

`dag_factory.py` is deployed into `plugins/` by its own CI/CD pipeline from the dedicated platform
repo, and is never committed here — a copy in a domain repo would silently drift from the manifest
schema that CI validates against. See [`plugins/README.md`](plugins/README.md).

## Verifying the DAG Factory is aligned with these manifests

Four checks, in two groups. Build-time (does the factory turn manifests into correct DAGs?):

```bash
pip install -e ".[dev,validation]"                      # adds Airflow
gsutil cp gs://<composer-bucket>/plugins/dag_factory.py plugins/

pytest tests/test_dag_factory_alignment.py
python scripts/validate_dag_factory.py
```

This asserts no manifest is missed, no DAG is invented, and each DAG's `dag_id`, schedule, owner,
tags, task set and dependency edges match the manifest. `validate_dag_factory.py` prints the same
result as a missing/extra/wrong report and exits non-zero on drift. The tests **skip** when Airflow
or the factory is absent, so a plain checkout stays green; CI installs both and enforces them
(`dag-factory-alignment` job).

Deploy-time (is it actually live?):

```bash
python scripts/verify_deployed_dags.py --gcs
python scripts/verify_deployed_dags.py --airflow --env <composer-env> --location us-central1
```

**The two are not interchangeable.** With a factory pattern the bucket's `dags/` folder never holds
one `.py` per DAG — DAGs are built in memory at parse time. Listing GCS can only confirm the
*inputs* are present (factory, loader, manifest); only `--airflow` proves a DAG was generated.

The manifest→DAG contract these checks assert lives in
[`tests/dag_expectations.py`](tests/dag_expectations.py) — correct it there if the real factory's
conventions differ, and every assertion follows.

### Packaging note

`dags/` and `data/` sit at the repository root to preserve the bucket mirror, but the wheel still
carries them: `pyproject.toml` maps both trees under the `etl_mtm` package via `package-dir`, so an
installed copy has them at `site-packages/etl_mtm/{dags,data}`. `etl_mtm.ROOT` resolves to whichever
layout is in play, so tests and CI behave identically in a checkout and in an installed wheel.
`logs/` and `plugins/` are deliberately excluded from the wheel.

## CI/CD flow

1. **PR opened** → `ci.yml` (lint/test) + `validate-dags.yml` (schema validation, manifest checks,
   `dbt parse`, manifest↔config coverage) must pass.
2. **Merge to `main`** → `deploy.yml` builds the wheel (`etl-mtm==X.Y.Z.devN+g<sha>` via
   setuptools-scm), asserts it contains the manifest and dbt project, publishes it to Artifact
   Registry, and pushes the `.whl` into the **Dev** Composer bucket under `${WHEEL_PREFIX}/`.
3. **`workflow_dispatch` (release.yml, bump: patch/minor/major)** → computes the next semver tag, builds
   a clean `X.Y.Z` wheel, publishes to Artifact Registry, and deploys to **Prod** — gated behind the
   `prod` GitHub Environment's required reviewers.

Dev and Prod now ship the same artifact by the same mechanism, so a Dev deploy is a genuine
rehearsal of a Prod one. Installing the wheel into the environment is Terraform's job, not this
repo's — the wheel is pushed to a `wheels/` prefix rather than `dags/`, deliberately, since
Airflow parses `dags/` and would only find an unopenable archive there.

## Authentication

All registry publishes and bucket writes authenticate via **Workload Identity Federation (WIF)**,
never a static service-account key. See `.github/workflows/deploy.yml` for the exact
`google-github-actions/auth@v2` config.

## Two GCP projects

A deploy touches **two** projects, and conflating them is the easiest way to break this pipeline:

| Project | Holds | Variable |
|---|---|---|
| `eo-dev-comp-orch-gl-2540` | `composer-udp-env` and its GCS bucket | `COMPOSER_PROJECT_ID` |
| `eo-prod-artifact-na-fb1e` | the `elc-udp-pypi` Python registry | `AR_PROJECT_ID` |

Note `AR_REGION` is the multi-region **`us`**, not `us-central1` — the registry hostname is
`us-python.pkg.dev`. Getting that wrong produces a 404 on publish.

The registry is shared across every domain and every environment — so a **Dev** deploy publishes
into a **prod** project. That's intended, but it means the CI/CD service account needs write access
there, and it's why the two IDs must stay separate: build the registry URL from
`COMPOSER_PROJECT_ID` and you publish to a repository that doesn't exist.

## Required repo variables

All are GitHub Actions **repository variables** (`vars.*`), not secrets — they are identifiers, and
WIF means no key material is stored in this repo at all.

Currently defined:

| Name | Value | Used by |
|---|---|---|
| `WIF_PROVIDER` | `projects/118254587311/locations/global/workloadIdentityPools/…` | deploy, release, validate |
| `GCP_SERVICE_ACCOUNT` | `udp-shared-code-gh-repo@eo-devops-aa51.iam.gserviceaccount.com` | deploy, release, validate |
| `COMPOSER_PROJECT_ID` | `eo-dev-comp-orch-gl-2540` | deploy, release, validate |
| `COMPOSER_BUCKET` | `elc-composer-udp-env-dev` | deploy, validate |
| `AR_PROJECT_ID` | `eo-prod-artifact-na-fb1e` | deploy, release, validate |
| `AR_REPO_NAME` | `elc-udp-pypi` | deploy, release, validate |
| `AR_REGION` | `us` | deploy, release, validate |

Not yet defined — `release.yml` fails fast with a clear message until they exist:

| Name | Purpose |
|---|---|
| `COMPOSER_BUCKET_PROD` | Prod Composer bucket name |
| `COMPOSER_ENV_PROD` | Prod Composer environment name |
| `COMPOSER_LOCATION` | Composer region for the prod environment |
| `COMPOSER_WHEEL_PREFIX` | Optional; bucket prefix for the wheel, defaults to `wheels` |

Note the WIF provider and service account live in **`eo-devops-aa51`**, a third project separate
from both the Composer and registry projects.

## IAM prerequisites (see TRD-002)

- CI/CD SA (`udp-shared-code-gh-repo@eo-devops-aa51`): `roles/artifactregistry.writer` on
  **`eo-prod-artifact-na-fb1e`** (publish the wheel to `elc-udp-pypi`), plus
  `roles/storage.objectAdmin` on the Composer bucket and `roles/storage.objectViewer` to read the
  deployed `dag_factory.py` during CI validation.
- Composer worker SA: `roles/artifactregistry.reader` on **`eo-prod-artifact-na-fb1e`** (pull both
  `etl-mtm` and `dag_factory` at environment update).
- WIF pool: this repo added as an additional provider in the shared UDP WIF pool.

## Local development

```bash
pip install -e ".[dev]" --break-system-packages
ruff check .
pytest
```
