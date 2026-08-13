"""etl-mtm — MTM domain package for the Unified Data Platform.

This package ships no pipeline logic. It carries the MTM domain's declarative assets and
resolves their locations for tooling (tests, CI validation, the dag_factory).

Layout
------
The repository mirrors the deployed Cloud Composer bucket described in ``structure.md``::

    dags/manifests/mtm_manifest.yaml            -> gs://<bucket>/dags/manifests/
    data/etp/configs/mtm__ingestion__configs.yaml -> gs://<bucket>/data/etp/configs/
    data/etp/dbt/                                 -> gs://<bucket>/data/etp/dbt/

``dags/`` and ``data/`` therefore live at the repository root, not under ``src/``. The wheel
build (see ``pyproject.toml``) maps them into this package, so an installed copy has them at
``site-packages/etl_mtm/{dags,data}``. :data:`ROOT` resolves to whichever of the two applies,
which is why every path below is derived from it rather than hardcoded.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import yaml

try:
    __version__ = version("etl-mtm")
except PackageNotFoundError:  # running from a source tree with no metadata
    __version__ = "0.0.0.dev0"

_PACKAGE_DIR = Path(__file__).resolve().parent

# Installed wheel: dags/ and data/ sit inside the package. Source checkout: they sit two
# levels up, at the repository root (src/etl_mtm/__init__.py -> src -> repo root).
ROOT: Path = _PACKAGE_DIR if (_PACKAGE_DIR / "dags").is_dir() else _PACKAGE_DIR.parents[1]

DAGS_DIR: Path = ROOT / "dags"
MANIFESTS_DIR: Path = DAGS_DIR / "manifests"
DATA_DIR: Path = ROOT / "data"

#: Where the platform pipeline deploys dag_factory.py. Present in a checkout and in the
#: Composer bucket, but deliberately NOT packaged into the wheel, so this path may not exist
#: for an installed copy -- callers must handle its absence (see tests/conftest.py).
PLUGINS_DIR: Path = ROOT / "plugins"

#: Waves present in this repository. The Composer bucket also hosts other waves (dig, vch,
#: ...), but they are owned by their own repos and are not mirrored here -- etl-mtm only
#: carries the wave it deploys into. Add an entry only when this repo starts deploying to it.
WAVES: tuple[str, ...] = ("etp",)

#: This repository's domain, and the wave it belongs to.
DOMAIN = "mtm"
OWNED_WAVE = "etp"


def wave_dir(wave: str = OWNED_WAVE) -> Path:
    """Return ``data/<wave>/``."""
    return DATA_DIR / wave


def configs_dir(wave: str = OWNED_WAVE) -> Path:
    """Return ``data/<wave>/configs/``, where ``<wave>__<job>__configs.yaml`` files live."""
    return wave_dir(wave) / "configs"


def dbt_dir(wave: str = OWNED_WAVE) -> Path:
    """Return ``data/<wave>/dbt/``, the wave-scoped dbt Core project."""
    return wave_dir(wave) / "dbt"


def manifest_path(domain: str = DOMAIN) -> Path:
    """Return ``dags/manifests/<domain>_manifest.yaml``."""
    return MANIFESTS_DIR / f"{domain}_manifest.yaml"


def config_path(job: str, wave: str = OWNED_WAVE) -> Path:
    """Return the source config for ``job``, following the ``<wave>__<job>__configs`` rule.

    The job name is domain-prefixed (``mtm_ingestion``); the filename drops that prefix and
    uses a double underscore as the separator (``mtm__ingestion__configs.yaml``).
    """
    domain, _, suffix = job.partition("_")
    stem = f"{domain}__{suffix}" if suffix else domain
    return configs_dir(wave) / f"{stem}__configs.yaml"


def load_manifest(domain: str = DOMAIN) -> dict[str, Any]:
    """Parse and return one domain manifest."""
    return yaml.safe_load(manifest_path(domain).read_text(encoding="utf-8")) or {}


def iter_manifests() -> dict[str, dict[str, Any]]:
    """Return every manifest in ``dags/manifests/``, keyed by domain name.

    This repo carries only MTM's manifest; other domains ship theirs from their own repos.
    """
    return {
        path.stem.removesuffix("_manifest"): yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for path in sorted(MANIFESTS_DIR.glob("*_manifest.yaml"))
    }


def iter_dag_entries(domain: str = DOMAIN) -> list[dict[str, Any]]:
    """Return the DAG entries declared in a domain manifest.

    The factory requires a top-level ``dags:`` key and logs an error for anything else, so
    that is the only key read here -- a manifest using a different key would pass a lenient
    test and then generate nothing in Composer.
    """
    return load_manifest(domain).get("dags") or []


__all__ = [
    "DAGS_DIR",
    "DATA_DIR",
    "DOMAIN",
    "MANIFESTS_DIR",
    "OWNED_WAVE",
    "PLUGINS_DIR",
    "ROOT",
    "WAVES",
    "__version__",
    "config_path",
    "configs_dir",
    "dbt_dir",
    "iter_dag_entries",
    "iter_manifests",
    "load_manifest",
    "manifest_path",
    "wave_dir",
]
