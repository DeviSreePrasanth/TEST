"""Package-level smoke tests.

These assert that the layout etl_mtm resolves at import time actually exists. They are the
tripwire for the dual-mode path resolution in etl_mtm.__init__: if the wheel's package-dir
mapping in pyproject.toml is broken, ROOT resolves to the wrong place and these fail.
"""

from etl_mtm import (
    DAGS_DIR,
    DATA_DIR,
    MANIFESTS_DIR,
    ROOT,
    WAVES,
    __version__,
    configs_dir,
    dbt_dir,
    manifest_path,
)


def test_version_is_set():
    assert __version__


def test_composer_mirror_dirs_exist():
    assert DAGS_DIR.is_dir()
    assert MANIFESTS_DIR.is_dir()
    assert DATA_DIR.is_dir()


def test_every_wave_dir_exists():
    for wave in WAVES:
        assert configs_dir(wave).is_dir(), f"missing data/{wave}/configs/"
        assert dbt_dir(wave).is_dir(), f"missing data/{wave}/dbt/"


def test_mtm_manifest_exists():
    assert manifest_path().is_file()


def test_dbt_project_is_present_and_named_for_dbt():
    """dbt only recognises dbt_project.yml / profiles.yml -- not the .yaml spelling."""
    assert (dbt_dir() / "dbt_project.yml").is_file()
    assert (dbt_dir() / "profiles.yml").is_file()
    for subdir in ("models", "macros", "tests"):
        assert (dbt_dir() / subdir).is_dir(), f"missing data/etp/dbt/{subdir}/"


def test_runtime_dirs_are_not_packaged():
    """logs/ and plugins/ are Composer runtime dirs; they must never ship inside the wheel."""
    if ROOT.name == "etl_mtm":  # installed wheel
        assert not (ROOT / "logs").exists()
        assert not (ROOT / "plugins").exists()
