# logs/ — Composer runtime directory

Mirrors `gs://<composer-bucket>/logs/`, where Airflow writes task logs at runtime.

Nothing is ever committed or deployed here. The directory is tracked (via `.gitkeep`) only to
keep this repo's layout aligned with [`structure.md`](../structure.md); its contents are
gitignored.
