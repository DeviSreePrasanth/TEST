# `gcloud storage rsync` — test scenarios

Scratchpad for observing how rsync decides what to copy and what to delete.

**Setup used below**

```bash
SRC=mtm-test/dags
DST=gs://elc-composer-udp-env-dev1/dags

# Always preview first. --dry-run prints the decision without acting.
plan() { gcloud storage rsync --recursive --dry-run "$@" "$SRC" "$DST"; }
apply() { gcloud storage rsync --recursive "$@" "$SRC" "$DST"; }
look() { gcloud storage ls --recursive --long "$DST/**"; }
```

Reading the output:

| Line | Meaning |
|---|---|
| `Would copy file://… to gs://…` | new **or** modified — will upload |
| `Would remove gs://…` | in bucket, absent from source (only with the delete flag) |
| *(no line for a file)* | unchanged — skipped |

> On Windows you'll see `WARNING: The following characters are invalid…` / `Renaming …`
> lines. Cosmetic — gcloud's temp tracker filenames. They do not appear on Linux runners.

---

## Status legend

- **[verified]** — observed live against `gs://elc-composer-udp-env-dev1`
- **[untested]** — reason it wasn't run is noted; verify before relying on it

---

## 1. New file added — [verified]

```bash
printf 'version: 1\n' > $SRC/probe.txt
plan     # -> Would copy file://…/probe.txt to gs://…/dags/probe.txt
apply
look     # dags/probe.txt now present
```

**Observed:** one `Would copy` line; object created at the mirrored relative path.
`mtm-test/dags/manifests/x.yaml` → `dags/manifests/x.yaml`.

## 2. Existing file modified — [verified]

```bash
printf 'version: 2 CHANGED\n' > $SRC/probe.txt
plan     # -> Would copy (same path)
apply
gcloud storage cat $DST/probe.txt   # -> version: 2 CHANGED
```

**Observed:** object overwritten in place. Same name, new content, new generation.

## 3. No change at all — [verified]

```bash
plan     # -> no copy lines
```

**Observed:** rsync skipped everything. This is the baseline "clean" result.

## 4. File deleted locally, **without** the delete flag — [verified]

```bash
rm $SRC/probe.txt
plan                                  # -> nothing
gcloud storage ls $DST/probe.txt      # -> STILL THERE
```

**Observed: deletions do NOT propagate by default.** This is the single most
important behaviour to internalise. Without `--delete-unmatched-destination-objects`
rsync is additive only — the bucket accumulates orphans forever.

## 5. File deleted locally, **with** the delete flag — [untested]

```bash
rm $SRC/probe.txt
plan  --delete-unmatched-destination-objects
apply --delete-unmatched-destination-objects
```

**Expected:** a `Would remove gs://…/probe.txt` line, then the object disappears.

> Not confirmed live — the dry-run output didn't match the grep filter used, so the
> exact wording is unverified. Run the plan step and note what it actually prints.

## 6. File renamed — [untested]

```bash
git mv $SRC/probe.txt $SRC/probe-renamed.txt
plan --delete-unmatched-destination-objects
```

**Expected:** rsync has no rename concept — one `copy` (new name) plus one `remove`
(old name). Without the delete flag you get **both** names in the bucket.

## 7. Empty source directory + delete flag — [untested] ⚠️ DANGEROUS

```bash
mkdir -p /tmp/empty
gcloud storage rsync --recursive --delete-unmatched-destination-objects \
  --dry-run /tmp/empty "$DST"
```

**Expected: every object under the destination prefix is deleted.** Test with
`--dry-run` only. This is the failure mode to fear in CI — if a path is wrong, a
build step didn't produce output, or a directory is conditionally absent, a
delete-sync silently wipes the prefix. Nothing warns you.

## 8. Touch without changing content — [untested]

```bash
touch $SRC/probe.txt
plan
```

**Expected: a copy, despite identical content.** rsync compares **size + mtime**, not
content. Since `actions/checkout` rewrites mtimes on every run, expect CI to re-upload
everything on every build. Compare against:

```bash
plan --checksums-only    # should skip — compares content hashes instead
```

Worth measuring if the synced tree is large; `--checksums-only` costs local CPU but
avoids pointless uploads.

## 9. Exclude pattern + delete flag — [untested] ⚠️ VERIFY THIS ONE

```bash
# seed the bucket with something the exclude will later hide
gcloud storage rsync --recursive $SRC $DST

# now sync again, excluding it, WITH delete
plan --delete-unmatched-destination-objects --exclude='(^|/)target/'
```

**The question:** does `--exclude` protect matching objects *in the destination* from
deletion, or does the exclude only filter the *source* listing — making excluded
destination objects look "unmatched" and therefore deletable?

I attempted to verify this live and the command was blocked (it required recursively
clearing a scratch prefix that hadn't been explicitly authorised). **It remains an open
question and it directly affects the `sync()` function below.** Test it in a throwaway
bucket before trusting the exclude to protect anything.

## 10. New nested directory — [untested]

```bash
mkdir -p $SRC/newdir/sub && printf 'x\n' > $SRC/newdir/sub/f.txt
plan
```

**Expected:** copied. GCS has no real directories, so depth costs nothing and no
directory needs pre-creating. Contrast with the **wheel** path, where a new directory
must be added to `packages` in `pyproject.toml` or it is silently dropped.

## 11. Empty directory — [verified, incidentally]

`mtm-test/dags/templates/` is empty and produced **no** object. Empty directories are
not representable in GCS and are not tracked by git either. Anything relying on a
directory existing will not find it.

## 12. Drift — object changed in the bucket only — [untested]

```bash
echo 'edited directly in gcs' | gcloud storage cp - $DST/probe.txt
plan
```

**Expected:** rsync is one-way. If the sizes differ it re-copies from source and your
bucket edit is lost. If the size happens to match, mtime decides. Do not hand-edit
objects under a synced prefix.

## 13. Trailing slashes — [untested]

```bash
plan   # with SRC=mtm-test/dags   and DST=gs://…/dags
plan   # with SRC=mtm-test/dags/  and DST=gs://…/dags/
```

**Expected:** equivalent for `gcloud storage rsync` (unlike Unix `rsync`, where the
trailing slash on the source is load-bearing). Confirm rather than assume — getting it
wrong nests the tree one level deeper, e.g. `dags/dags/manifests/…`.

---

## Notes on the `sync()` function you shared

Reviewing the snippet against this repo:

**1. `${DOMAIN}` is never defined.** The `env:` block sets only `COMPOSER_BUCKET`. With
`set -u` active the function dies on first use — loudly, at least, but it will not run
as written.

**2. The source paths don't match this repo's layout.** The snippet syncs `configs/`
and `dbt/` from the repo root; here they live at `data/etp/configs/` and
`data/etp/dbt/`. See [pyproject.toml](mtm-test/pyproject.toml).

**3. Per-domain delete scoping is the right call.** Because each destination is
`…/${DOMAIN}/`, the delete flag can only ever remove that one domain's objects. Other
domains sharing the bucket are structurally protected. Good design — keep it.

**4. Preview-then-apply runs rsync twice.** Correct and worth the cost, but note the
two invocations are not atomic: the preview reflects bucket state at time T, the apply
acts at T+n. On a bucket with concurrent writers they can disagree.

**5. Scenario 7 is the live risk.** Three delete-syncs, each pointed at a source
directory that must exist and be populated. If `dbt/` is ever missing or empty, that
prefix is emptied in the bucket. Consider guarding each call:

```bash
if [[ -z "$(find "${src}" -type f -print -quit 2>/dev/null)" ]]; then
  echo "::error::${src} is empty or missing — refusing to delete-sync ${dst}"
  return 1
fi
```

**6. Scenario 9 is unresolved and applies directly to `dbt_exclude`.** If `--exclude`
does not shield destination objects, the delete flag will remove anything already in
the bucket under `target/`, `logs/`, or `dbt_packages/`. Probably desirable here —
those are build artefacts — but confirm it's what actually happens.

---

## Cleanup

```bash
gcloud storage rm --recursive gs://elc-composer-udp-env-dev1/dags
git checkout -- mtm-test/dags/
```
