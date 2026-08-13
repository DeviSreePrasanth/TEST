# `gcloud storage rsync` — manual test steps

Local-only. No GitHub Actions, no WIF, no workflow.

## Setup

```bash
cd c:/Users/prasanth.byreddi/Downloads/TEST

SRC=mtm-test/dags
DST=gs://elc-composer-udp-env-dev1/dags

# generation changes on EVERY overwrite — the reliable "was it re-uploaded?" signal.
# update_time alone is not enough to reason from.
stamp() { gcloud storage objects describe "$DST/$1" --format="value(generation,update_time,md5_hash)"; }

look()  { gcloud storage ls --long --recursive "$DST/**"; }
plan()  { gcloud storage rsync --recursive --dry-run "$@" "$SRC" "$DST"; }
sync()  { gcloud storage rsync --recursive "$@" "$SRC" "$DST"; }
```

Reading the output:

| Line | Meaning |
|---|---|
| `Would copy file://… to gs://…` | new **or** changed — will upload |
| `Would remove gs://…` | in bucket, not in source (only with the delete flag) |
| *(nothing for a file)* | unchanged — skipped |

> On Windows, ignore `WARNING: The following characters are invalid…` and `Renaming …`.
> Cosmetic noise from gcloud's temp tracker filenames. Absent on Linux.

---

# Part 1 — Verified scenarios

All four were run live against `gs://elc-composer-udp-env-dev1` and behaved as recorded.

## 1. New file is added ✅

```bash
printf 'hello\n' > $SRC/test1.txt
sync
stamp test1.txt      # -> note the generation, call it A
look                 # dags/test1.txt present
```

**Result:** uploaded. Path mirrors the source relative path —
`mtm-test/dags/manifests/x.yaml` → `dags/manifests/x.yaml`.

## 2. Unmodified file is NOT re-uploaded ✅

```bash
sync                 # change nothing, just run it again
stamp test1.txt      # -> generation B
```

**Result:** `A == B`. Generation unchanged, so the object was never rewritten.

```
A = 1786620014994924
B = 1786620014994924   <- identical
```

This is the behaviour you were asking about, and it holds.

## 3. Modified file IS re-uploaded ✅

```bash
printf 'hello CHANGED\n' > $SRC/test1.txt
sync
stamp test1.txt      # -> generation C
```

**Result:** `C != A`. New generation, new `update_time`.

```
C = 1786620090204127   <- changed
```

## 4. `touch` alone does NOT trigger an upload ✅

```bash
touch $SRC/test1.txt        # content identical, mtime bumped
plan                        # -> no copy line
```

**Result:** skipped, despite the local mtime moving 52 seconds ahead of the mtime
stored on the object (`goog-reserved-file-mtime`).

**Why this matters:** rsync compares size and MD5, not just mtime. So
`actions/checkout` rewriting mtimes on every CI run will **not** cause spurious
re-uploads. `--checksums-only` is not needed for this.

## 5. Deleting locally does NOT delete in the bucket ✅

```bash
rm $SRC/test1.txt
plan                                    # -> nothing
gcloud storage ls $DST/test1.txt        # -> STILL THERE
```

**Result:** without `--delete-unmatched-destination-objects`, rsync is **additive
only**. Orphans accumulate in the bucket forever. Most important behaviour in this
document.

## 6. Empty directories produce nothing ✅

`mtm-test/dags/templates/` is empty and created no object. GCS has no real
directories, and git doesn't track empty ones either.

---

# Part 2 — Not yet verified

Run these yourself; each notes what to look for.

## 7. Delete with the flag

```bash
rm $SRC/test1.txt
plan --delete-unmatched-destination-objects
```

Expect a removal line, then apply and confirm the object is gone. **Note the exact
wording** — my grep filter missed it, so I can't tell you what it prints.

## 8. Rename

```bash
mv $SRC/test1.txt $SRC/test2.txt
plan --delete-unmatched-destination-objects
```

rsync has no rename concept — expect one copy (new name) + one remove (old name).
Without the delete flag you keep **both** names in the bucket.

## 9. Empty source + delete flag ⚠️ DANGEROUS — dry-run only

```bash
mkdir -p /tmp/empty
gcloud storage rsync --recursive --delete-unmatched-destination-objects \
  --dry-run /tmp/empty "$DST"
```

Expect **every object under the prefix** to be marked for deletion. This is the CI
failure mode to fear: a wrong path, a build step that produced no output, or a
conditionally-absent directory silently wipes the prefix.

## 10. Exclude + delete flag ⚠️ VERIFY BEFORE RELYING ON IT

Does `--exclude` protect matching objects **already in the destination** from being
deleted, or does it only filter the source listing — making those objects look
"unmatched" and therefore deletable?

```bash
plan --delete-unmatched-destination-objects --exclude='(^|/)target/'
```

I attempted this live and the command was blocked (it needed to recursively clear a
scratch prefix that hadn't been authorised). **Unresolved, and it directly affects the
`dbt_exclude` in your sync function.** Test in a throwaway bucket.

## 11. Drift — object edited in the bucket only

```bash
echo 'edited in gcs' | gcloud storage cp - $DST/test1.txt
plan
```

rsync is one-way; expect your bucket edit to be overwritten from source. Don't
hand-edit objects under a synced prefix.

## 12. Trailing slashes

```bash
plan    # SRC=mtm-test/dags   DST=gs://…/dags
plan    # SRC=mtm-test/dags/  DST=gs://…/dags/
```

Expect equivalence (unlike Unix `rsync`, where the source trailing slash is
load-bearing). Confirm — getting it wrong nests as `dags/dags/manifests/…`.

---

## Cleanup

```bash
rm -f $SRC/test1.txt $SRC/test2.txt
gcloud storage rm $DST/test1.txt $DST/test2.txt
```

---

# Part 3 — Notes on your `sync()` function

**`${DOMAIN}` is never defined.** The `env:` block sets only `COMPOSER_BUCKET`. With
`set -u` the function dies on first call.

**Source paths don't match this repo.** The snippet syncs `configs/` and `dbt/` from
the root; here they are at `data/etp/configs/` and `data/etp/dbt/`.

**Per-domain delete scoping is correct — keep it.** Because each destination ends in
`/${DOMAIN}/`, the delete flag can only ever touch that one domain. Other domains
sharing the bucket are structurally protected.

**Preview-then-apply runs rsync twice.** Worth the cost, but not atomic — the preview
reflects state at T, the apply acts at T+n.

**Guard against scenario 9.** Three delete-syncs, each assuming its source exists and
is populated:

```bash
if [[ -z "$(find "${src}" -type f -print -quit 2>/dev/null)" ]]; then
  echo "::error::${src} is empty or missing — refusing to delete-sync ${dst}"
  return 1
fi
```
