#!/usr/bin/env bash
# Local reproduction of the "Sync to Composer bucket" step, run outside GitHub Actions.
#
# Only two things differ from the workflow snippet:
#   - COMPOSER_BUCKET / DOMAIN come from the environment instead of `vars.`
#   - the configs/ and dbt/ source paths are corrected to where they actually live
#     in this repo (data/etp/...). The originals do not exist here.
#
# Usage:
#   DOMAIN=mtm ./test-sync.sh              # apply for real
#   DOMAIN=mtm PREVIEW_ONLY=1 ./test-sync.sh   # dry-run only, nothing is written

set -euo pipefail
cd "$(dirname "$0")/mtm-test"

COMPOSER_BUCKET="${COMPOSER_BUCKET:-elc-composer-udp-env-dev1}"
bucket="gs://${COMPOSER_BUCKET}"

# Delete-sync with a dry-run first, so the deletion set is visible in the log
# before it is applied. $3 is an optional exclude regex.
sync() {
  local src="$1" dst="$2" exclude="${3:-}"
  local flags=(--recursive --delete-unmatched-destination-objects)
  if [[ -n "${exclude}" ]]; then
    flags+=(--exclude="${exclude}")
  fi

  echo "::group::${src} -> ${dst} (preview)"
  gcloud storage rsync "${flags[@]}" --dry-run "${src}" "${dst}"
  echo "::endgroup::"

  if [[ -n "${PREVIEW_ONLY:-}" ]]; then
    echo "(PREVIEW_ONLY set - skipping apply)"
    return 0
  fi

  echo "::group::${src} -> ${dst} (apply)"
  gcloud storage rsync "${flags[@]}" "${src}" "${dst}"
  echo "::endgroup::"
}

# dbt build artefacts are regenerated where dbt runs and must never be deployed.
dbt_exclude='(^|/)(target|logs|dbt_packages)/|(^|/)\.user\.yml$'

sync "dags/manifests/"   "${bucket}/dags/manifests/${DOMAIN}/"
sync "data/etp/configs/" "${bucket}/data/${DOMAIN}/configs/"
sync "data/etp/dbt/"     "${bucket}/data/${DOMAIN}/dbt/" "${dbt_exclude}"
