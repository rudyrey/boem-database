#!/usr/bin/env bash
#
# BOEM Database Updater
# Downloads fresh data from BOEM/BSEE, rebuilds the SQLite database,
# and optionally creates a GitHub release.
#
# Usage:
#   ./update_database.sh              # Download all + rebuild
#   ./update_database.sh --download   # Download only
#   ./update_database.sh --build      # Rebuild only (from existing files)
#   ./update_database.sh --release    # Download + rebuild + GitHub release
#
# Requirements: curl, python3, unzip
# Optional:     gh (GitHub CLI) for --release

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RAW_DIR="$SCRIPT_DIR/raw_data"
EXTRACTED_DIR="$RAW_DIR/extracted"
DB_PATH="$SCRIPT_DIR/boem.db"

BOEM="https://www.data.boem.gov"
BSEE="https://www.data.bsee.gov"

# ============================================================================
# DATA SOURCES
# Format: "local_filename|url|description"
# Each entry is one zip to download. URLs verified 2026-03-18.
# ============================================================================
DOWNLOADS=(
  # Wells & Boreholes
  "borehole_delimit.zip|${BOEM}/Well/Files/borehole_delimit.zip|Boreholes / Wells"
  "rig_id_delimit.zip|${BOEM}/Well/Files/rig_id_delimit.zip|Rig ID List"

  # eWell Submissions (from BSEE — updated daily)
  "eWellAPDRawData.zip|${BSEE}/Well/Files/eWellAPDRawData.zip|eWell APD (Permit to Drill)"
  "eWellAPMRawData.zip|${BSEE}/Well/Files/eWellAPMRawData.zip|eWell APM (Permit to Modify)"

  # Companies
  "company_all_delimit.zip|${BOEM}/Company/Files/company_all_delimit.zip|Companies"

  # Leasing
  "lease_data_fixed.zip|${BOEM}/Leasing/Files/lease_data_fixed.zip|Lease Data"
  "lease_list_fixed.zip|${BOEM}/Leasing/Files/lease_list_fixed.zip|Lease List"
  "lease_owner_op_delimit.zip|${BOEM}/Leasing/Files/lease_owner_op_delimit.zip|Lease Owners"

  # Platforms
  "platform_master_fixed.zip|${BOEM}/Platform/Files/platform_master_fixed.zip|Platform Masters"
  "platform_structure_fixed.zip|${BOEM}/Platform/Files/platform_structure_fixed.zip|Platform Structures"
  "platform_location_fixed.zip|${BOEM}/Platform/Files/platform_location_fixed.zip|Platform Locations"
  "platform_approvals_delimit.zip|${BOEM}/Platform/Files/platform_approvals_delimit.zip|Platform Approvals"
  "platform_removed_delimit.zip|${BOEM}/Platform/Files/platform_removed_delimit.zip|Platform Removals"

  # Pipelines
  "pipeline_master_delimit.zip|${BOEM}/Pipeline/Files/pipeline_master_delimit.zip|Pipeline Masters"
  "pipeline_location_delimit.zip|${BOEM}/Pipeline/Files/pipeline_location_delimit.zip|Pipeline Locations"

  # Fields & Appendices
  "field_names_delimit.zip|${BOEM}/Production/Files/field_names_delimit.zip|Field Names"
  "field_production_delimit.zip|${BOEM}/Production/Files/field_production_delimit.zip|Field Production"
  "appendix_a_delimit.zip|${BOEM}/Production/Files/appendix_a_delimit.zip|Appendix A (Area/Block to Field)"
  "appendix_b_delimit.zip|${BOEM}/Production/Files/appendix_b_delimit.zip|Appendix B (Lease to Field)"
  "appendix_c_delimit.zip|${BOEM}/Production/Files/appendix_c_delimit.zip|Appendix C (Operator to Field)"
)

# Production OGOR-A — one zip per year
OGORA_START_YEAR=1996

# ============================================================================
# FUNCTIONS
# ============================================================================

log() { echo "[$(date '+%H:%M:%S')] $*"; }

download_file() {
  local dest="$1"
  local url="$2"
  local desc="$3"

  # Use -z to skip download if local file is newer than remote (conditional GET)
  if [ -f "$dest" ]; then
    local http_code
    http_code=$(curl -fSL --retry 3 --retry-delay 5 -z "$dest" -o "$dest" -w "%{http_code}" "$url" 2>/dev/null) || true
    if [ "$http_code" = "304" ]; then
      log "    $(basename "$dest") (unchanged)"
      return 0
    fi
  else
    curl -fSL --retry 3 --retry-delay 5 -o "$dest" "$url" 2>/dev/null || true
  fi

  if [ -f "$dest" ] && [ -s "$dest" ]; then
    local size
    size=$(du -h "$dest" | cut -f1)
    log "    $(basename "$dest") (${size})"
    return 0
  else
    log "    FAILED: $(basename "$dest")"
    return 1
  fi
}

download_all() {
  log "=========================================="
  log "Downloading BOEM/BSEE raw data"
  log "=========================================="
  mkdir -p "$RAW_DIR"

  local ok=0 fail=0

  for entry in "${DOWNLOADS[@]}"; do
    IFS='|' read -r fname url desc <<< "$entry"
    log "  ${desc}..."
    if download_file "$RAW_DIR/$fname" "$url" "$desc"; then
      ((ok++))
    else
      ((fail++))
    fi
  done

  # OGOR-A production (one per year)
  local current_year
  current_year=$(date +%Y)
  log ""
  log "OGOR-A production (${OGORA_START_YEAR}–${current_year})..."

  for year in $(seq $OGORA_START_YEAR "$current_year"); do
    local ogor_file="ogora_${year}_delimit.zip"
    local ogor_url="${BSEE}/Production/Files/ogora${year}delimit.zip"
    if download_file "$RAW_DIR/$ogor_file" "$ogor_url" "OGOR-A ${year}"; then
      ((ok++))
    else
      if [ "$year" -ge "$((current_year))" ]; then
        log "    (${year} may not be available yet)"
      else
        ((fail++))
      fi
    fi
  done

  log ""
  log "Downloads: ${ok} OK, ${fail} failed"
}

extract_all() {
  log "Extracting zip files..."
  mkdir -p "$EXTRACTED_DIR"

  for zf in "$RAW_DIR"/*.zip; do
    [ -f "$zf" ] || continue
    local bn
    bn=$(basename "$zf")

    if [[ "$bn" == eWell* ]]; then
      # eWell zips nest files in a subdirectory — flatten
      local tmpdir
      tmpdir=$(mktemp -d)
      unzip -qo "$zf" -d "$tmpdir" 2>/dev/null || true
      find "$tmpdir" -type f \( -name "*.txt" -o -name "*.DAT" \) -exec mv {} "$EXTRACTED_DIR/" \;
      rm -rf "$tmpdir"
    else
      unzip -qo "$zf" -d "$EXTRACTED_DIR" 2>/dev/null || true
    fi
  done

  local count
  count=$(find "$EXTRACTED_DIR" -type f \( -name "*.txt" -o -name "*.DAT" \) | wc -l | tr -d ' ')
  log "  ${count} data files ready"
}

build_database() {
  log "=========================================="
  log "Building database${FULL_REBUILD:+ (full rebuild)}"
  log "=========================================="
  extract_all
  cd "$SCRIPT_DIR"
  if [ "${FULL_REBUILD:-}" = "1" ]; then
    python3 build_database.py --full
  else
    python3 build_database.py
  fi
}

create_release() {
  log "=========================================="
  log "Creating GitHub release"
  log "=========================================="

  if ! command -v gh &>/dev/null; then
    log "ERROR: gh (GitHub CLI) not found. Install with: brew install gh"
    exit 1
  fi
  if [ ! -f "$DB_PATH" ]; then
    log "ERROR: ${DB_PATH} not found. Build first."
    exit 1
  fi

  local db_size
  db_size=$(du -h "$DB_PATH" | cut -f1)
  local tag="v$(date +%Y.%m.%d)"

  # Delete existing tag/release if it exists
  if gh release view "$tag" &>/dev/null 2>&1; then
    log "Release ${tag} exists — replacing..."
    gh release delete "$tag" --yes 2>/dev/null || true
    git tag -d "$tag" 2>/dev/null || true
    git push origin ":refs/tags/${tag}" 2>/dev/null || true
  fi

  local row_count
  row_count=$(python3 -c "
import sqlite3
conn = sqlite3.connect('$DB_PATH')
tables = [r[0] for r in conn.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall()]
total = sum(conn.execute(f'SELECT COUNT(*) FROM \"{t}\"').fetchone()[0] for t in tables)
print(f'{total:,}')
conn.close()
")

  local table_count
  table_count=$(python3 -c "
import sqlite3
conn = sqlite3.connect('$DB_PATH')
print(conn.execute(\"SELECT COUNT(*) FROM sqlite_master WHERE type='table'\").fetchone()[0])
conn.close()
")

  log "Uploading ${db_size} database..."
  gh release create "$tag" "$DB_PATH" \
    --title "${tag} — BOEM Database" \
    --notes "$(cat <<EOF
Automated rebuild from BOEM/BSEE raw data.

- **Date:** $(date '+%B %d, %Y')
- **Size:** ${db_size}
- **Tables:** ${table_count}
- **Rows:** ${row_count}
EOF
)" \
    --latest

  local repo
  repo=$(gh repo view --json nameWithOwner -q .nameWithOwner)
  log ""
  log "Release: https://github.com/${repo}/releases/tag/${tag}"
  log ""
  log "Update Railway BOEM_DB_URL to:"
  log "  https://github.com/${repo}/releases/download/${tag}/boem.db"
}

show_help() {
  cat <<EOF
BOEM Database Updater

Downloads raw data from BOEM/BSEE data centers, rebuilds the SQLite
database, and optionally publishes it as a GitHub release.

Incremental by default — only downloads changed files (HTTP conditional
GET) and only reloads tables whose source data has changed.

Usage:
  ./update_database.sh              Download + incremental rebuild
  ./update_database.sh --download   Download only (skip rebuild)
  ./update_database.sh --build      Incremental rebuild from existing files
  ./update_database.sh --full       Download + full rebuild from scratch
  ./update_database.sh --release    Download + incremental rebuild + GitHub release
  ./update_database.sh --help       Show this help

Data sources:
  BOEM Data Center  https://www.data.boem.gov
  BSEE Data Center  https://www.data.bsee.gov

The script downloads ~50 zip files covering wells, permits, leases,
platforms, pipelines, production, companies, and rigs. Production data
(OGOR-A) spans ${OGORA_START_YEAR} to present with one file per year.
EOF
}

# ============================================================================
# MAIN
# ============================================================================

case "${1:-all}" in
  --download|-d)  download_all ;;
  --build|-b)     build_database ;;
  --full|-f)      download_all; FULL_REBUILD=1 build_database ;;
  --release|-r)   download_all; build_database; create_release ;;
  --help|-h)      show_help ;;
  all|"")         download_all; build_database ;;
  *)              echo "Unknown: $1 (try --help)"; exit 1 ;;
esac

log "Done!"
