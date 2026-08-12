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
# NOTE (2026-05): BOEM restructured its data center. The old per-dataset
# *_delimit.zip / *_fixed.zip files were replaced by consolidated *RawData.zip
# files (header-row delimited "mv_*" views, nested in a subdirectory). The
# field/reserves and rig-list reference files were preserved in the OLD format
# but RELOCATED (/FieldReserves/Files/ and /Platform/Files/). Local filenames
# below are kept stable so LOADER_MAP / checksum keys don't change; only the
# remote URLs and the extracted inner files differ.
DOWNLOADS=(
  # Wells & Boreholes  (new consolidated RawData format)
  "borehole_delimit.zip|${BOEM}/Well/Files/BoreholeRawData.zip|Boreholes / Wells"
  "rig_id_delimit.zip|${BOEM}/Platform/Files/rigidlistdelimit.zip|Rig ID List (relocated, old format)"

  # eWell Submissions (from BSEE — updated daily)
  "eWellAPDRawData.zip|${BSEE}/Well/Files/eWellAPDRawData.zip|eWell APD (Permit to Drill)"
  "eWellAPMRawData.zip|${BSEE}/Well/Files/eWellAPMRawData.zip|eWell APM (Permit to Modify)"
  "eWellEORRawData.zip|${BSEE}/Well/Files/eWellEORRawData.zip|eWell EOR (End of Operations)"
  "eWellWARRawData.zip|${BSEE}/Well/Files/eWellWARRawData.zip|eWell WAR (Well Activity Reports)"

  # Companies  (new consolidated RawData format)
  "company_all_delimit.zip|${BOEM}/Company/Files/CompanyRawData.zip|Companies"

  # Leasing  (new consolidated RawData format)
  "lease_data_fixed.zip|${BOEM}/Leasing/Files/SerialRegRawData.zip|Lease Data (Serial Register)"
  "lease_list_fixed.zip|${BOEM}/Leasing/Files/LABRawData.zip|Lease List (Area/Block)"
  "lease_owner_op_delimit.zip|${BOEM}/Leasing/Files/LeaseOwnerRawData.zip|Lease Owners"

  # Platforms  (master/structure/location consolidated into PlatStruc)
  "platform_master_fixed.zip|${BOEM}/Platform/Files/PlatStrucRawData.zip|Platform Structures (master+location)"

  # Pipelines  (new consolidated RawData format)
  "pipeline_master_delimit.zip|${BOEM}/Pipeline/Files/PipePermRawData.zip|Pipeline Permits/Segments"
  "pipeline_location_delimit.zip|${BOEM}/Pipeline/Files/PipeLocRawData.zip|Pipeline Locations"

  # Fields & Appendices  (relocated to /FieldReserves/, OLD format preserved)
  "field_names_delimit.zip|${BOEM}/FieldReserves/Files/mastdatadelimit.zip|Field Names"
  "field_production_delimit.zip|${BOEM}/FieldReserves/Files/mastproddelimit.zip|Field Production"
  "appendix_a_delimit.zip|${BOEM}/FieldReserves/Files/appendadelimit.zip|Appendix A (Area/Block to Field)"
  "appendix_b_delimit.zip|${BOEM}/FieldReserves/Files/appendbdelimit.zip|Appendix B (Lease to Field)"
  "appendix_c_delimit.zip|${BOEM}/FieldReserves/Files/appendcdelimit.zip|Appendix C (Operator to Field)"

  # Production by Platform
  "ProdByPlatformRawData.zip|${BSEE}/Production/Files/ProdByPlatformRawData.zip|Production by Platform"
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

  # Download to a temp file and only replace the previous copy after the new
  # one verifies as a readable zip — a failed or truncated transfer must never
  # destroy the last good download.
  local tmp="${dest}.tmp"
  if [ -f "$dest" ]; then
    # Conditional GET against the existing file's mtime (304 = unchanged)
    local http_code
    http_code=$(curl -fsSL --retry 3 --retry-delay 5 -z "$dest" -o "$tmp" -w "%{http_code}" "$url") || http_code=""
    if [ "$http_code" = "304" ]; then
      rm -f "$tmp"
      log "    $(basename "$dest") (unchanged)"
      return 0
    fi
  else
    curl -fsSL --retry 3 --retry-delay 5 -o "$tmp" "$url" || true
  fi

  if [ -s "$tmp" ] && unzip -tqq "$tmp" >/dev/null 2>&1; then
    mv -f "$tmp" "$dest"
    local size
    size=$(du -h "$dest" | cut -f1)
    log "    $(basename "$dest") (${size})"
    return 0
  fi

  rm -f "$tmp"
  if [ -f "$dest" ]; then
    log "    FAILED: $(basename "$dest") (kept previous copy)"
  else
    log "    FAILED: $(basename "$dest")"
  fi
  return 1
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
      ok=$((ok+1))
    else
      fail=$((fail+1))
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
      ok=$((ok+1))
    else
      if [ "$year" -ge "$((current_year))" ]; then
        log "    (${year} may not be available yet)"
      else
        fail=$((fail+1))
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
    # Most BOEM/BSEE RawData zips nest their data files in a subdirectory
    # (e.g. BoreholeRawData/mv_boreholes_all.txt). Extract to a temp dir and
    # flatten all .txt/.DAT files to EXTRACTED_DIR root so loaders find them.
    local tmpdir
    tmpdir=$(mktemp -d)
    if ! unzip -qo "$zf" -d "$tmpdir" 2>/dev/null; then
      log "  WARNING: could not extract $(basename "$zf") — skipping"
      rm -rf "$tmpdir"
      continue
    fi
    find "$tmpdir" -type f \( -name "*.txt" -o -name "*.DAT" -o -name "*.dat" \) \
      -exec mv -f {} "$EXTRACTED_DIR/" \; 2>/dev/null || true
    rm -rf "$tmpdir"
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
