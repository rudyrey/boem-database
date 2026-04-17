#!/usr/bin/env bash
#
# BOEM CSV Updater
# Refreshes raw BOEM/BSEE data, rebuilds the SQLite database, and exports
# every table as a CSV file. Use this on machines where .db files aren't
# practical (e.g., PowerBI on an enterprise workstation) — point PowerBI
# at the ./exports/ folder and refresh.
#
# Usage:
#   ./update_csvs.sh              # Download + rebuild + export all CSVs
#   ./update_csvs.sh --export     # Export only (reuse existing boem.db)
#   ./update_csvs.sh --full       # Full rebuild from scratch + export
#   ./update_csvs.sh --help       # Show this help
#
# Requirements: bash, curl, python3, unzip

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DB_PATH="$SCRIPT_DIR/boem.db"
EXPORT_DIR="$SCRIPT_DIR/exports"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

export_csvs() {
  log "=========================================="
  log "Exporting all tables to CSV"
  log "=========================================="
  if [ ! -f "$DB_PATH" ]; then
    log "ERROR: $DB_PATH not found. Run without --export first to build it."
    exit 1
  fi
  python3 "$SCRIPT_DIR/export_csv.py"
  log ""
  log "CSVs written to: $EXPORT_DIR/"
}

show_help() {
  cat <<EOF
BOEM CSV Updater

Refreshes raw BOEM/BSEE data, rebuilds the SQLite database, and exports
every table as a CSV file under ./exports/. Intended for environments
where SQLite .db files aren't practical (e.g., PowerBI on an enterprise
workstation) — point PowerBI at the ./exports/ folder and refresh.

Usage:
  ./update_csvs.sh             Download + incremental rebuild + export
  ./update_csvs.sh --export    Export only (reuse existing boem.db)
  ./update_csvs.sh --full      Full rebuild from scratch + export
  ./update_csvs.sh --help      Show this help

Output:
  ./exports/<table_name>.csv   One CSV per table (UTF-8, with headers)

This script wraps ./update_database.sh — see that script's --help for
details on the download/rebuild stage.
EOF
}

case "${1:-all}" in
  --export|-e)
    export_csvs
    ;;
  --full|-f)
    "$SCRIPT_DIR/update_database.sh" --full
    export_csvs
    ;;
  --help|-h)
    show_help
    ;;
  all|"")
    "$SCRIPT_DIR/update_database.sh"
    export_csvs
    ;;
  *)
    echo "Unknown: $1 (try --help)"
    exit 1
    ;;
esac

log "Done!"
