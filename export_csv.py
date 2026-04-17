#!/usr/bin/env python3
"""
Export every table from boem.db to individual CSV files under ./exports/.

Used for environments without SQLite tooling (e.g., PowerBI on enterprise
workstations) where CSVs are the only practical data source.
"""

import csv
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent / "boem.db"
OUT_DIR = Path(__file__).parent / "exports"
BATCH_SIZE = 10000


def list_tables(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def export_table(conn, table_name):
    out_path = OUT_DIR / f"{table_name}.csv"
    cursor = conn.execute(f'SELECT * FROM "{table_name}"')
    columns = [desc[0] for desc in cursor.description]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        count = 0
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            writer.writerows(rows)
            count += len(rows)

    size_mb = out_path.stat().st_size / 1e6
    print(f"  {table_name}.csv — {count:>12,} rows ({size_mb:>7,.1f} MB)")
    return count, size_mb


def main():
    if not DB_PATH.exists():
        sys.exit(f"ERROR: {DB_PATH} not found. Build it with ./update_database.sh")

    OUT_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    tables = list_tables(conn)
    print(f"Exporting {len(tables)} tables from boem.db to {OUT_DIR}/\n")

    total_rows = 0
    total_mb = 0.0
    for t in tables:
        rows, mb = export_table(conn, t)
        total_rows += rows
        total_mb += mb

    conn.close()
    print(f"\n{len(tables)} tables, {total_rows:,} rows, {total_mb:,.1f} MB total")


if __name__ == "__main__":
    main()
