#!/usr/bin/env python3
"""
Export production and wells tables from boem.db to CSV files.
"""

import csv
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "boem.db"
OUT_DIR = Path(__file__).parent / "exports"


def export_table(conn, table_name):
    OUT_DIR.mkdir(exist_ok=True)
    out_path = OUT_DIR / f"{table_name}.csv"

    cursor = conn.execute(f"SELECT * FROM {table_name}")
    columns = [desc[0] for desc in cursor.description]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        count = 0
        while True:
            rows = cursor.fetchmany(10000)
            if not rows:
                break
            writer.writerows(rows)
            count += len(rows)

    size_mb = out_path.stat().st_size / 1e6
    print(f"  {table_name}.csv — {count:,} rows ({size_mb:.1f} MB)")


def main():
    conn = sqlite3.connect(str(DB_PATH))
    print("Exporting tables from boem.db...\n")

    export_table(conn, "production")
    export_table(conn, "wells")

    conn.close()
    print(f"\nFiles saved to {OUT_DIR}/")


if __name__ == "__main__":
    main()
