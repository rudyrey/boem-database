#!/usr/bin/env python3
"""
BOEM Relational Database Builder
Parses all downloaded BOEM flat files and assembles them into a SQLite database.

Data sources:
  - Boreholes/Wells (delimited)
  - Companies (delimited)
  - Leases (fixed-width)
  - Lease Owners w/ Designated Operator (delimited)
  - Platform Masters (fixed-width)
  - Platform Structures (fixed-width)
  - Platform Locations (fixed-width)
  - Platform Approvals (delimited)
  - Platform Structures Removed (delimited)
  - Pipeline Masters (delimited)
  - Pipeline Locations (delimited)
  - Field Names (delimited)
  - Field Production (delimited)
  - OGOR-A Production (delimited, 1996-2025)
  - Rig ID List (delimited)
  - APD / Permits to Drill (delimited)
  - Lease List (fixed-width)
  - Appendix A: Area/Block to Field (delimited)
  - Appendix B: Lease to Field (delimited)
  - Appendix C: Operator to Field (delimited)
"""

import csv
import glob
import os
import re
import sqlite3
import sys
import zipfile
from pathlib import Path

BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "raw_data"
EXTRACTED_DIR = RAW_DIR / "extracted"
DB_PATH = BASE_DIR / "boem.db"


def s(val):
    """Strip whitespace and quotes from a value, return None if empty."""
    if val is None:
        return None
    v = val.strip().strip('"').strip()
    return v if v else None


def to_int(val):
    """Convert to int, return None on failure."""
    v = s(val)
    if v is None:
        return None
    try:
        return int(v)
    except ValueError:
        return None


def to_float(val):
    """Convert to float, return None on failure."""
    v = s(val)
    if v is None:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def parse_date_yyyymmdd(val):
    """Parse YYYYMMDD to YYYY-MM-DD, return None if invalid."""
    v = s(val)
    if v is None or len(v) < 8:
        return None
    v = v.replace("/", "").replace("-", "")
    if len(v) < 8 or not v[:8].isdigit():
        return None
    y, m, d = v[:4], v[4:6], v[6:8]
    if y == "0000" or m == "00" or d == "00":
        return None
    return f"{y}-{m}-{d}"


def parse_date_mmddyyyy(val):
    """Parse MM/DD/YYYY to YYYY-MM-DD."""
    v = s(val)
    if v is None:
        return None
    parts = v.split("/")
    if len(parts) == 3:
        m, d, y = parts
        if y.strip() and m.strip() and d.strip():
            return f"{y.strip()}-{m.strip().zfill(2)}-{d.strip().zfill(2)}"
    return None


def parse_date_mdy(val):
    """Parse M/D/YYYY (with optional time) to YYYY-MM-DD."""
    v = s(val)
    if v is None:
        return None
    # Strip time portion if present (e.g. "3/27/2024 7:30:24 AM")
    date_part = v.split(" ")[0] if " " in v else v
    parts = date_part.split("/")
    if len(parts) == 3:
        m, d, y = parts
        if y.strip() and m.strip() and d.strip():
            return f"{y.strip()}-{m.strip().zfill(2)}-{d.strip().zfill(2)}"
    return None


def parse_date_mon_year(val):
    """Parse MON-YYYY (e.g. JAN-2020) to YYYY-MM."""
    v = s(val)
    if v is None:
        return None
    months = {
        "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
        "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
        "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
    }
    parts = v.split("-")
    if len(parts) == 2:
        mon = months.get(parts[0].upper())
        yr = parts[1].strip()
        if mon and yr and yr.isdigit():
            return f"{yr}-{mon}"
    return None


def parse_delimited_file(filepath, encoding="latin-1"):
    """Parse a comma-delimited file with quoted fields. No header row."""
    rows = []
    with open(filepath, "r", encoding=encoding) as f:
        reader = csv.reader(f, delimiter=",", quotechar='"')
        for row in reader:
            rows.append([field.strip() for field in row])
    return rows


def parse_fixed_width(filepath, spec, encoding="latin-1"):
    """Parse a fixed-width file given a spec of (start_1based, length, name) tuples."""
    rows = []
    with open(filepath, "r", encoding=encoding) as f:
        for line in f:
            if not line.strip():
                continue
            row = {}
            for start, length, name in spec:
                idx = start - 1
                val = line[idx : idx + length].strip() if idx < len(line) else ""
                row[name] = val if val else None
            rows.append(row)
    return rows


def extract_if_needed():
    """Extract all zip files to the extracted directory."""
    EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)
    for zf in RAW_DIR.glob("*.zip"):
        with zipfile.ZipFile(zf, "r") as z:
            z.extractall(EXTRACTED_DIR)
    print(f"Extracted all zip files to {EXTRACTED_DIR}")


def create_schema(conn):
    """Create all database tables."""
    conn.executescript("""
    -- ========================================================
    -- REFERENCE TABLES
    -- ========================================================

    CREATE TABLE IF NOT EXISTS companies (
        company_num         TEXT PRIMARY KEY,
        company_name        TEXT,
        sort_name           TEXT,
        start_date          TEXT,
        term_date           TEXT,
        pacific_region      TEXT,
        gom_region          TEXT,
        alaska_region       TEXT,
        atlantic_region     TEXT,
        duns_number         TEXT,
        term_action_date    TEXT,
        termination_code    TEXT,
        division_name       TEXT,
        address_line1       TEXT,
        address_line2       TEXT,
        city                TEXT,
        state_code          TEXT,
        zip_code            TEXT,
        country             TEXT
    );

    CREATE TABLE IF NOT EXISTS rigs (
        rig_id      TEXT PRIMARY KEY,
        rig_name    TEXT,
        rig_type    TEXT
    );

    -- ========================================================
    -- FIELD / RESERVES
    -- ========================================================

    CREATE TABLE IF NOT EXISTS fields (
        field_name_code     TEXT,
        lease_number        TEXT,
        area_code           TEXT,
        block_number        TEXT,
        eia_code            TEXT,
        designated_operator TEXT,
        effective_date      TEXT,
        termination_date    TEXT,
        termination_code    TEXT,
        lease_portion       TEXT,
        PRIMARY KEY (field_name_code, lease_number)
    );

    CREATE TABLE IF NOT EXISTS field_production (
        field_name_code         TEXT,
        lease_number            TEXT,
        cum_oil_volume          REAL,
        cum_gas_volume_1        REAL,
        cum_gas_volume_2        REAL,
        cum_cond_volume         REAL,
        cum_water_volume_1      REAL,
        cum_water_volume_2      REAL,
        cum_boe                 REAL,
        first_production_date   TEXT,
        PRIMARY KEY (field_name_code, lease_number)
    );

    CREATE TABLE IF NOT EXISTS area_block_to_field (
        area_code       TEXT,
        block_number    TEXT,
        field_name_code TEXT,
        lease_number    TEXT,
        area_block      TEXT
    );

    CREATE TABLE IF NOT EXISTS lease_to_field (
        lease_number    TEXT,
        field_name_code TEXT,
        area_code       TEXT,
        block_number    TEXT,
        area_block      TEXT
    );

    CREATE TABLE IF NOT EXISTS operator_to_field (
        operator_name   TEXT,
        field_name_code TEXT
    );

    -- ========================================================
    -- LEASING
    -- ========================================================

    CREATE TABLE IF NOT EXISTS leases (
        lease_number            TEXT PRIMARY KEY,
        serial_type_code        TEXT,
        sale_number             TEXT,
        expected_expiration     TEXT,
        api_state_county        TEXT,
        tract_number            TEXT,
        effective_date          TEXT,
        primary_term            INTEGER,
        expiration_date         TEXT,
        bid_system_code         TEXT,
        royalty_rate             REAL,
        initial_area            REAL,
        current_area            REAL,
        rent_per_unit           REAL,
        bid_amount              REAL,
        bid_per_unit            REAL,
        min_water_depth         INTEGER,
        max_water_depth         INTEGER,
        measure_flag            TEXT,
        planning_area_code      TEXT,
        district_code           TEXT,
        lease_status            TEXT,
        status_effective_date   TEXT,
        suspension_expiration   TEXT,
        suspension_type         TEXT,
        well_name               TEXT,
        qualifying_well_type    TEXT,
        qualifying_date         TEXT,
        discovery_type          TEXT,
        field_discovery         TEXT,
        distance_to_shore       INTEGER,
        num_platforms           INTEGER,
        platform_approval_date  TEXT,
        first_platform_set_date TEXT,
        lease_section_code      TEXT,
        postal_state_code       TEXT,
        lease_section_area      REAL,
        protraction_number      TEXT,
        suspension_eff_date     TEXT,
        first_production_date   TEXT,
        area_code               TEXT,
        block_number            TEXT
    );

    CREATE TABLE IF NOT EXISTS lease_list (
        lease_number        TEXT,
        district_code       TEXT,
        appeal_flag         TEXT,
        pending_flag        TEXT,
        mineral_type        TEXT,
        area_block          TEXT,
        multi_partial       TEXT,
        lease_status        TEXT,
        status_date         TEXT,
        order4_det          TEXT,
        status_flag         TEXT,
        designated_operator TEXT
    );

    CREATE TABLE IF NOT EXISTS lease_owners (
        lease_number        TEXT,
        company_num         TEXT,
        assignment_pct      REAL,
        assignment_approval TEXT,
        assignment_effective TEXT,
        assignment_term     TEXT,
        assignment_status   TEXT,
        owner_aliquot       TEXT,
        owner_group         TEXT,
        designated_operator TEXT
    );

    -- ========================================================
    -- PLATFORMS / STRUCTURES
    -- ========================================================

    CREATE TABLE IF NOT EXISTS platforms (
        complex_id          TEXT PRIMARY KEY,
        company_num         TEXT,
        lease_number        TEXT,
        area_code           TEXT,
        block_number        TEXT,
        field_name_code     TEXT,
        district_code       TEXT,
        water_depth         INTEGER,
        distance_to_shore   INTEGER,
        oil_producing       TEXT,
        gas_producing       TEXT,
        water_producing     TEXT,
        condensate_producing TEXT,
        drilling            TEXT,
        manned_24hr         TEXT,
        attended_8hr        TEXT,
        heliport            TEXT,
        sulfur_producing    TEXT,
        compressor          TEXT,
        workover            TEXT,
        injection_code      TEXT,
        production_flag     TEXT,
        prod_equipment      TEXT,
        power_source        TEXT,
        power_gen           TEXT,
        major_complex       TEXT,
        rig_count           INTEGER,
        crane_count         INTEGER,
        bed_count           INTEGER,
        subdistrict_code    TEXT,
        last_revision_date  TEXT
    );

    CREATE TABLE IF NOT EXISTS platform_structures (
        complex_id          TEXT,
        structure_number    TEXT,
        structure_name      TEXT,
        structure_type      TEXT,
        area_code           TEXT,
        block_number        TEXT,
        install_date        TEXT,
        removal_date        TEXT,
        deck_count          INTEGER,
        slot_count          INTEGER,
        slant_slot_count    INTEGER,
        slot_drill_count    INTEGER,
        satellite_count     INTEGER,
        underwater_count    INTEGER,
        major_structure     TEXT,
        ns_departure        TEXT,
        ew_departure        TEXT,
        ns_distance         TEXT,
        ew_distance         TEXT,
        authority_type      TEXT,
        authority_number    TEXT,
        authority_status    TEXT,
        last_revision_date  TEXT,
        PRIMARY KEY (complex_id, structure_number)
    );

    CREATE TABLE IF NOT EXISTS platform_locations (
        complex_id      TEXT,
        structure_number TEXT,
        district_code    TEXT,
        area_code        TEXT,
        block_number     TEXT,
        structure_name   TEXT,
        longitude        REAL,
        latitude         REAL,
        x_location       REAL,
        y_location       REAL,
        ns_distance      TEXT,
        ns_code          TEXT,
        ew_distance      TEXT,
        ew_code          TEXT,
        PRIMARY KEY (complex_id, structure_number)
    );

    CREATE TABLE IF NOT EXISTS platform_approvals (
        application_date    TEXT,
        application_number  TEXT,
        company_name        TEXT,
        company_num         TEXT,
        temp_structure      TEXT,
        expiration_date     TEXT,
        lease_number        TEXT,
        area_code           TEXT,
        block_number        TEXT,
        structure_name      TEXT,
        ns_distance         TEXT,
        ns_code             TEXT,
        ew_distance         TEXT,
        ew_code             TEXT,
        water_depth         INTEGER,
        approved_date       TEXT,
        pvp_flag            TEXT
    );

    CREATE TABLE IF NOT EXISTS platform_removals (
        company_name        TEXT,
        company_num         TEXT,
        application_number  TEXT,
        received_date       TEXT,
        final_action_date   TEXT,
        removal_date        TEXT,
        site_clearance_date TEXT,
        submittal_type      TEXT,
        lease_number        TEXT,
        area_code           TEXT,
        block_number        TEXT,
        structure_name      TEXT,
        proposed_removal_date TEXT,
        removal_method      TEXT,
        district_code       TEXT,
        complex_id          TEXT,
        structure_number    TEXT,
        water_depth         INTEGER
    );

    -- ========================================================
    -- WELLS / BOREHOLES
    -- ========================================================

    CREATE TABLE IF NOT EXISTS wells (
        api_well_number     TEXT PRIMARY KEY,
        well_name           TEXT,
        well_name_suffix    TEXT,
        operator_num        TEXT,
        bottom_field_code   TEXT,
        spud_date           TEXT,
        bottom_lease_number TEXT,
        rkb_elevation       INTEGER,
        total_measured_depth INTEGER,
        true_vertical_depth INTEGER,
        water_depth         INTEGER,
        surface_longitude   REAL,
        surface_latitude    REAL,
        bottom_longitude    REAL,
        bottom_latitude     REAL,
        status_code         TEXT,
        type_code           TEXT,
        well_class          TEXT,
        district_code       TEXT,
        area_block          TEXT,
        completion_date     TEXT,
        plugback_date       TEXT
    );

    -- ========================================================
    -- PIPELINES
    -- ========================================================

    CREATE TABLE IF NOT EXISTS pipelines (
        segment_num         TEXT PRIMARY KEY,
        segment_length      REAL,
        origin_name         TEXT,
        origin_area         TEXT,
        origin_block        TEXT,
        origin_lease        TEXT,
        dest_name           TEXT,
        dest_area           TEXT,
        dest_block          TEXT,
        dest_lease          TEXT,
        abandon_approval    TEXT,
        abandon_date        TEXT,
        approved_date       TEXT,
        auth_code           TEXT,
        boarding_sdv        TEXT,
        buried_flag         TEXT,
        cathodic_life       INTEGER,
        flow_direction      TEXT,
        construction_date   TEXT,
        leak_detection      TEXT,
        last_revision       TEXT,
        hydrotest_date      TEXT,
        fed_state_length    REAL,
        status_code         TEXT,
        pipe_size           TEXT,
        row_number          TEXT,
        recv_maop           REAL,
        recv_segment        TEXT,
        proposed_const_date TEXT,
        product_code        TEXT,
        system_code         TEXT,
        row_permittee       TEXT,
        facility_operator   TEXT,
        min_water_depth     INTEGER,
        max_water_depth     INTEGER,
        protraction_number  TEXT,
        maop_pressure       REAL,
        cathodic_code       TEXT,
        bidirectional       TEXT,
        boarding_fsv        TEXT,
        approval_code       TEXT,
        abandon_type        TEXT
    );

    CREATE TABLE IF NOT EXISTS pipeline_locations (
        segment_num     TEXT,
        point_seq       INTEGER,
        latitude        REAL,
        longitude       REAL,
        nad_year        TEXT,
        proj_code       TEXT,
        x_coord         REAL,
        y_coord         REAL,
        last_revision   TEXT,
        version_date    TEXT,
        asbuilt_flag    TEXT,
        PRIMARY KEY (segment_num, point_seq)
    );

    -- ========================================================
    -- PRODUCTION (OGOR-A)
    -- ========================================================

    CREATE TABLE IF NOT EXISTS apd (
        sn_apd              TEXT PRIMARY KEY,
        api_well_number     TEXT,
        operator_num        TEXT,
        well_name           TEXT,
        permit_type         TEXT,
        well_type_code      TEXT,
        water_depth         INTEGER,
        req_spud_date       TEXT,
        apd_status_dt       TEXT,
        apd_sub_status_dt   TEXT,
        surf_area_code      TEXT,
        surf_block_number   TEXT,
        surf_lease_number   TEXT,
        botm_area_code      TEXT,
        botm_block_number   TEXT,
        botm_lease_number   TEXT,
        rig_name            TEXT,
        rig_type_code       TEXT,
        rig_id_num          TEXT,
        bus_asc_name        TEXT
    );

    CREATE TABLE IF NOT EXISTS apm (
        sn_apm              TEXT PRIMARY KEY,
        api_well_number     TEXT,
        operator_num        TEXT,
        well_name           TEXT,
        well_type_code      TEXT,
        water_depth         INTEGER,
        borehole_stat_cd    TEXT,
        apm_op_cd           TEXT,
        acc_status_date     TEXT,
        sub_stat_date       TEXT,
        surf_area_code      TEXT,
        surf_block_num      TEXT,
        surf_lease_num      TEXT,
        botm_area_code      TEXT,
        botm_block_num      TEXT,
        botm_lease_num      TEXT,
        rig_id_num          TEXT,
        bus_asc_name        TEXT,
        sv_type             TEXT,
        est_operation_days  INTEGER,
        work_commences_date TEXT
    );

    CREATE TABLE IF NOT EXISTS production (
        lease_number        TEXT,
        completion_name     TEXT,
        production_date     TEXT,
        days_on_production  INTEGER,
        product_code        TEXT,
        oil_volume          REAL,
        gas_volume          REAL,
        water_volume        REAL,
        api_well_number     TEXT,
        well_status         TEXT,
        area_block          TEXT,
        operator_num        TEXT,
        operator_name       TEXT,
        field_name_code     TEXT,
        injection_volume    REAL,
        prod_interval_code  TEXT,
        first_prod_date     TEXT,
        unit_agreement      TEXT,
        unit_alloc_suffix   TEXT
    );

    -- ========================================================
    -- INDEXES
    -- ========================================================

    CREATE INDEX IF NOT EXISTS idx_wells_operator ON wells(operator_num);
    CREATE INDEX IF NOT EXISTS idx_wells_spud ON wells(spud_date);
    CREATE INDEX IF NOT EXISTS idx_wells_area ON wells(area_block);
    CREATE INDEX IF NOT EXISTS idx_wells_lease ON wells(bottom_lease_number);

    CREATE INDEX IF NOT EXISTS idx_leases_area ON leases(area_code, block_number);
    CREATE INDEX IF NOT EXISTS idx_leases_status ON leases(lease_status);

    CREATE INDEX IF NOT EXISTS idx_lease_owners_lease ON lease_owners(lease_number);
    CREATE INDEX IF NOT EXISTS idx_lease_owners_company ON lease_owners(company_num);

    CREATE INDEX IF NOT EXISTS idx_platforms_area ON platforms(area_code, block_number);
    CREATE INDEX IF NOT EXISTS idx_platforms_company ON platforms(company_num);
    CREATE INDEX IF NOT EXISTS idx_platforms_lease ON platforms(lease_number);

    CREATE INDEX IF NOT EXISTS idx_platform_structures_area ON platform_structures(area_code, block_number);
    CREATE INDEX IF NOT EXISTS idx_platform_locations_coords ON platform_locations(longitude, latitude);

    CREATE INDEX IF NOT EXISTS idx_pipelines_origin ON pipelines(origin_area, origin_block);
    CREATE INDEX IF NOT EXISTS idx_pipelines_dest ON pipelines(dest_area, dest_block);
    CREATE INDEX IF NOT EXISTS idx_pipelines_status ON pipelines(status_code);

    CREATE INDEX IF NOT EXISTS idx_pipeline_locs_seg ON pipeline_locations(segment_num);

    CREATE INDEX IF NOT EXISTS idx_apd_api ON apd(api_well_number);

    CREATE INDEX IF NOT EXISTS idx_apm_api ON apm(api_well_number);

    CREATE INDEX IF NOT EXISTS idx_production_lease ON production(lease_number);
    CREATE INDEX IF NOT EXISTS idx_production_api ON production(api_well_number);
    CREATE INDEX IF NOT EXISTS idx_production_date ON production(production_date);
    CREATE INDEX IF NOT EXISTS idx_production_field ON production(field_name_code);
    CREATE INDEX IF NOT EXISTS idx_production_operator ON production(operator_num);

    CREATE INDEX IF NOT EXISTS idx_fields_lease ON fields(lease_number);
    CREATE INDEX IF NOT EXISTS idx_fields_area ON fields(area_code, block_number);

    CREATE INDEX IF NOT EXISTS idx_lease_list_area ON lease_list(area_block);
    """)


# ========================================================
# LOADERS
# ========================================================

def load_companies(conn):
    print("Loading companies...")
    rows = parse_delimited_file(EXTRACTED_DIR / "compalldelimit.txt")
    data = []
    for r in rows:
        if len(r) < 5:
            continue
        data.append((
            s(r[0]),           # company_num
            s(r[2]),           # company_name (bus_asc_name)
            s(r[3]),           # sort_name
            parse_date_yyyymmdd(r[1]),  # start_date
            parse_date_yyyymmdd(r[4]),  # term_date
            s(r[5]) if len(r) > 5 else None,   # pacific
            s(r[6]) if len(r) > 6 else None,   # gom
            s(r[7]) if len(r) > 7 else None,   # alaska
            s(r[8]) if len(r) > 8 else None,   # atlantic
            s(r[9]) if len(r) > 9 else None,   # duns
            parse_date_yyyymmdd(r[10]) if len(r) > 10 else None,  # term_action
            s(r[11]) if len(r) > 11 else None,  # term_code
            s(r[12]) if len(r) > 12 else None,  # division
            s(r[13]) if len(r) > 13 else None,  # addr1
            s(r[14]) if len(r) > 14 else None,  # addr2
            s(r[15]) if len(r) > 15 else None,  # city
            s(r[16]) if len(r) > 16 else None,  # state
            s(r[17]) if len(r) > 17 else None,  # zip
            s(r[18]) if len(r) > 18 else None,  # country
        ))
    conn.executemany("""INSERT OR REPLACE INTO companies VALUES
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", data)
    print(f"  -> {len(data)} companies loaded")


def load_rigs(conn):
    print("Loading rigs...")
    rows = parse_delimited_file(EXTRACTED_DIR / "rigidlistdelimit.txt")
    data = [(s(r[0]), s(r[1]), s(r[2]) if len(r) > 2 else None) for r in rows if len(r) >= 2]
    conn.executemany("INSERT OR REPLACE INTO rigs VALUES (?,?,?)", data)
    print(f"  -> {len(data)} rigs loaded")


def load_fields(conn):
    print("Loading fields...")
    rows = parse_delimited_file(EXTRACTED_DIR / "mastdatadelimit.txt")
    data = []
    for r in rows:
        if len(r) < 6:
            continue
        data.append((
            s(r[0]),  # field_name_code
            s(r[1]),  # lease_number
            s(r[2]),  # area_code
            s(r[3]),  # block_number
            s(r[4]),  # eia_code
            s(r[5]),  # designated_operator
            parse_date_mon_year(r[6]) if len(r) > 6 else None,
            parse_date_mon_year(r[7]) if len(r) > 7 else None,
            s(r[8]) if len(r) > 8 else None,
            s(r[9]) if len(r) > 9 else None,
        ))
    conn.executemany("INSERT OR REPLACE INTO fields VALUES (?,?,?,?,?,?,?,?,?,?)", data)
    print(f"  -> {len(data)} field-lease records loaded")


def load_field_production(conn):
    print("Loading field production...")
    rows = parse_delimited_file(EXTRACTED_DIR / "mastproddelimit.txt")
    data = []
    for r in rows:
        if len(r) < 9:
            continue
        data.append((
            s(r[0]),       # field_name_code
            s(r[1]),       # lease_number
            to_float(r[2]),  # cum_oil
            to_float(r[3]),
            to_float(r[4]),
            to_float(r[5]),
            to_float(r[6]),
            to_float(r[7]),
            to_float(r[8]),
            parse_date_mon_year(r[9]) if len(r) > 9 else None,
        ))
    conn.executemany("INSERT OR REPLACE INTO field_production VALUES (?,?,?,?,?,?,?,?,?,?)", data)
    print(f"  -> {len(data)} field production records loaded")


def load_appendices(conn):
    print("Loading appendix A (area/block to field)...")
    rows = parse_delimited_file(EXTRACTED_DIR / "appendadelimit.txt")
    data = [(s(r[0]), s(r[1]), s(r[2]), s(r[3]), s(r[4]) if len(r) > 4 else None) for r in rows if len(r) >= 4]
    conn.executemany("INSERT INTO area_block_to_field VALUES (?,?,?,?,?)", data)
    print(f"  -> {len(data)} records")

    print("Loading appendix B (lease to field)...")
    rows = parse_delimited_file(EXTRACTED_DIR / "appendbdelimit.txt")
    data = [(s(r[0]), s(r[1]), s(r[2]), s(r[3]), s(r[4]) if len(r) > 4 else None) for r in rows if len(r) >= 4]
    conn.executemany("INSERT INTO lease_to_field VALUES (?,?,?,?,?)", data)
    print(f"  -> {len(data)} records")

    print("Loading appendix C (operator to field)...")
    rows = parse_delimited_file(EXTRACTED_DIR / "appendcdelimit.txt")
    data = [(s(r[0]), s(r[1])) for r in rows if len(r) >= 2]
    conn.executemany("INSERT INTO operator_to_field VALUES (?,?)", data)
    print(f"  -> {len(data)} records")


def load_leases(conn):
    print("Loading leases (fixed-width)...")
    spec = [
        (1, 7, "lease_number"),
        (16, 1, "serial_type_code"),
        (17, 7, "sale_number"),
        (28, 8, "expected_expiration"),
        (36, 5, "api_state_county"),
        (41, 10, "tract_number"),
        (51, 8, "effective_date"),
        (59, 2, "primary_term"),
        (61, 8, "expiration_date"),
        (69, 5, "bid_system_code"),
        (74, 10, "royalty_rate"),
        (87, 14, "initial_area"),
        (101, 14, "current_area"),
        (115, 8, "rent_per_unit"),
        (123, 13, "bid_amount"),
        (136, 13, "bid_per_unit"),
        (150, 5, "min_water_depth"),
        (155, 5, "max_water_depth"),
        (160, 1, "measure_flag"),
        (161, 3, "planning_area"),
        (166, 2, "district_code"),
        (171, 6, "lease_status"),
        (177, 8, "status_eff_date"),
        (185, 8, "suspension_exp"),
        (193, 1, "suspension_type"),
        (194, 6, "well_name"),
        (200, 1, "qualifying_well_type"),
        (201, 8, "qualifying_date"),
        (209, 3, "discovery_type"),
        (212, 1, "field_discovery"),
        (213, 3, "distance_to_shore"),
        (217, 3, "num_platforms"),
        (220, 8, "platform_approval"),
        (228, 8, "first_platform_set"),
        (236, 2, "lease_section"),
        (238, 4, "postal_state"),
        (242, 12, "lease_section_area"),
        (254, 7, "protraction_number"),
        (269, 8, "suspension_eff"),
        (277, 8, "first_production"),
        (289, 2, "area_code"),
        (293, 6, "block_number"),
    ]
    rows = parse_fixed_width(EXTRACTED_DIR / "LSETAPE.DAT", spec)
    data = []
    for r in rows:
        data.append((
            r["lease_number"],
            r["serial_type_code"],
            r["sale_number"],
            parse_date_yyyymmdd(r["expected_expiration"]),
            r["api_state_county"],
            r["tract_number"],
            parse_date_yyyymmdd(r["effective_date"]),
            to_int(r["primary_term"]),
            parse_date_yyyymmdd(r["expiration_date"]),
            r["bid_system_code"],
            to_float(r["royalty_rate"]),
            to_float(r["initial_area"]),
            to_float(r["current_area"]),
            to_float(r["rent_per_unit"]),
            to_float(r["bid_amount"]),
            to_float(r["bid_per_unit"]),
            to_int(r["min_water_depth"]),
            to_int(r["max_water_depth"]),
            r["measure_flag"],
            r["planning_area"],
            r["district_code"],
            r["lease_status"],
            parse_date_yyyymmdd(r["status_eff_date"]),
            parse_date_yyyymmdd(r["suspension_exp"]),
            r["suspension_type"],
            r["well_name"],
            r["qualifying_well_type"],
            parse_date_yyyymmdd(r["qualifying_date"]),
            r["discovery_type"],
            r["field_discovery"],
            to_int(r["distance_to_shore"]),
            to_int(r["num_platforms"]),
            parse_date_yyyymmdd(r["platform_approval"]),
            parse_date_yyyymmdd(r["first_platform_set"]),
            r["lease_section"],
            r["postal_state"],
            to_float(r["lease_section_area"]),
            r["protraction_number"],
            parse_date_yyyymmdd(r["suspension_eff"]),
            parse_date_yyyymmdd(r["first_production"]),
            r["area_code"],
            r["block_number"],
        ))
    conn.executemany(f"INSERT OR REPLACE INTO leases VALUES ({','.join('?' * 42)})", data)
    print(f"  -> {len(data)} leases loaded")


def load_lease_list(conn):
    print("Loading lease list (fixed-width)...")
    spec = [
        (1, 7, "lease_number"),
        (8, 2, "district_code"),
        (10, 1, "appeal_flag"),
        (11, 1, "pending_flag"),
        (12, 4, "mineral_type"),
        (16, 8, "area_block"),
        (24, 1, "multi_partial"),
        (25, 6, "lease_status"),
        (31, 10, "status_date"),
        (41, 1, "order4_det"),
        (42, 1, "status_flag"),
        (43, 50, "designated_operator"),
    ]
    rows = parse_fixed_width(EXTRACTED_DIR / "LSTLEASE.DAT", spec)
    data = []
    for r in rows:
        data.append((
            r["lease_number"],
            r["district_code"],
            r["appeal_flag"],
            r["pending_flag"],
            r["mineral_type"],
            r["area_block"],
            r["multi_partial"],
            r["lease_status"],
            r["status_date"],
            r["order4_det"],
            r["status_flag"],
            r["designated_operator"],
        ))
    conn.executemany("INSERT INTO lease_list VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", data)
    print(f"  -> {len(data)} lease list records loaded")


def load_lease_owners(conn):
    print("Loading lease owners w/ designated operator...")
    rows = parse_delimited_file(EXTRACTED_DIR / "lseownddelimit.txt")
    data = []
    for r in rows:
        if len(r) < 7:
            continue
        data.append((
            s(r[0]),                    # lease_number
            s(r[5]),                    # company_num
            to_float(r[6]),             # assignment_pct
            parse_date_yyyymmdd(r[1]),  # approval_date
            parse_date_yyyymmdd(r[2]),  # effective_date
            None,                       # term_date (not in this file)
            s(r[3]),                    # assignment_status
            None,                       # owner_aliquot
            None,                       # owner_group
            s(r[8]) if len(r) > 8 else None,  # designated_operator
        ))
    conn.executemany("INSERT INTO lease_owners VALUES (?,?,?,?,?,?,?,?,?,?)", data)
    print(f"  -> {len(data)} lease owner records loaded")


def load_platforms(conn):
    print("Loading platform masters (fixed-width)...")
    spec = [
        (1, 8, "complex_id"),
        (9, 1, "abandon_flag"),
        (10, 1, "alloc_meter"),
        (11, 1, "attended_8hr"),
        (12, 1, "condensate"),
        (13, 4, "distance_to_shore"),
        (17, 1, "drilling"),
        (18, 1, "fired_vessel"),
        (19, 1, "gas_prod"),
        (20, 1, "gas_flaring"),
        (21, 5, "company_num"),
        (26, 1, "manned_24hr"),
        (27, 1, "major_complex"),
        (28, 7, "lease_number"),
        (35, 11, "last_rev_date"),
        (46, 1, "lact_meter"),
        (47, 1, "injection_code"),
        (48, 1, "heliport"),
        (49, 1, "workover"),
        (50, 1, "water_prod"),
        (51, 5, "water_depth"),
        (56, 1, "tank_gauge"),
        (57, 1, "sulfur_prod"),
        (58, 2, "subdistrict"),
        (60, 1, "store_tank"),
        (61, 2, "rig_count"),
        (63, 1, "qtr_type"),
        (64, 1, "prod_eqmt"),
        (65, 1, "production"),
        (66, 1, "power_source"),
        (67, 1, "power_gen"),
        (68, 1, "oil_prod"),
        (69, 1, "gas_sale_meter"),
        (70, 8, "field_name_code"),
        (78, 3, "district_code"),
        (81, 2, "crane_count"),
        (83, 1, "compressor"),
        (84, 1, "comgl_prod"),
        (85, 3, "bed_count"),
        (88, 2, "area_code"),
        (90, 6, "block_number"),
        (96, 1, "meter_prover"),
    ]
    rows = parse_fixed_width(EXTRACTED_DIR / "platmast.DAT", spec)
    data = []
    for r in rows:
        data.append((
            s(r["complex_id"]),
            s(r["company_num"]),
            s(r["lease_number"]),
            s(r["area_code"]),
            s(r["block_number"]),
            s(r["field_name_code"]),
            s(r["district_code"]),
            to_int(r["water_depth"]),
            to_int(r["distance_to_shore"]),
            r["oil_prod"],
            r["gas_prod"],
            r["water_prod"],
            r["condensate"],
            r["drilling"],
            r["manned_24hr"],
            r["attended_8hr"],
            r["heliport"],
            r["sulfur_prod"],
            r["compressor"],
            r["workover"],
            r["injection_code"],
            r["production"],
            r["prod_eqmt"],
            r["power_source"],
            r["power_gen"],
            r["major_complex"],
            to_int(r["rig_count"]),
            to_int(r["crane_count"]),
            to_int(r["bed_count"]),
            r["subdistrict"],
            parse_date_mon_year(r["last_rev_date"]) if r["last_rev_date"] else None,
        ))
    conn.executemany(f"INSERT OR REPLACE INTO platforms VALUES ({','.join('?' * 31)})", data)
    print(f"  -> {len(data)} platforms loaded")


def load_platform_structures(conn):
    print("Loading platform structures (fixed-width)...")
    spec = [
        (1, 2, "area_code"),
        (3, 6, "block_number"),
        (9, 8, "complex_id"),
        (17, 2, "deck_count"),
        (19, 1, "ew_departure"),
        (20, 11, "install_date"),
        (31, 11, "last_revision"),
        (42, 1, "major_structure"),
        (43, 1, "ns_departure"),
        (44, 11, "removal_date"),
        (55, 3, "slant_slot_count"),
        (58, 3, "slot_count"),
        (61, 3, "slot_drill_count"),
        (64, 3, "satellite_count"),
        (67, 15, "structure_name"),
        (82, 3, "structure_number"),
        (85, 5, "structure_type"),
        (90, 6, "ew_distance"),
        (96, 6, "ns_distance"),
        (102, 3, "underwater_count"),
        (105, 16, "authority_type"),
        (121, 8, "authority_number"),
        (129, 20, "authority_status"),
    ]
    rows = parse_fixed_width(EXTRACTED_DIR / "platstru.DAT", spec)
    data = []
    for r in rows:
        data.append((
            s(r["complex_id"]),
            s(r["structure_number"]),
            s(r["structure_name"]),
            s(r["structure_type"]),
            s(r["area_code"]),
            s(r["block_number"]),
            parse_date_mon_year(r["install_date"]) if r["install_date"] else parse_date_yyyymmdd(r["install_date"]),
            parse_date_mon_year(r["removal_date"]) if r["removal_date"] else parse_date_yyyymmdd(r["removal_date"]),
            to_int(r["deck_count"]),
            to_int(r["slot_count"]),
            to_int(r["slant_slot_count"]),
            to_int(r["slot_drill_count"]),
            to_int(r["satellite_count"]),
            to_int(r["underwater_count"]),
            r["major_structure"],
            r["ns_departure"],
            r["ew_departure"],
            s(r["ns_distance"]),
            s(r["ew_distance"]),
            s(r["authority_type"]),
            s(r["authority_number"]),
            s(r["authority_status"]),
            parse_date_mon_year(r["last_revision"]) if r["last_revision"] else None,
        ))
    conn.executemany(f"INSERT OR REPLACE INTO platform_structures VALUES ({','.join('?' * 23)})", data)
    print(f"  -> {len(data)} platform structures loaded")


def load_platform_locations(conn):
    print("Loading platform locations (fixed-width)...")
    spec = [
        (1, 3, "district_code"),
        (4, 8, "complex_id"),
        (12, 3, "structure_number"),
        (15, 2, "area_code"),
        (17, 6, "block_number"),
        (23, 15, "structure_name"),
        (38, 6, "ns_distance"),
        (44, 1, "ns_code"),
        (45, 6, "ew_distance"),
        (51, 1, "ew_code"),
        (52, 17, "x_location"),
        (69, 17, "y_location"),
        (86, 14, "longitude"),
        (100, 13, "latitude"),
    ]
    rows = parse_fixed_width(EXTRACTED_DIR / "platloc.DAT", spec)
    data = []
    for r in rows:
        data.append((
            s(r["complex_id"]),
            s(r["structure_number"]),
            s(r["district_code"]),
            s(r["area_code"]),
            s(r["block_number"]),
            s(r["structure_name"]),
            to_float(r["longitude"]),
            to_float(r["latitude"]),
            to_float(r["x_location"]),
            to_float(r["y_location"]),
            s(r["ns_distance"]),
            r["ns_code"],
            s(r["ew_distance"]),
            r["ew_code"],
        ))
    conn.executemany(f"INSERT OR REPLACE INTO platform_locations VALUES ({','.join('?' * 14)})", data)
    print(f"  -> {len(data)} platform locations loaded")


def load_platform_approvals(conn):
    print("Loading platform approvals...")
    rows = parse_delimited_file(EXTRACTED_DIR / "platformapprovalsdelimit.txt")
    data = []
    for r in rows:
        if len(r) < 10:
            continue
        data.append((
            parse_date_mmddyyyy(r[0]),   # application_date
            s(r[3]),                      # application_number
            s(r[2]),                      # company_name
            s(r[1]),                      # company_num (district? checking position)
            s(r[4]),                      # temp_structure
            parse_date_mmddyyyy(r[5]) if len(r) > 5 else None,
            s(r[6]),                      # lease_number
            s(r[7]),                      # area_code
            s(r[8]),                      # block_number
            s(r[9]),                      # structure_name
            s(r[10]) if len(r) > 10 else None,
            s(r[11]) if len(r) > 11 else None,
            s(r[12]) if len(r) > 12 else None,
            s(r[13]) if len(r) > 13 else None,
            to_int(r[14]) if len(r) > 14 else None,
            parse_date_mmddyyyy(r[15]) if len(r) > 15 else None,
            s(r[16]) if len(r) > 16 else None,
        ))
    conn.executemany(f"INSERT INTO platform_approvals VALUES ({','.join('?' * 17)})", data)
    print(f"  -> {len(data)} platform approval records loaded")


def load_platform_removals(conn):
    print("Loading platform removals...")
    rows = parse_delimited_file(EXTRACTED_DIR / "platstruremdelimit.txt")
    data = []
    for r in rows:
        if len(r) < 13:
            continue
        data.append((
            s(r[0]),                      # company_name
            s(r[1]),                      # company_num
            s(r[2]),                      # application_number
            parse_date_mmddyyyy(r[3]),    # received_date
            parse_date_mmddyyyy(r[4]),    # final_action_date
            parse_date_mmddyyyy(r[5]),    # removal_date
            parse_date_mmddyyyy(r[6]),    # site_clearance_date
            s(r[7]),                      # submittal_type
            s(r[8]),                      # lease_number
            s(r[9]),                      # area_code
            s(r[10]),                     # block_number
            s(r[11]),                     # structure_name
            parse_date_mon_year(r[12]) if len(r) > 12 else None,
            s(r[13]) if len(r) > 13 else None,   # removal_method
            s(r[14]) if len(r) > 14 else None,   # district_code
            s(r[15]) if len(r) > 15 else None,   # complex_id
            s(r[16]) if len(r) > 16 else None,   # structure_number
            to_int(r[17]) if len(r) > 17 else None,  # water_depth
        ))
    conn.executemany(f"INSERT INTO platform_removals VALUES ({','.join('?' * 18)})", data)
    print(f"  -> {len(data)} platform removal records loaded")


def load_wells(conn):
    print("Loading wells/boreholes...")
    rows = parse_delimited_file(EXTRACTED_DIR / "5010.txt")
    data = []
    for r in rows:
        if len(r) < 20:
            continue
        data.append((
            s(r[0]),             # api_well_number
            s(r[1]),             # well_name
            s(r[2]),             # well_name_suffix
            s(r[3]),             # operator_num
            s(r[4]),             # bottom_field_code
            parse_date_yyyymmdd(r[5]),  # spud_date
            s(r[6]),             # bottom_lease_number
            to_int(r[7]),        # rkb_elevation
            to_int(r[8]),        # total_measured_depth
            to_int(r[9]),        # true_vertical_depth
            to_int(r[19]) if len(r) > 19 else None,  # water_depth
            to_float(r[20]) if len(r) > 20 else None,  # surface_longitude
            to_float(r[21]) if len(r) > 21 else None,  # surface_latitude
            to_float(r[22]) if len(r) > 22 else None,  # bottom_longitude
            to_float(r[23]) if len(r) > 23 else None,  # bottom_latitude
            s(r[16]) if len(r) > 16 else None,  # status_code
            s(r[17]) if len(r) > 17 else None,  # type_code
            s(r[18]) if len(r) > 18 else None,  # well_class
            s(r[15]) if len(r) > 15 else None,  # district_code
            s(r[11]) if len(r) > 11 else None,  # area_block
            parse_date_yyyymmdd(r[14]) if len(r) > 14 else None,  # completion_date
            parse_date_yyyymmdd(r[13]) if len(r) > 13 else None,  # plugback_date
        ))
    conn.executemany(f"INSERT OR REPLACE INTO wells VALUES ({','.join('?' * 22)})", data)
    print(f"  -> {len(data)} wells loaded")


def load_apd(conn):
    """Load APD (Application for Permit to Drill) data."""
    print("Loading APD permits...")
    filepath = EXTRACTED_DIR / "mv_apd_main.txt"
    if not filepath.exists():
        print("  -> mv_apd_main.txt not found, skipping APD")
        return
    rows = parse_delimited_file(filepath)
    # First row is the header — skip it
    # Header: SN_APD(0), API_WELL_NUMBER(1), MMS_COMPANY_NUM(2), KICKOFF_POINT_MD(3),
    #   WELL_NAME(4), ..., WATER_DEPTH(7), PERMIT_TYPE(8), ..., MINERAL_CODE(10),
    #   REQ_SPUD_DATE(11), ..., WELL_TYPE_CODE(13), ...,
    #   SURF_AREA_CODE(16), SURF_BLOCK_NUMBER(17), SURF_LEASE_NUMBER(18), ...,
    #   BOTM_AREA_CODE(28), BOTM_BLOCK_NUMBER(29), BOTM_LEASE_NUMBER(30), ...,
    #   RIG_NAME(45), RIG_TYPE_CODE(46), RIG_ID_NUM(47), ...,
    #   BUS_ASC_NAME(59), APD_SUB_STATUS_DT(60), APD_STATUS_DT(61)
    data = []
    for i, r in enumerate(rows):
        if i == 0:  # skip header
            continue
        if len(r) < 62:
            continue
        data.append((
            s(r[0]),              # sn_apd
            s(r[1]),              # api_well_number
            s(r[2]),              # operator_num (MMS_COMPANY_NUM)
            s(r[4]),              # well_name
            s(r[8]),              # permit_type
            s(r[13]),             # well_type_code
            to_int(r[7]),         # water_depth
            parse_date_mdy(r[11]),  # req_spud_date
            parse_date_mdy(r[61]),  # apd_status_dt
            parse_date_mdy(r[60]),  # apd_sub_status_dt
            s(r[16]),             # surf_area_code
            s(r[17]),             # surf_block_number
            s(r[18]),             # surf_lease_number
            s(r[28]),             # botm_area_code
            s(r[29]),             # botm_block_number
            s(r[30]),             # botm_lease_number
            s(r[45]),             # rig_name
            s(r[46]),             # rig_type_code
            s(r[47]),             # rig_id_num
            s(r[59]),             # bus_asc_name
        ))
    conn.executemany(f"INSERT OR REPLACE INTO apd VALUES ({','.join('?' * 20)})", data)
    print(f"  -> {len(data)} APD permits loaded")


def load_apm(conn):
    """Load APM (Application for Permit to Modify) data."""
    print("Loading APM permits...")
    filepath = EXTRACTED_DIR / "mv_apm_main.txt"
    if not filepath.exists():
        print("  -> mv_apm_main.txt not found, skipping APM")
        return
    rows = parse_delimited_file(filepath)
    # Header: SN_APM(0), MMS_COMPANY_NUM(1), API_WELL_NUMBER(2), WATER_DEPTH(3),
    #   WELL_NM(4), WELL_NM_BP_SFIX(5), WELL_NM_ST_SFIX(6),
    #   SURF_AREA_CODE(7), SURF_BLOCK_NUM(8), SURF_LEASE_NUM(9),
    #   BOTM_AREA_CODE(10), BOTM_BLOCK_NUM(11), BOTM_LEASE_NUM(12),
    #   RIG_ID_NUM(13), BOREHOLE_STAT_CD(14), WELL_TYPE_CODE(15),
    #   ACC_STATUS_DATE(16), APM_OP_CD(17), SUB_STAT_DATE(18),
    #   BUS_ASC_NAME(19), SV_TYPE(20), SV_FEET_BML(21),
    #   SHUTIN_TUBING_PRSS(22), EST_OPERATION_DAYS(23), WORK_COMMENCES_DATE(24)
    data = []
    for i, r in enumerate(rows):
        if i == 0:  # skip header
            continue
        if len(r) < 25:
            continue
        data.append((
            s(r[0]),              # sn_apm
            s(r[2]),              # api_well_number
            s(r[1]),              # operator_num (MMS_COMPANY_NUM)
            s(r[4]),              # well_name
            s(r[15]),             # well_type_code
            to_int(r[3]),         # water_depth
            s(r[14]),             # borehole_stat_cd
            s(r[17]),             # apm_op_cd
            parse_date_mdy(r[16]),  # acc_status_date
            parse_date_mdy(r[18]),  # sub_stat_date
            s(r[7]),              # surf_area_code
            s(r[8]),              # surf_block_num
            s(r[9]),              # surf_lease_num
            s(r[10]),             # botm_area_code
            s(r[11]),             # botm_block_num
            s(r[12]),             # botm_lease_num
            s(r[13]),             # rig_id_num
            s(r[19]),             # bus_asc_name
            s(r[20]),             # sv_type
            to_int(r[23]),        # est_operation_days
            parse_date_mdy(r[24]),  # work_commences_date
        ))
    conn.executemany(f"INSERT OR REPLACE INTO apm VALUES ({','.join('?' * 21)})", data)
    print(f"  -> {len(data)} APM permits loaded")


def load_pipelines(conn):
    print("Loading pipeline masters...")
    rows = parse_delimited_file(EXTRACTED_DIR / "pplmastdelimit.txt")
    data = []
    for r in rows:
        if len(r) < 20:
            continue
        data.append((
            s(r[0]),              # segment_num
            to_float(r[1]),       # segment_length
            s(r[2]),              # origin_name
            s(r[3]),              # origin_area
            s(r[4]),              # origin_block
            s(r[5]),              # origin_lease
            s(r[6]) if len(r) > 6 else None,   # dest_name (auth_code in some layouts)
            s(r[7]) if len(r) > 7 else None,   # dest_area
            s(r[8]) if len(r) > 8 else None,   # dest_block
            s(r[9]) if len(r) > 9 else None,   # dest_lease
            parse_date_yyyymmdd(r[10]) if len(r) > 10 else None,  # abandon_approval
            parse_date_yyyymmdd(r[11]) if len(r) > 11 else None,  # abandon_date
            parse_date_yyyymmdd(r[12]) if len(r) > 12 else None,  # approved_date
            s(r[13]) if len(r) > 13 else None,  # auth_code
            s(r[14]) if len(r) > 14 else None,  # boarding_sdv
            s(r[15]) if len(r) > 15 else None,  # buried_flag
            to_int(r[16]) if len(r) > 16 else None,  # cathodic_life
            s(r[17]) if len(r) > 17 else None,
            parse_date_yyyymmdd(r[18]) if len(r) > 18 else None,
            s(r[19]) if len(r) > 19 else None,
            parse_date_yyyymmdd(r[20]) if len(r) > 20 else None,  # last_revision
            parse_date_yyyymmdd(r[21]) if len(r) > 21 else None,  # hydrotest
            to_float(r[22]) if len(r) > 22 else None,  # fed_state_length
            s(r[23]) if len(r) > 23 else None,  # status_code
            s(r[24]) if len(r) > 24 else None,  # pipe_size
            s(r[25]) if len(r) > 25 else None,  # row_number
            to_float(r[26]) if len(r) > 26 else None,
            s(r[27]) if len(r) > 27 else None,
            parse_date_yyyymmdd(r[28]) if len(r) > 28 else None,
            s(r[29]) if len(r) > 29 else None,  # product_code
            s(r[30]) if len(r) > 30 else None,
            s(r[31]) if len(r) > 31 else None,
            s(r[32]) if len(r) > 32 else None,
            to_int(r[33]) if len(r) > 33 else None,
            to_int(r[34]) if len(r) > 34 else None,
            s(r[35]) if len(r) > 35 else None,
            to_float(r[36]) if len(r) > 36 else None,
            s(r[37]) if len(r) > 37 else None,
            s(r[38]) if len(r) > 38 else None,
            s(r[39]) if len(r) > 39 else None,
            s(r[40]) if len(r) > 40 else None,
            s(r[41]) if len(r) > 41 else None,
        ))
    conn.executemany(f"INSERT OR REPLACE INTO pipelines VALUES ({','.join('?' * 42)})", data)
    print(f"  -> {len(data)} pipelines loaded")


def load_pipeline_locations(conn):
    print("Loading pipeline locations...")
    rows = parse_delimited_file(EXTRACTED_DIR / "localldelimit.txt")
    data = []
    for r in rows:
        if len(r) < 10:
            continue
        data.append((
            s(r[0]),              # segment_num
            to_int(r[1]),         # point_seq
            to_float(r[2]),       # latitude
            to_float(r[3]),       # longitude
            s(r[4]),              # nad_year
            s(r[5]),              # proj_code
            to_float(r[6]),       # x_coord
            to_float(r[7]),       # y_coord
            parse_date_yyyymmdd(r[8]),   # last_revision
            parse_date_yyyymmdd(r[9]),   # version_date
            s(r[10]) if len(r) > 10 else None,  # asbuilt_flag
        ))
    conn.executemany(f"INSERT OR REPLACE INTO pipeline_locations VALUES ({','.join('?' * 11)})", data)
    print(f"  -> {len(data)} pipeline location points loaded")


def load_production(conn):
    """Load all OGOR-A production files (1996-2025)."""
    print("Loading OGOR-A production data...")
    total = 0

    # Find all OGOR-A zip files and extract them
    ogora_zips = sorted(RAW_DIR.glob("ogora_*_delimit.zip"))
    for zf in ogora_zips:
        year_match = re.search(r"ogora_(\d{4})_delimit\.zip", zf.name)
        if not year_match:
            continue
        year = year_match.group(1)

        # Extract to get the txt file
        with zipfile.ZipFile(zf, "r") as z:
            z.extractall(EXTRACTED_DIR)

        # Find the extracted file
        if year == "2025":
            txt_name = "ogoradelimit.txt"
        else:
            txt_name = f"ogora{year}delimit.txt"

        txt_path = EXTRACTED_DIR / txt_name
        if not txt_path.exists():
            # Try alternate name
            for candidate in EXTRACTED_DIR.glob(f"ogora*{year}*delimit*"):
                txt_path = candidate
                break

        if not txt_path.exists():
            print(f"  WARNING: Could not find extracted file for {year}")
            continue

        print(f"  Processing {year}...", end=" ")
        rows = parse_delimited_file(txt_path)
        data = []
        for r in rows:
            if len(r) < 14:
                continue
            data.append((
                s(r[0]),              # lease_number
                s(r[1]),              # completion_name
                s(r[2]),              # production_date (YYYYMM)
                to_int(r[3]),         # days_on_production
                s(r[4]),              # product_code
                to_float(r[5]),       # oil_volume
                to_float(r[6]),       # gas_volume
                to_float(r[7]),       # water_volume
                s(r[8]),              # api_well_number
                s(r[9]),              # well_status
                s(r[10]),             # area_block
                s(r[11]),             # operator_num
                s(r[12]),             # operator_name
                s(r[13]),             # field_name_code
                to_float(r[14]) if len(r) > 14 else None,  # injection_volume
                s(r[15]) if len(r) > 15 else None,
                parse_date_yyyymmdd(r[16]) if len(r) > 16 else None,
                s(r[17]) if len(r) > 17 else None,
                s(r[18]) if len(r) > 18 else None,
            ))
        conn.executemany(f"INSERT INTO production VALUES ({','.join('?' * 19)})", data)
        total += len(data)
        print(f"{len(data):,} records")

    print(f"  -> {total:,} total production records loaded")


def create_views(conn):
    """Create useful materialized views for common queries."""
    conn.executescript("""
    -- Active leases with their current operator
    CREATE VIEW IF NOT EXISTS v_active_leases AS
    SELECT
        l.lease_number,
        l.area_code,
        l.block_number,
        l.lease_status,
        l.effective_date,
        l.expiration_date,
        l.royalty_rate,
        l.bid_amount,
        l.min_water_depth,
        l.max_water_depth,
        l.first_production_date,
        l.num_platforms,
        ll.designated_operator,
        ll.mineral_type
    FROM leases l
    LEFT JOIN lease_list ll ON l.lease_number = ll.lease_number
    WHERE l.lease_status NOT IN ('RELINQ', 'EXPIR', 'TERMIN');

    -- Platforms with location and operator info
    CREATE VIEW IF NOT EXISTS v_platforms_full AS
    SELECT
        p.complex_id,
        p.area_code,
        p.block_number,
        p.field_name_code,
        p.water_depth,
        p.distance_to_shore,
        p.oil_producing,
        p.gas_producing,
        p.drilling,
        p.manned_24hr,
        c.company_name AS operator_name,
        p.company_num AS operator_num,
        p.lease_number,
        pl.longitude,
        pl.latitude,
        ps.structure_name,
        ps.structure_type,
        ps.install_date,
        ps.removal_date,
        ps.slot_count,
        ps.slot_drill_count
    FROM platforms p
    LEFT JOIN companies c ON p.company_num = c.company_num
    LEFT JOIN platform_locations pl ON p.complex_id = pl.complex_id
    LEFT JOIN platform_structures ps ON p.complex_id = ps.complex_id
        AND ps.structure_number = (
            SELECT MIN(ps2.structure_number)
            FROM platform_structures ps2
            WHERE ps2.complex_id = p.complex_id
        );

    -- Monthly production summary by lease
    CREATE VIEW IF NOT EXISTS v_monthly_production AS
    SELECT
        lease_number,
        production_date,
        field_name_code,
        operator_num,
        operator_name,
        SUM(oil_volume) AS total_oil_bbl,
        SUM(gas_volume) AS total_gas_mcf,
        SUM(water_volume) AS total_water_bbl,
        SUM(injection_volume) AS total_injection,
        COUNT(DISTINCT api_well_number) AS well_count
    FROM production
    GROUP BY lease_number, production_date;

    -- Annual production summary by field
    CREATE VIEW IF NOT EXISTS v_annual_field_production AS
    SELECT
        field_name_code,
        SUBSTR(production_date, 1, 4) AS year,
        SUM(oil_volume) AS total_oil_bbl,
        SUM(gas_volume) AS total_gas_mcf,
        SUM(water_volume) AS total_water_bbl,
        COUNT(DISTINCT lease_number) AS lease_count,
        COUNT(DISTINCT api_well_number) AS well_count
    FROM production
    GROUP BY field_name_code, SUBSTR(production_date, 1, 4);

    -- Wells with operator name
    CREATE VIEW IF NOT EXISTS v_wells_full AS
    SELECT
        w.*,
        c.company_name AS operator_name,
        c.sort_name AS operator_sort_name
    FROM wells w
    LEFT JOIN companies c ON w.operator_num = c.company_num;

    -- Pipelines with operator name
    CREATE VIEW IF NOT EXISTS v_pipelines_full AS
    SELECT
        p.*,
        c.company_name AS operator_name
    FROM pipelines p
    LEFT JOIN companies c ON p.facility_operator = c.company_num;
    """)
    print("Views created.")


def print_summary(conn):
    """Print a summary of all table row counts."""
    print("\n" + "=" * 60)
    print("DATABASE SUMMARY")
    print("=" * 60)
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    total = 0
    for (table,) in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
        total += count
        print(f"  {table:35s} {count:>12,} rows")
    print(f"  {'TOTAL':35s} {total:>12,} rows")

    views = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
    ).fetchall()
    print(f"\n  Views: {', '.join(v[0] for v in views)}")

    db_size = os.path.getsize(DB_PATH)
    print(f"\n  Database size: {db_size / 1024 / 1024:.1f} MB")
    print(f"  Location: {DB_PATH}")


def main():
    print("=" * 60)
    print("BOEM Relational Database Builder")
    print("=" * 60)

    # Extract zip files
    extract_if_needed()

    # Remove old database if it exists
    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"Removed existing database: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB cache

    try:
        create_schema(conn)
        conn.commit()

        # Load reference tables first
        load_companies(conn)
        conn.commit()
        load_rigs(conn)
        conn.commit()

        # Load field/reserves data
        load_fields(conn)
        conn.commit()
        load_field_production(conn)
        conn.commit()
        load_appendices(conn)
        conn.commit()

        # Load leasing data
        load_leases(conn)
        conn.commit()
        load_lease_list(conn)
        conn.commit()
        load_lease_owners(conn)
        conn.commit()

        # Load platform data
        load_platforms(conn)
        conn.commit()
        load_platform_structures(conn)
        conn.commit()
        load_platform_locations(conn)
        conn.commit()
        load_platform_approvals(conn)
        conn.commit()
        load_platform_removals(conn)
        conn.commit()

        # Load well data
        load_wells(conn)
        conn.commit()
        load_apd(conn)
        conn.commit()
        load_apm(conn)
        conn.commit()

        # Load pipeline data
        load_pipelines(conn)
        conn.commit()
        load_pipeline_locations(conn)
        conn.commit()

        # Load production data (largest dataset)
        load_production(conn)
        conn.commit()

        # Create views
        create_views(conn)
        conn.commit()

        # Print summary
        print_summary(conn)

    finally:
        conn.close()

    print("\nDone!")


if __name__ == "__main__":
    main()
