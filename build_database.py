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
  - Production by Platform (delimited)
  - End of Operations Reports / EOR (delimited)
  - Well Activity Reports / WAR (delimited)
  - Rig ID List (delimited)
  - APD / Permits to Drill (delimited)
  - Lease List (fixed-width)
  - Appendix A: Area/Block to Field (delimited)
  - Appendix B: Lease to Field (delimited)
  - Appendix C: Operator to Field (delimited)
"""

import argparse
import csv
import glob
import hashlib
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


def file_checksum(*filepaths):
    """Compute combined MD5 checksum of one or more files. Returns None if none exist."""
    h = hashlib.md5()
    found = False
    for filepath in filepaths:
        if not filepath.exists():
            continue
        found = True
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
    return h.hexdigest() if found else None


def init_build_meta(conn):
    """Create the build metadata table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _build_meta (
            source_key  TEXT PRIMARY KEY,
            checksum    TEXT,
            built_at    TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def source_changed(conn, key, checksum):
    """Check if a source file has changed since last build."""
    row = conn.execute(
        "SELECT checksum FROM _build_meta WHERE source_key = ?", (key,)
    ).fetchone()
    return row is None or row[0] != checksum


def mark_built(conn, key, checksum):
    """Record that a source was built with a given checksum."""
    conn.execute(
        "INSERT OR REPLACE INTO _build_meta (source_key, checksum, built_at) "
        "VALUES (?, ?, datetime('now'))",
        (key, checksum),
    )


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
        rig_id              TEXT PRIMARY KEY,
        rig_name            TEXT,
        rig_type            TEXT,
        rig_func_code       TEXT,
        construction_yr     INTEGER,
        shipyard_name       TEXT,
        refurbished_yr      INTEGER,
        rated_water_depth   INTEGER,
        rated_drill_depth   INTEGER,
        abs_cert_expr_dt    TEXT,
        cg_cert_expr_dt     TEXT,
        anchor_flag         TEXT,
        apd_count           INTEGER DEFAULT 0,
        apm_count           INTEGER DEFAULT 0,
        last_permit_date    TEXT
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

    CREATE TABLE IF NOT EXISTS apd_casing_intervals (
        sn_apd_csg_intv     TEXT PRIMARY KEY,
        sn_apd_fk           TEXT,
        csng_intv_num       INTEGER,
        csng_intv_type_cd   TEXT,
        csng_intv_name      TEXT,
        csng_holesize       TEXT,
        csng_mud_wgt_ppg    REAL,
        csng_mud_type_cd    TEXT,
        csng_frac_grad_ppg  REAL,
        csng_top_md         REAL,
        csng_cement_vol     REAL,
        csng_preventer_cd   TEXT,
        csng_bop_stack_size TEXT,
        csng_wellhead_rating TEXT,
        csng_annular_rating TEXT,
        csng_bop_rating     TEXT,
        csng_annular_test_prss REAL,
        csng_bop_div_test_prss REAL,
        csng_mud_test_prss  REAL,
        csng_liner_test     REAL,
        csng_formation_test_prss REAL
    );

    CREATE TABLE IF NOT EXISTS apd_casing_sections (
        sn_apd_csng_intv_fk TEXT,
        casing_section_num  INTEGER,
        casing_size         TEXT,
        casing_weight       REAL,
        casing_grade        TEXT,
        casing_burst_psi    REAL,
        casing_collapse_psi REAL,
        casing_section_md   REAL,
        casing_section_tvd  REAL,
        casing_pore_prss_ppg REAL
    );

    CREATE TABLE IF NOT EXISTS apd_geologic (
        sn_apd              TEXT,
        h2s_designation     TEXT,
        h2s_actvtn_plan_tvd TEXT,
        geo_marker_name     TEXT,
        top_md              REAL
    );

    CREATE TABLE IF NOT EXISTS apm_preventers (
        sn_apm_fk           TEXT,
        sn_apm_preventer    TEXT PRIMARY KEY,
        apm_preventer_cd    TEXT,
        bop_stack_size      TEXT,
        bop_working_prss    REAL,
        bop_high_test_prss  REAL,
        bop_low_test_prss   REAL
    );

    CREATE TABLE IF NOT EXISTS apm_suboperations (
        sn_apm_fk           TEXT,
        sn_apm_suboperation TEXT PRIMARY KEY,
        apm_subop_cd        TEXT
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

    -- ========================================================
    -- END OF OPERATIONS REPORTS (EOR)
    -- ========================================================

    CREATE TABLE IF NOT EXISTS eor (
        sn_eor              INTEGER PRIMARY KEY,
        operation_cd        TEXT,
        api_well_number     TEXT,
        well_name           TEXT,
        well_nm_st_sfix     TEXT,
        well_nm_bp_sfix     TEXT,
        company_num         TEXT,
        bus_asc_name        TEXT,
        botm_lease_number   TEXT,
        botm_area_code      TEXT,
        botm_block_number   TEXT,
        surf_lease_number   TEXT,
        surf_area_code      TEXT,
        surf_block_number   TEXT,
        borehole_stat_cd    TEXT,
        borehole_stat_dt    TEXT,
        operational_narrative TEXT,
        subsea_completion   TEXT,
        subsea_protection   TEXT,
        subsea_buoy         TEXT,
        subsea_tree_height  REAL,
        obstruction_protection TEXT,
        obstruction_type_cd TEXT,
        obstruction_buoy    TEXT,
        obstruction_height  REAL,
        botm_longitude      REAL,
        botm_latitude       REAL,
        total_md            REAL,
        well_bore_tvd       REAL,
        kickoff_md          REAL
    );

    CREATE TABLE IF NOT EXISTS eor_completions (
        sn_eor_fk           INTEGER,
        sn_eor_well_comp    INTEGER,
        interval            TEXT,
        comp_lease_number   TEXT,
        comp_area_code      TEXT,
        comp_block_number   TEXT,
        comp_status_cd      TEXT,
        comp_latitude       REAL,
        comp_longitude      REAL,
        comp_rsvr_name      TEXT,
        comp_interval_name  TEXT
    );

    CREATE TABLE IF NOT EXISTS eor_cut_casings (
        sn_eor_fk           INTEGER,
        casing_size         TEXT,
        casing_cut_date     TEXT,
        casing_cut_method   TEXT,
        casing_cut_depth    REAL,
        casing_cut_mdl_ind  TEXT
    );

    CREATE TABLE IF NOT EXISTS eor_geomarkers (
        sn_eor_fk           INTEGER,
        geo_marker_name     TEXT,
        top_md              REAL
    );

    CREATE TABLE IF NOT EXISTS eor_hc_intervals (
        sn_hc_bearing_intvl INTEGER,
        sn_eor_fk           INTEGER,
        interval_name       TEXT,
        top_md              REAL,
        bottom_md           REAL,
        hydrocarbon_type_cd TEXT
    );

    CREATE TABLE IF NOT EXISTS eor_perf_intervals (
        sn_eor_well_comp_fk INTEGER,
        perf_top_md         REAL,
        perf_botm_tvd       REAL,
        perf_top_tvd        REAL,
        perf_base_md        REAL
    );

    -- ========================================================
    -- WELL ACTIVITY REPORTS (WAR)
    -- ========================================================

    CREATE TABLE IF NOT EXISTS war (
        sn_war              INTEGER PRIMARY KEY,
        war_start_dt        TEXT,
        war_end_dt          TEXT,
        contact_name        TEXT,
        phone_number        TEXT,
        rig_name            TEXT,
        water_depth         REAL,
        rkb_elevation       REAL,
        bop_test_date       TEXT,
        ram_tst_prss        REAL,
        annular_tst_prss    REAL,
        bus_asc_name        TEXT,
        company_num         TEXT,
        api_well_number     TEXT,
        well_name           TEXT,
        well_nm_bp_sfix     TEXT,
        well_nm_st_sfix     TEXT,
        surf_lease_num      TEXT,
        surf_area_code      TEXT,
        surf_block_num      TEXT,
        botm_lease_num      TEXT,
        botm_area_code      TEXT,
        botm_block_num      TEXT,
        well_activity_cd    TEXT,
        well_actv_start_dt  TEXT,
        well_actv_end_dt    TEXT,
        total_depth_date    TEXT,
        drilling_md         REAL,
        drilling_tvd        REAL,
        drill_fluid_wgt     REAL
    );

    CREATE TABLE IF NOT EXISTS war_boreholes (
        api_well_number     TEXT,
        botm_lease_num      TEXT,
        well_spud_date      TEXT,
        total_depth_date    TEXT,
        borehole_stat_dt    TEXT,
        bh_total_md         REAL,
        well_bore_tvd       REAL
    );

    CREATE TABLE IF NOT EXISTS war_tubulars (
        sn_war_fk           INTEGER,
        csng_intv_type_cd   TEXT,
        csng_hole_size      TEXT,
        casing_size         TEXT,
        casing_weight       REAL,
        casing_grade        TEXT,
        csng_liner_test_prss REAL,
        csng_shoe_test_prss REAL,
        csng_cement_vol     REAL,
        csng_setting_top_md REAL,
        csng_setting_botm_md REAL
    );

    CREATE TABLE IF NOT EXISTS production_by_platform (
        complex_id_num      INTEGER,
        structure_number    TEXT,
        area_code           TEXT,
        block_number        TEXT,
        lease_number        TEXT,
        structure_name      TEXT,
        install_date        TEXT,
        removal_date        TEXT,
        operator            TEXT,
        operator_num        TEXT,
        production_date     TEXT,
        producing_wells     INTEGER,
        bopd                REAL,
        mcfpd               REAL,
        boepd               REAL,
        bwpd                REAL,
        region_code         TEXT
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
    CREATE INDEX IF NOT EXISTS idx_apd_casing_intervals_fk ON apd_casing_intervals(sn_apd_fk);
    CREATE INDEX IF NOT EXISTS idx_apd_casing_sections_fk ON apd_casing_sections(sn_apd_csng_intv_fk);
    CREATE INDEX IF NOT EXISTS idx_apd_geologic_fk ON apd_geologic(sn_apd);

    CREATE INDEX IF NOT EXISTS idx_apm_api ON apm(api_well_number);
    CREATE INDEX IF NOT EXISTS idx_apm_preventers_fk ON apm_preventers(sn_apm_fk);
    CREATE INDEX IF NOT EXISTS idx_apm_suboperations_fk ON apm_suboperations(sn_apm_fk);

    CREATE INDEX IF NOT EXISTS idx_war_api ON war(api_well_number);
    CREATE INDEX IF NOT EXISTS idx_war_rig ON war(rig_name);
    CREATE INDEX IF NOT EXISTS idx_war_start ON war(war_start_dt);
    CREATE INDEX IF NOT EXISTS idx_war_area ON war(botm_area_code, botm_block_num);
    CREATE INDEX IF NOT EXISTS idx_war_company ON war(company_num);
    CREATE INDEX IF NOT EXISTS idx_war_boreholes_api ON war_boreholes(api_well_number);
    CREATE INDEX IF NOT EXISTS idx_war_tubulars_fk ON war_tubulars(sn_war_fk);

    CREATE INDEX IF NOT EXISTS idx_eor_api ON eor(api_well_number);
    CREATE INDEX IF NOT EXISTS idx_eor_lease ON eor(botm_lease_number);
    CREATE INDEX IF NOT EXISTS idx_eor_area ON eor(botm_area_code, botm_block_number);
    CREATE INDEX IF NOT EXISTS idx_eor_company ON eor(company_num);
    CREATE INDEX IF NOT EXISTS idx_eor_completions_fk ON eor_completions(sn_eor_fk);
    CREATE INDEX IF NOT EXISTS idx_eor_cut_casings_fk ON eor_cut_casings(sn_eor_fk);
    CREATE INDEX IF NOT EXISTS idx_eor_geomarkers_fk ON eor_geomarkers(sn_eor_fk);
    CREATE INDEX IF NOT EXISTS idx_eor_hc_intervals_fk ON eor_hc_intervals(sn_eor_fk);
    CREATE INDEX IF NOT EXISTS idx_eor_perf_intervals_fk ON eor_perf_intervals(sn_eor_well_comp_fk);

    CREATE INDEX IF NOT EXISTS idx_prod_plat_complex ON production_by_platform(complex_id_num);
    CREATE INDEX IF NOT EXISTS idx_prod_plat_area ON production_by_platform(area_code, block_number);
    CREATE INDEX IF NOT EXISTS idx_prod_plat_date ON production_by_platform(production_date);
    CREATE INDEX IF NOT EXISTS idx_prod_plat_lease ON production_by_platform(lease_number);

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
    """Load rigs from basic list, then enrich with APD and APM rig data."""
    print("Loading rigs...")

    # Step 1: Load basic rig list (rig_id, rig_name, rig_type)
    basic_path = EXTRACTED_DIR / "rigidlistdelimit.txt"
    rigs = {}  # keyed by rig_id
    if basic_path.exists():
        for r in parse_delimited_file(basic_path):
            if len(r) >= 2:
                rid = s(r[0])
                if rid:
                    rigs[rid] = {
                        'rig_id': rid,
                        'rig_name': s(r[1]),
                        'rig_type': s(r[2]) if len(r) > 2 else None,
                    }
        print(f"  -> {len(rigs)} rigs from basic list")

    # Step 2: Enrich from APD data (most recent per rig_id wins)
    apd_path = EXTRACTED_DIR / "mv_apd_main.txt"
    apd_counts = {}
    apd_last_date = {}
    if apd_path.exists():
        rows = parse_delimited_file(apd_path)
        for i, r in enumerate(rows):
            if i == 0 or len(r) < 62:
                continue
            rid = s(r[47])  # RIG_ID_NUM
            if not rid:
                continue
            apd_counts[rid] = apd_counts.get(rid, 0) + 1
            status_dt = parse_date_mdy(r[61])  # APD_STATUS_DT
            if status_dt:
                if rid not in apd_last_date or status_dt > apd_last_date[rid]:
                    apd_last_date[rid] = status_dt

            # Create or update rig entry with enriched data
            if rid not in rigs:
                rigs[rid] = {'rig_id': rid}
            rig = rigs[rid]
            # Only overwrite with non-None values from newer permits
            name = s(r[45])  # RIG_NAME
            if name:
                rig['rig_name'] = name
            rtype = s(r[46])  # RIG_TYPE_CODE
            if rtype:
                rig['rig_type'] = rtype
            func = s(r[48])  # RIG_FUNC_CODE
            if func:
                rig['rig_func_code'] = func
            cy = to_int(r[49])  # CONSTRUCTION_YR
            if cy:
                rig['construction_yr'] = cy
            sy = s(r[50])  # SHIPYARD_NAME
            if sy:
                rig['shipyard_name'] = sy
            ry = to_int(r[51])  # REFURBISHED_YR
            if ry:
                rig['refurbished_yr'] = ry
            rwd = to_int(r[52])  # RATED_WTR_DEPTH
            if rwd:
                rig['rated_water_depth'] = rwd
            rdd = to_int(r[53])  # RATE_DRIL_DEPTH
            if rdd:
                rig['rated_drill_depth'] = rdd
            abs_dt = parse_date_mdy(r[54])  # ABS_CERT_EXPR_DT
            if abs_dt:
                rig['abs_cert_expr_dt'] = abs_dt
            cg_dt = parse_date_mdy(r[55])  # CG_CERT_EXPR_DT
            if cg_dt:
                rig['cg_cert_expr_dt'] = cg_dt
            af = s(r[44])  # RIG_ANCHOR_FLAG
            if af:
                rig['anchor_flag'] = af
        print(f"  -> enriched from {len(apd_counts)} unique rigs in APD data")

    # Step 3: Enrich from APM rig view (certification dates)
    apm_rig_path = EXTRACTED_DIR / "mv_apm_rig_view.txt"
    if apm_rig_path.exists():
        rows = parse_delimited_file(apm_rig_path)
        apm_rig_count = 0
        for i, r in enumerate(rows):
            if i == 0 or len(r) < 5:
                continue
            rid = s(r[0])  # RIG_ID_NUM
            if not rid:
                continue
            apm_rig_count += 1
            if rid not in rigs:
                rigs[rid] = {'rig_id': rid}
            rig = rigs[rid]
            name = s(r[1])
            if name:
                rig['rig_name'] = name
            rtype = s(r[4])
            if rtype:
                rig['rig_type'] = rtype
            abs_dt = parse_date_mdy(r[2])
            if abs_dt:
                rig.setdefault('abs_cert_expr_dt', abs_dt)
            cg_dt = parse_date_mdy(r[3])
            if cg_dt:
                rig.setdefault('cg_cert_expr_dt', cg_dt)
        print(f"  -> merged {apm_rig_count} rigs from APM rig view")

    # Step 4: Count APM permits per rig
    apm_counts = {}
    apm_last = {}
    apm_path = EXTRACTED_DIR / "mv_apm_main.txt"
    if apm_path.exists():
        rows = parse_delimited_file(apm_path)
        for i, r in enumerate(rows):
            if i == 0 or len(r) < 25:
                continue
            rid = s(r[13])  # RIG_ID_NUM
            if not rid:
                continue
            apm_counts[rid] = apm_counts.get(rid, 0) + 1
            dt = parse_date_mdy(r[16])  # ACC_STATUS_DATE
            if dt and (rid not in apm_last or dt > apm_last[rid]):
                apm_last[rid] = dt

    # Step 5: Build final insert data
    data = []
    for rid, rig in rigs.items():
        apd_c = apd_counts.get(rid, 0)
        apm_c = apm_counts.get(rid, 0)
        last_apd = apd_last_date.get(rid)
        last_apm = apm_last.get(rid)
        last_permit = max(filter(None, [last_apd, last_apm]), default=None)
        data.append((
            rig.get('rig_id'),
            rig.get('rig_name'),
            rig.get('rig_type'),
            rig.get('rig_func_code'),
            rig.get('construction_yr'),
            rig.get('shipyard_name'),
            rig.get('refurbished_yr'),
            rig.get('rated_water_depth'),
            rig.get('rated_drill_depth'),
            rig.get('abs_cert_expr_dt'),
            rig.get('cg_cert_expr_dt'),
            rig.get('anchor_flag'),
            apd_c,
            apm_c,
            last_permit,
        ))
    conn.executemany(f"INSERT OR REPLACE INTO rigs VALUES ({','.join('?' * 15)})", data)
    print(f"  -> {len(data)} total rigs loaded (enriched)")


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


def load_apd_casing(conn):
    """Load APD casing intervals and sections."""
    print("Loading APD casing data...")

    # Casing intervals
    filepath = EXTRACTED_DIR / "mv_apd_casing_intervals.txt"
    if not filepath.exists():
        print("  -> mv_apd_casing_intervals.txt not found, skipping")
        return
    rows = parse_delimited_file(filepath)
    # Header: SN_APD_CSG_INTV(0), SN_APD_FK(1), CSNG_INTV_NUM(2), CSNG_INTV_TYPE_CD(3),
    #   CSNG_INTV_NAME(4), CSNG_HOLESIZE(5), CSNG_MUD_WGT_PPG(6), CSNG_MUD_TYPE_CD(7),
    #   CSNG_FRAC_GRAD_PPG(8), CSNG_TOP_MD(9), CSNG_CEMENT_VOL(10), CSNG_PREVENTER_CD(11),
    #   CSNG_BOP_STACK_SIZE(12), CSNG_WELLHEAD_RATING(13), CSNG_ANNULAR_RATING(14),
    #   CSNG_BOP_RATING(15), CSNG_ANNULAR_TEST_PRSS(16), CSNG_BOP_DIV_TEST_PRSS(17),
    #   CSNG_MUD_TEST_PRSS(18), CSNG_LINER_TEST(19), CSNG_FORMATION_TEST_PRSS(20)
    data = []
    for i, r in enumerate(rows):
        if i == 0:
            continue
        if len(r) < 21:
            continue
        data.append((
            s(r[0]),              # sn_apd_csg_intv
            s(r[1]),              # sn_apd_fk
            to_int(r[2]),         # csng_intv_num
            s(r[3]),              # csng_intv_type_cd
            s(r[4]),              # csng_intv_name
            s(r[5]),              # csng_holesize
            to_float(r[6]),       # csng_mud_wgt_ppg
            s(r[7]),              # csng_mud_type_cd
            to_float(r[8]),       # csng_frac_grad_ppg
            to_float(r[9]),       # csng_top_md
            to_float(r[10]),      # csng_cement_vol
            s(r[11]),             # csng_preventer_cd
            s(r[12]),             # csng_bop_stack_size
            s(r[13]),             # csng_wellhead_rating
            s(r[14]),             # csng_annular_rating
            s(r[15]),             # csng_bop_rating
            to_float(r[16]),      # csng_annular_test_prss
            to_float(r[17]),      # csng_bop_div_test_prss
            to_float(r[18]),      # csng_mud_test_prss
            to_float(r[19]),      # csng_liner_test
            to_float(r[20]),      # csng_formation_test_prss
        ))
    conn.executemany(f"INSERT OR REPLACE INTO apd_casing_intervals VALUES ({','.join('?' * 21)})", data)
    print(f"  -> {len(data)} casing intervals loaded")

    # Casing sections
    filepath2 = EXTRACTED_DIR / "mv_apd_casing_sectons.txt"
    if not filepath2.exists():
        print("  -> mv_apd_casing_sectons.txt not found, skipping")
        return
    rows2 = parse_delimited_file(filepath2)
    # Header: SN_APD_CSNG_INTV_FK(0), CASING_SECTION_NUM(1), CASING_SIZE(2),
    #   CASING_WEIGHT(3), CASING_GRADE(4), CASING_BURST_PSI(5),
    #   CASING_COLLPSE_PSI(6), CASING_SECTION_MD(7), CASING_SECTION_TVD(8),
    #   CASING_PORE_PRSS_PPG(9)
    data2 = []
    for i, r in enumerate(rows2):
        if i == 0:
            continue
        if len(r) < 10:
            continue
        data2.append((
            s(r[0]),              # sn_apd_csng_intv_fk
            to_int(r[1]),         # casing_section_num
            s(r[2]),              # casing_size
            to_float(r[3]),       # casing_weight
            s(r[4]),              # casing_grade
            to_float(r[5]),       # casing_burst_psi
            to_float(r[6]),       # casing_collapse_psi
            to_float(r[7]),       # casing_section_md
            to_float(r[8]),       # casing_section_tvd
            to_float(r[9]),       # casing_pore_prss_ppg
        ))
    conn.executemany(f"INSERT OR REPLACE INTO apd_casing_sections VALUES ({','.join('?' * 10)})", data2)
    print(f"  -> {len(data2)} casing sections loaded")


def load_apd_geologic(conn):
    """Load APD geologic marker data."""
    print("Loading APD geologic data...")
    filepath = EXTRACTED_DIR / "mv_apd_geologic.txt"
    if not filepath.exists():
        print("  -> mv_apd_geologic.txt not found, skipping")
        return
    rows = parse_delimited_file(filepath)
    # Header: SN_APD(0), H2S_DESIGNATION(1), H2S_ACTVTN_PLAN_TVD(2),
    #   GEO_MARKER_NAME(3), TOP_MD(4)
    data = []
    for i, r in enumerate(rows):
        if i == 0:
            continue
        if len(r) < 5:
            continue
        data.append((
            s(r[0]),              # sn_apd
            s(r[1]),              # h2s_designation
            s(r[2]),              # h2s_actvtn_plan_tvd
            s(r[3]),              # geo_marker_name
            to_float(r[4]),       # top_md
        ))
    conn.executemany(f"INSERT OR REPLACE INTO apd_geologic VALUES ({','.join('?' * 5)})", data)
    print(f"  -> {len(data)} geologic markers loaded")


def load_apm_sub_data(conn):
    """Load APM preventers and suboperations."""
    print("Loading APM sub-data...")

    # Preventers
    filepath = EXTRACTED_DIR / "mv_apm_preventers.txt"
    if filepath.exists():
        rows = parse_delimited_file(filepath)
        # Header: SN_APM_FK(0), SN_APM_PREVENTER(1), APM_PREVENTER_CD(2),
        #   BOP_STACK_SIZE(3), BOP_WORKING_PRSS(4), BOP_HIGH_TEST_PRSS(5), BOP_LOW_TEST_PRSS(6)
        data = []
        for i, r in enumerate(rows):
            if i == 0:
                continue
            if len(r) < 7:
                continue
            data.append((
                s(r[0]),              # sn_apm_fk
                s(r[1]),              # sn_apm_preventer
                s(r[2]),              # apm_preventer_cd
                s(r[3]),              # bop_stack_size
                to_float(r[4]),       # bop_working_prss
                to_float(r[5]),       # bop_high_test_prss
                to_float(r[6]),       # bop_low_test_prss
            ))
        conn.executemany(f"INSERT OR REPLACE INTO apm_preventers VALUES ({','.join('?' * 7)})", data)
        print(f"  -> {len(data)} APM preventers loaded")
    else:
        print("  -> mv_apm_preventers.txt not found, skipping")

    # Suboperations
    filepath2 = EXTRACTED_DIR / "mv_apm_suboperations.txt"
    if filepath2.exists():
        rows2 = parse_delimited_file(filepath2)
        # Header: SN_APM_FK(0), SN_APM_SUBOPERATION(1), APM_SUBOP_CD(2)
        data2 = []
        for i, r in enumerate(rows2):
            if i == 0:
                continue
            if len(r) < 3:
                continue
            data2.append((
                s(r[0]),              # sn_apm_fk
                s(r[1]),              # sn_apm_suboperation
                s(r[2]),              # apm_subop_cd
            ))
        conn.executemany(f"INSERT OR REPLACE INTO apm_suboperations VALUES ({','.join('?' * 3)})", data2)
        print(f"  -> {len(data2)} APM suboperations loaded")
    else:
        print("  -> mv_apm_suboperations.txt not found, skipping")


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


def load_production_incremental(conn):
    """Load OGOR-A production data, skipping years whose zip files haven't changed."""
    print("Loading OGOR-A production data (incremental)...")
    total_loaded = 0
    total_skipped = 0

    ogora_zips = sorted(RAW_DIR.glob("ogora_*_delimit.zip"))
    for zf in ogora_zips:
        year_match = re.search(r"ogora_(\d{4})_delimit\.zip", zf.name)
        if not year_match:
            continue
        year = year_match.group(1)
        meta_key = f"production_{year}"

        checksum = file_checksum(zf)
        if not source_changed(conn, meta_key, checksum):
            total_skipped += 1
            continue

        # Extract this year's zip
        with zipfile.ZipFile(zf, "r") as z:
            z.extractall(EXTRACTED_DIR)

        # Find extracted file
        if year == "2025":
            txt_name = "ogoradelimit.txt"
        else:
            txt_name = f"ogora{year}delimit.txt"

        txt_path = EXTRACTED_DIR / txt_name
        if not txt_path.exists():
            for candidate in EXTRACTED_DIR.glob(f"ogora*{year}*delimit*"):
                txt_path = candidate
                break

        if not txt_path.exists():
            print(f"  WARNING: Could not find extracted file for {year}")
            continue

        print(f"  Processing {year}...", end=" ")

        # Delete old data for this year before inserting
        conn.execute(
            "DELETE FROM production WHERE production_date LIKE ?",
            (f"{year}%",),
        )

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
                to_float(r[14]) if len(r) > 14 else None,
                s(r[15]) if len(r) > 15 else None,
                parse_date_yyyymmdd(r[16]) if len(r) > 16 else None,
                s(r[17]) if len(r) > 17 else None,
                s(r[18]) if len(r) > 18 else None,
            ))
        conn.executemany(f"INSERT INTO production VALUES ({','.join('?' * 19)})", data)
        mark_built(conn, meta_key, checksum)
        conn.commit()
        total_loaded += len(data)
        print(f"{len(data):,} records")

    if total_skipped:
        print(f"  -> {total_skipped} years unchanged (skipped)")
    print(f"  -> {total_loaded:,} production records loaded")


def load_war(conn):
    """Load Well Activity Reports (merged main + props)."""
    print("Loading WAR data...")

    main_path = EXTRACTED_DIR / "mv_war_main.txt"
    prop_path = EXTRACTED_DIR / "mv_war_main_prop.txt"

    if not main_path.exists():
        print("  WARNING: mv_war_main.txt not found, skipping")
        return

    # Build props lookup: sn_war -> (activity_cd, actv_start, actv_end, td_date, drill_md, drill_tvd, fluid_wgt)
    props = {}
    if prop_path.exists():
        for r in parse_delimited_file(str(prop_path)):
            if len(r) < 9 or r[0] == "SN_WAR":
                continue
            props[s(r[0])] = (
                s(r[4]),              # well_activity_cd
                parse_date_mdy(r[2]), # well_actv_start_dt
                parse_date_mdy(r[5]), # well_actv_end_dt
                parse_date_mdy(r[3]), # total_depth_date
                to_float(r[6]),       # drilling_md
                to_float(r[7]),       # drilling_tvd
                to_float(r[8]),       # drill_fluid_wgt
            )

    rows = parse_delimited_file(str(main_path))
    data = []
    for r in rows:
        if len(r) < 23 or r[0] == "SN_WAR":
            continue
        sn = s(r[0])
        prop = props.get(sn, (None, None, None, None, None, None, None))
        data.append((
            to_int(r[0]),             # sn_war
            parse_date_mdy(r[1]),     # war_start_dt
            parse_date_mdy(r[2]),     # war_end_dt
            s(r[3]),                  # contact_name
            s(r[4]),                  # phone_number
            s(r[5]),                  # rig_name
            to_float(r[6]),           # water_depth
            to_float(r[7]),           # rkb_elevation
            parse_date_mdy(r[8]),     # bop_test_date
            to_float(r[9]),           # ram_tst_prss
            to_float(r[10]),          # annular_tst_prss
            s(r[11]),                 # bus_asc_name
            s(r[12]),                 # company_num
            s(r[13]),                 # api_well_number
            s(r[14]),                 # well_name
            s(r[15]),                 # well_nm_bp_sfix
            s(r[16]),                 # well_nm_st_sfix
            s(r[17]),                 # surf_lease_num
            s(r[18]),                 # surf_area_code
            s(r[19]),                 # surf_block_num
            s(r[20]),                 # botm_lease_num
            s(r[21]),                 # botm_area_code
            s(r[22]),                 # botm_block_num
            prop[0],                  # well_activity_cd
            prop[1],                  # well_actv_start_dt
            prop[2],                  # well_actv_end_dt
            prop[3],                  # total_depth_date
            prop[4],                  # drilling_md
            prop[5],                  # drilling_tvd
            prop[6],                  # drill_fluid_wgt
        ))
    conn.executemany(f"INSERT INTO war VALUES ({','.join('?' * 30)})", data)
    print(f"  {len(data):,} WAR records")


def load_war_sub_data(conn):
    """Load WAR sub-tables: boreholes and tubular summaries."""
    print("Loading WAR sub-data...")

    # Boreholes
    bh_path = EXTRACTED_DIR / "mv_war_boreholes_view.txt"
    if bh_path.exists():
        rows = parse_delimited_file(str(bh_path))
        data = []
        for r in rows:
            if len(r) < 7 or r[0] == "API_WELL_NUMBER":
                continue
            data.append((
                s(r[0]),              # api_well_number
                s(r[1]),              # botm_lease_num
                parse_date_mdy(r[2]), # well_spud_date
                parse_date_mdy(r[3]), # total_depth_date
                parse_date_mdy(r[4]), # borehole_stat_dt
                to_float(r[5]),       # bh_total_md
                to_float(r[6]),       # well_bore_tvd
            ))
        conn.executemany(f"INSERT INTO war_boreholes VALUES ({','.join('?' * 7)})", data)
        print(f"  war_boreholes: {len(data):,}")

    # Tubular summaries (merge with prop for setting depths)
    tub_path = EXTRACTED_DIR / "mv_war_tubular_summaries.txt"
    tub_prop_path = EXTRACTED_DIR / "mv_war_tubular_summaries_prop.txt"
    if tub_path.exists():
        # Build props lookup: sn_war_csng_intv -> (top_md, botm_md)
        tub_props = {}
        if tub_prop_path.exists():
            for r in parse_delimited_file(str(tub_prop_path)):
                if len(r) < 4 or r[0] == "SN_WAR_CSNG_INTV_FK":
                    continue
                tub_props[s(r[0])] = (to_float(r[3]), to_float(r[2]))  # top_md, botm_md

        rows = parse_delimited_file(str(tub_path))
        data = []
        for r in rows:
            if len(r) < 10 or r[0] == "SN_WAR_FK":
                continue
            sn_intv = s(r[9]) if len(r) > 9 else None
            prop = tub_props.get(sn_intv, (None, None))
            data.append((
                to_int(r[0]),         # sn_war_fk
                s(r[1]),              # csng_intv_type_cd
                s(r[2]),              # csng_hole_size
                s(r[3]),              # casing_size
                to_float(r[4]),       # casing_weight
                s(r[5]),              # casing_grade
                to_float(r[6]),       # csng_liner_test_prss
                to_float(r[7]),       # csng_shoe_test_prss
                to_float(r[8]),       # csng_cement_vol
                prop[0],              # csng_setting_top_md
                prop[1],              # csng_setting_botm_md
            ))
        conn.executemany(f"INSERT INTO war_tubulars VALUES ({','.join('?' * 11)})", data)
        print(f"  war_tubulars: {len(data):,}")


def load_eor(conn):
    """Load End of Operations Report main records (merged with location/depth props)."""
    print("Loading EOR data...")

    main_path = EXTRACTED_DIR / "mv_eor_mainquery.txt"
    prop_path = EXTRACTED_DIR / "mv_eor_mainquery_prop.txt"

    if not main_path.exists():
        print("  WARNING: mv_eor_mainquery.txt not found, skipping")
        return

    # Build props lookup: sn_eor -> (lon, lat, total_md, tvd, kickoff_md)
    props = {}
    if prop_path.exists():
        for r in parse_delimited_file(str(prop_path)):
            if len(r) < 6 or r[0] == "SN_EOR":
                continue
            props[s(r[0])] = (to_float(r[1]), to_float(r[2]), to_float(r[3]), to_float(r[4]), to_float(r[5]))

    rows = parse_delimited_file(str(main_path))
    data = []
    for r in rows:
        if len(r) < 25 or r[0] == "SN_EOR":
            continue
        sn = s(r[0])
        prop = props.get(sn, (None, None, None, None, None))
        data.append((
            to_int(r[0]),         # sn_eor
            s(r[1]),              # operation_cd
            s(r[2]),              # api_well_number
            s(r[3]),              # well_name
            s(r[4]),              # well_nm_st_sfix
            s(r[5]),              # well_nm_bp_sfix
            s(r[6]),              # company_num
            s(r[7]),              # bus_asc_name
            s(r[8]),              # botm_lease_number
            s(r[9]),              # botm_area_code
            s(r[10]),             # botm_block_number
            s(r[11]),             # surf_lease_number
            s(r[12]),             # surf_area_code
            s(r[13]),             # surf_block_number
            s(r[14]),             # borehole_stat_cd
            parse_date_mdy(r[15]),  # borehole_stat_dt
            s(r[16]),             # operational_narrative
            s(r[17]),             # subsea_completion
            s(r[18]),             # subsea_protection
            s(r[19]),             # subsea_buoy
            to_float(r[20]),      # subsea_tree_height
            s(r[21]),             # obstruction_protection
            s(r[22]),             # obstruction_type_cd
            s(r[23]),             # obstruction_buoy
            to_float(r[24]),      # obstruction_height
            prop[0],              # botm_longitude
            prop[1],              # botm_latitude
            prop[2],              # total_md
            prop[3],              # well_bore_tvd
            prop[4],              # kickoff_md
        ))
    conn.executemany(f"INSERT INTO eor VALUES ({','.join('?' * 30)})", data)
    print(f"  {len(data):,} EOR records")


def load_eor_sub_data(conn):
    """Load EOR sub-tables: completions, cut casings, geomarkers, HC intervals, perforations."""
    print("Loading EOR sub-data...")

    # Completions (merge completions + completionsprop)
    comp_path = EXTRACTED_DIR / "mv_eor_completions.txt"
    comp_prop_path = EXTRACTED_DIR / "mv_eor_completionsprop.txt"
    if comp_path.exists():
        # Build props lookup: (sn_eor_fk, sn_eor_well_comp) -> (lat, lon, rsvr, interval)
        comp_props = {}
        if comp_prop_path.exists():
            for r in parse_delimited_file(str(comp_prop_path)):
                if len(r) < 6 or r[0] == "SN_EOR_FK":
                    continue
                key = (s(r[0]), s(r[1]))
                comp_props[key] = (to_float(r[2]), to_float(r[3]), s(r[4]), s(r[5]))

        rows = parse_delimited_file(str(comp_path))
        data = []
        for r in rows:
            if len(r) < 7 or r[0] == "SN_EOR_FK":
                continue
            key = (s(r[0]), s(r[1]))
            prop = comp_props.get(key, (None, None, None, None))
            data.append((
                to_int(r[0]), to_int(r[1]), s(r[2]), s(r[3]), s(r[4]), s(r[5]), s(r[6]),
                prop[0], prop[1], prop[2], prop[3],
            ))
        conn.executemany(f"INSERT INTO eor_completions VALUES ({','.join('?' * 11)})", data)
        print(f"  eor_completions: {len(data):,}")

    # Cut casings
    cc_path = EXTRACTED_DIR / "mv_eor_cut_casings.txt"
    if cc_path.exists():
        rows = parse_delimited_file(str(cc_path))
        data = []
        for r in rows:
            if len(r) < 6 or r[0] == "SN_EOR_FK":
                continue
            data.append((
                to_int(r[0]), s(r[1]), parse_date_mdy(r[2]), s(r[3]), to_float(r[4]), s(r[5]),
            ))
        conn.executemany(f"INSERT INTO eor_cut_casings VALUES ({','.join('?' * 6)})", data)
        print(f"  eor_cut_casings: {len(data):,}")

    # Geomarkers
    geo_path = EXTRACTED_DIR / "mv_eor_geomarkers.txt"
    if geo_path.exists():
        rows = parse_delimited_file(str(geo_path))
        data = []
        for r in rows:
            if len(r) < 3 or r[0] == "SN_EOR_FK":
                continue
            data.append((to_int(r[0]), s(r[1]), to_float(r[2])))
        conn.executemany(f"INSERT INTO eor_geomarkers VALUES ({','.join('?' * 3)})", data)
        print(f"  eor_geomarkers: {len(data):,}")

    # HC-bearing intervals
    hc_path = EXTRACTED_DIR / "mv_hcbearing_intervals.txt"
    if hc_path.exists():
        rows = parse_delimited_file(str(hc_path))
        data = []
        for r in rows:
            if len(r) < 6 or r[0] == "SN_HC_BEARING_INTVL":
                continue
            data.append((
                to_int(r[0]), to_int(r[1]), s(r[2]), to_float(r[3]), to_float(r[4]), s(r[5]),
            ))
        conn.executemany(f"INSERT INTO eor_hc_intervals VALUES ({','.join('?' * 6)})", data)
        print(f"  eor_hc_intervals: {len(data):,}")

    # Perforation intervals
    perf_path = EXTRACTED_DIR / "mv_eor_perf_intervals.txt"
    if perf_path.exists():
        rows = parse_delimited_file(str(perf_path))
        data = []
        for r in rows:
            if len(r) < 5 or r[0] == "SN_EOR_WELL_COMP_FK":
                continue
            data.append((
                to_int(r[0]), to_float(r[1]), to_float(r[2]), to_float(r[3]), to_float(r[4]),
            ))
        conn.executemany(f"INSERT INTO eor_perf_intervals VALUES ({','.join('?' * 5)})", data)
        print(f"  eor_perf_intervals: {len(data):,}")


def load_production_by_platform(conn):
    """Load production-by-platform data (BSEE)."""
    print("Loading production by platform...")

    # The zip contains a subdirectory — find the txt file
    src = EXTRACTED_DIR / "mv_prod_by_platform_all.txt"
    if not src.exists():
        # Try extracting from zip directly
        zf = RAW_DIR / "ProdByPlatformRawData.zip"
        if zf.exists():
            import tempfile
            tmpdir = tempfile.mkdtemp()
            with zipfile.ZipFile(zf, "r") as z:
                z.extractall(tmpdir)
            # Flatten nested directory
            for root, dirs, files in os.walk(tmpdir):
                for f in files:
                    if f.endswith(".txt"):
                        import shutil
                        shutil.move(os.path.join(root, f), str(EXTRACTED_DIR / f))
            shutil.rmtree(tmpdir)

    if not src.exists():
        print("  WARNING: mv_prod_by_platform_all.txt not found, skipping")
        return

    rows = parse_delimited_file(str(src))
    data = []
    for r in rows:
        if len(r) < 17:
            continue
        # Skip header row
        if r[0] == "COMPLEX_ID_NUM":
            continue
        data.append((
            to_int(r[0]),         # complex_id_num
            s(r[1]),              # structure_number
            s(r[2]),              # area_code
            s(r[3]),              # block_number
            s(r[4]),              # lease_number
            s(r[5]),              # structure_name
            parse_date_mdy(r[6]),  # install_date
            parse_date_mdy(r[7]),  # removal_date
            s(r[8]),              # operator
            s(r[9]),              # operator_num
            parse_date_mdy(r[10]),  # production_date
            to_int(r[11]),        # producing_wells
            to_float(r[12]),      # bopd
            to_float(r[13]),      # mcfpd
            to_float(r[14]),      # boepd
            to_float(r[15]),      # bwpd
            s(r[16]),             # region_code
        ))
    conn.executemany(f"INSERT INTO production_by_platform VALUES ({','.join('?' * 17)})", data)
    print(f"  {len(data):,} records")


# Map each loader to its source zip file(s) and the tables it populates.
# Format: (meta_key, zip_filenames, loader_fn, tables_to_clear)
LOADER_MAP = [
    ("companies",        ["company_all_delimit.zip"],        load_companies,          ["companies"]),
    ("rigs",             ["rig_id_delimit.zip", "eWellAPDRawData.zip", "eWellAPMRawData.zip"],
                                                             load_rigs,               ["rigs"]),
    ("fields",           ["field_names_delimit.zip"],        load_fields,             ["fields"]),
    ("field_production", ["field_production_delimit.zip"],   load_field_production,   ["field_production"]),
    ("appendices",       ["appendix_a_delimit.zip", "appendix_b_delimit.zip", "appendix_c_delimit.zip"],
                                                             load_appendices,         ["area_block_to_field", "lease_to_field", "operator_to_field"]),
    ("leases",           ["lease_data_fixed.zip"],           load_leases,             ["leases"]),
    ("lease_list",       ["lease_list_fixed.zip"],           load_lease_list,         ["lease_list"]),
    ("lease_owners",     ["lease_owner_op_delimit.zip"],     load_lease_owners,       ["lease_owners"]),
    ("platforms",        ["platform_master_fixed.zip"],      load_platforms,          ["platforms"]),
    ("platform_structs", ["platform_structure_fixed.zip"],   load_platform_structures,["platform_structures"]),
    ("platform_locs",    ["platform_location_fixed.zip"],    load_platform_locations, ["platform_locations"]),
    ("platform_approvals",["platform_approvals_delimit.zip"],load_platform_approvals,["platform_approvals"]),
    ("platform_removals",["platform_removed_delimit.zip"],   load_platform_removals,  ["platform_removals"]),
    ("wells",            ["borehole_delimit.zip"],           load_wells,              ["wells"]),
    ("apd",              ["eWellAPDRawData.zip"],            load_apd,               ["apd"]),
    ("apd_casing",       ["eWellAPDRawData.zip"],            load_apd_casing,        ["apd_casing_intervals", "apd_casing_sections"]),
    ("apd_geologic",     ["eWellAPDRawData.zip"],            load_apd_geologic,      ["apd_geologic"]),
    ("apm",              ["eWellAPMRawData.zip"],            load_apm,               ["apm"]),
    ("apm_sub_data",     ["eWellAPMRawData.zip"],            load_apm_sub_data,      ["apm_preventers", "apm_suboperations"]),
    ("pipelines",        ["pipeline_master_delimit.zip"],    load_pipelines,          ["pipelines"]),
    ("pipeline_locs",    ["pipeline_location_delimit.zip"],  load_pipeline_locations, ["pipeline_locations"]),
    ("prod_by_platform", ["ProdByPlatformRawData.zip"],      load_production_by_platform, ["production_by_platform"]),
    ("eor",              ["eWellEORRawData.zip"],             load_eor,                ["eor"]),
    ("eor_sub_data",     ["eWellEORRawData.zip"],             load_eor_sub_data,       ["eor_completions", "eor_cut_casings", "eor_geomarkers", "eor_hc_intervals", "eor_perf_intervals"]),
    ("war",              ["eWellWARRawData.zip"],             load_war,                ["war"]),
    ("war_sub_data",     ["eWellWARRawData.zip"],             load_war_sub_data,       ["war_boreholes", "war_tubulars"]),
]


def main():
    parser = argparse.ArgumentParser(description="BOEM Relational Database Builder")
    parser.add_argument("--full", action="store_true",
                        help="Full rebuild (delete existing DB and start fresh)")
    args = parser.parse_args()

    print("=" * 60)
    print("BOEM Relational Database Builder")
    print("=" * 60)

    # Extract zip files
    extract_if_needed()

    full_rebuild = args.full or not DB_PATH.exists()

    if full_rebuild and DB_PATH.exists():
        DB_PATH.unlink()
        print("Removed existing database (full rebuild)")
    elif not full_rebuild:
        print("Incremental mode — only reloading changed data sources")

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB cache

    try:
        create_schema(conn)
        init_build_meta(conn)
        conn.commit()

        skipped = 0
        loaded = 0

        for meta_key, zip_names, loader_fn, tables in LOADER_MAP:
            zip_paths = [RAW_DIR / z for z in zip_names]
            checksum = file_checksum(*zip_paths)

            if not full_rebuild and checksum and not source_changed(conn, meta_key, checksum):
                skipped += 1
                continue

            # Clear target tables before reloading
            for table in tables:
                conn.execute(f'DELETE FROM "{table}"')

            loader_fn(conn)
            if checksum:
                mark_built(conn, meta_key, checksum)
            conn.commit()
            loaded += 1

        # Production — handled separately (per-year incremental)
        if full_rebuild:
            conn.execute("DELETE FROM production")
            load_production(conn)
            conn.commit()
        else:
            load_production_incremental(conn)

        # Create views (always — they're cheap)
        create_views(conn)
        conn.commit()

        if not full_rebuild:
            print(f"\n  {loaded} data sources reloaded, {skipped} unchanged (skipped)")

        # Print summary
        print_summary(conn)

    finally:
        conn.close()

    print("\nDone!")


if __name__ == "__main__":
    main()
