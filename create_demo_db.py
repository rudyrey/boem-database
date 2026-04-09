#!/usr/bin/env python3
"""Create a small demo SQLite database for previewing the dashboard."""

import sqlite3
import random

DB_PATH = "boem.db"

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Create all required tables
c.executescript("""
CREATE TABLE IF NOT EXISTS companies (
    company_num TEXT PRIMARY KEY, company_name TEXT, sort_name TEXT,
    start_date TEXT, term_date TEXT, pacific_region TEXT, gom_region TEXT,
    alaska_region TEXT, atlantic_region TEXT, duns_number TEXT,
    term_action_date TEXT, termination_code TEXT, division_name TEXT,
    address_line1 TEXT, address_line2 TEXT, city TEXT, state_code TEXT,
    zip_code TEXT, country TEXT
);

CREATE TABLE IF NOT EXISTS fields (
    field_name_code TEXT, lease_number TEXT, area_code TEXT, block_number TEXT,
    eia_code TEXT, designated_operator TEXT, effective_date TEXT,
    termination_date TEXT, termination_code TEXT, lease_portion TEXT,
    PRIMARY KEY (field_name_code, lease_number)
);

CREATE TABLE IF NOT EXISTS leases (
    lease_number TEXT PRIMARY KEY, serial_type_code TEXT, sale_number TEXT,
    expected_expiration TEXT, api_state_county TEXT, tract_number TEXT,
    effective_date TEXT, primary_term INTEGER, expiration_date TEXT,
    bid_system_code TEXT, royalty_rate REAL, initial_area REAL,
    current_area REAL, rent_per_unit REAL, bid_amount REAL,
    bid_per_unit REAL, min_water_depth INTEGER, max_water_depth INTEGER,
    measure_flag TEXT, planning_area_code TEXT, district_code TEXT,
    lease_status TEXT, status_effective_date TEXT, suspension_expiration TEXT,
    suspension_type TEXT, well_name TEXT, qualifying_well_type TEXT,
    qualifying_date TEXT, discovery_type TEXT, field_discovery TEXT,
    distance_to_shore INTEGER, num_platforms INTEGER,
    platform_approval_date TEXT, first_platform_set_date TEXT,
    lease_section_code TEXT, postal_state_code TEXT, lease_section_area REAL,
    protraction_number TEXT, suspension_eff_date TEXT,
    first_production_date TEXT, area_code TEXT, block_number TEXT
);

CREATE TABLE IF NOT EXISTS platforms (
    complex_id TEXT PRIMARY KEY, company_num TEXT, lease_number TEXT,
    area_code TEXT, block_number TEXT, field_name_code TEXT,
    district_code TEXT, water_depth INTEGER, distance_to_shore INTEGER,
    oil_producing TEXT, gas_producing TEXT, water_producing TEXT,
    condensate_producing TEXT, drilling TEXT, manned_24hr TEXT,
    attended_8hr TEXT, heliport TEXT, sulfur_producing TEXT,
    compressor TEXT, workover TEXT, injection_code TEXT,
    production_flag TEXT, prod_equipment TEXT, power_source TEXT,
    power_gen TEXT, major_complex TEXT, rig_count INTEGER,
    crane_count INTEGER, bed_count INTEGER, subdistrict_code TEXT,
    last_revision_date TEXT
);

CREATE TABLE IF NOT EXISTS wells (
    api_well_number TEXT PRIMARY KEY, well_name TEXT, well_name_suffix TEXT,
    operator_num TEXT, bottom_field_code TEXT, spud_date TEXT,
    bottom_lease_number TEXT, rkb_elevation INTEGER,
    total_measured_depth INTEGER, true_vertical_depth INTEGER,
    water_depth INTEGER, surface_longitude REAL, surface_latitude REAL,
    bottom_longitude REAL, bottom_latitude REAL, status_code TEXT,
    type_code TEXT, well_class TEXT, district_code TEXT,
    area_block TEXT, completion_date TEXT, plugback_date TEXT
);

CREATE TABLE IF NOT EXISTS pipelines (
    segment_num TEXT PRIMARY KEY, segment_length REAL, origin_name TEXT,
    origin_area TEXT, origin_block TEXT, origin_lease TEXT,
    dest_name TEXT, dest_area TEXT, dest_block TEXT, dest_lease TEXT,
    abandon_approval TEXT, abandon_date TEXT, approved_date TEXT,
    auth_code TEXT, boarding_sdv TEXT, buried_flag TEXT,
    cathodic_life INTEGER, flow_direction TEXT, construction_date TEXT,
    leak_detection TEXT, last_revision TEXT, hydrotest_date TEXT,
    fed_state_length REAL, status_code TEXT, pipe_size TEXT,
    row_number TEXT, recv_maop REAL, recv_segment TEXT,
    proposed_const_date TEXT, product_code TEXT, system_code TEXT,
    row_permittee TEXT, facility_operator TEXT, min_water_depth INTEGER,
    max_water_depth INTEGER, protraction_number TEXT, maop_pressure REAL,
    cathodic_code TEXT, bidirectional TEXT, boarding_fsv TEXT,
    approval_code TEXT, abandon_type TEXT
);

CREATE TABLE IF NOT EXISTS production (
    lease_number TEXT, completion_name TEXT, production_date TEXT,
    days_on_production INTEGER, product_code TEXT, oil_volume REAL,
    gas_volume REAL, water_volume REAL, api_well_number TEXT,
    well_status TEXT, area_block TEXT, operator_num TEXT,
    operator_name TEXT, field_name_code TEXT, injection_volume REAL,
    prod_interval_code TEXT, first_prod_date TEXT, unit_agreement TEXT,
    unit_alloc_suffix TEXT
);

CREATE TABLE IF NOT EXISTS eor (
    sn_eor INTEGER PRIMARY KEY, operation_cd TEXT, api_well_number TEXT,
    well_name TEXT, well_nm_st_sfix TEXT, well_nm_bp_sfix TEXT,
    company_num TEXT, bus_asc_name TEXT, botm_lease_number TEXT,
    botm_area_code TEXT, botm_block_number TEXT, surf_lease_number TEXT,
    surf_area_code TEXT, surf_block_number TEXT, borehole_stat_cd TEXT,
    borehole_stat_dt TEXT, operational_narrative TEXT,
    subsea_completion TEXT, subsea_protection TEXT, subsea_buoy TEXT,
    subsea_tree_height REAL, obstruction_protection TEXT,
    obstruction_type_cd TEXT, obstruction_buoy TEXT,
    obstruction_height REAL, botm_longitude REAL, botm_latitude REAL,
    total_md REAL, well_bore_tvd REAL, kickoff_md REAL
);

CREATE TABLE IF NOT EXISTS war (
    sn_war INTEGER PRIMARY KEY, war_start_dt TEXT, war_end_dt TEXT,
    contact_name TEXT, phone_number TEXT, rig_name TEXT,
    water_depth REAL, rkb_elevation REAL, bop_test_date TEXT,
    ram_tst_prss REAL, annular_tst_prss REAL, bus_asc_name TEXT,
    company_num TEXT, api_well_number TEXT, well_name TEXT,
    well_nm_bp_sfix TEXT, well_nm_st_sfix TEXT, surf_lease_num TEXT,
    surf_area_code TEXT, surf_block_num TEXT, botm_lease_num TEXT,
    botm_area_code TEXT, botm_block_num TEXT, well_activity_cd TEXT,
    well_actv_start_dt TEXT, well_actv_end_dt TEXT, total_depth_date TEXT,
    drilling_md REAL, drilling_tvd REAL, drill_fluid_wgt REAL
);

CREATE TABLE IF NOT EXISTS apd (
    sn_apd TEXT PRIMARY KEY, api_well_number TEXT, operator_num TEXT,
    well_name TEXT, permit_type TEXT, well_type_code TEXT,
    water_depth INTEGER, req_spud_date TEXT, apd_status_dt TEXT,
    apd_sub_status_dt TEXT, surf_area_code TEXT, surf_block_number TEXT,
    surf_lease_number TEXT, botm_area_code TEXT, botm_block_number TEXT,
    botm_lease_number TEXT, rig_name TEXT, rig_type_code TEXT,
    rig_id_num TEXT, bus_asc_name TEXT
);

CREATE TABLE IF NOT EXISTS apm (
    sn_apm TEXT PRIMARY KEY, api_well_number TEXT, operator_num TEXT,
    well_name TEXT, well_type_code TEXT, water_depth INTEGER,
    borehole_stat_cd TEXT, apm_op_cd TEXT, acc_status_date TEXT,
    sub_stat_date TEXT, surf_area_code TEXT, surf_block_num TEXT,
    surf_lease_num TEXT, botm_area_code TEXT, botm_block_num TEXT,
    botm_lease_num TEXT, rig_id_num TEXT, bus_asc_name TEXT,
    sv_type TEXT, est_operation_days INTEGER, work_commences_date TEXT
);

CREATE TABLE IF NOT EXISTS field_production (
    field_name_code TEXT, lease_number TEXT, cum_oil_volume REAL,
    cum_gas_volume_1 REAL, cum_gas_volume_2 REAL, cum_cond_volume REAL,
    cum_water_volume_1 REAL, cum_water_volume_2 REAL, cum_boe REAL,
    first_production_date TEXT,
    PRIMARY KEY (field_name_code, lease_number)
);

CREATE TABLE IF NOT EXISTS lease_owners (
    lease_number TEXT, company_num TEXT, assignment_pct REAL,
    assignment_approval TEXT, assignment_effective TEXT,
    assignment_term TEXT, assignment_status TEXT,
    owner_aliquot TEXT, owner_group TEXT, designated_operator TEXT
);

CREATE TABLE IF NOT EXISTS platform_structures (
    complex_id TEXT, structure_number TEXT, structure_name TEXT,
    structure_type TEXT, area_code TEXT, block_number TEXT,
    install_date TEXT, removal_date TEXT, deck_count INTEGER,
    slot_count INTEGER, slant_slot_count INTEGER, slot_drill_count INTEGER,
    satellite_count INTEGER, underwater_count INTEGER, major_structure TEXT,
    ns_departure TEXT, ew_departure TEXT, ns_distance TEXT, ew_distance TEXT,
    authority_type TEXT, authority_number TEXT, authority_status TEXT,
    last_revision_date TEXT,
    PRIMARY KEY (complex_id, structure_number)
);

CREATE TABLE IF NOT EXISTS platform_locations (
    complex_id TEXT, structure_number TEXT, district_code TEXT,
    area_code TEXT, block_number TEXT, structure_name TEXT,
    longitude REAL, latitude REAL, x_location REAL, y_location REAL,
    ns_distance TEXT, ns_code TEXT, ew_distance TEXT, ew_code TEXT,
    PRIMARY KEY (complex_id, structure_number)
);

CREATE TABLE IF NOT EXISTS pipeline_locations (
    segment_num TEXT, point_seq INTEGER, latitude REAL, longitude REAL,
    nad_year TEXT, proj_code TEXT, x_coord REAL, y_coord REAL,
    last_revision TEXT, version_date TEXT, asbuilt_flag TEXT,
    PRIMARY KEY (segment_num, point_seq)
);

CREATE TABLE IF NOT EXISTS production_by_platform (
    complex_id_num INTEGER, structure_number TEXT, area_code TEXT,
    block_number TEXT, lease_number TEXT, structure_name TEXT,
    install_date TEXT, removal_date TEXT, operator TEXT,
    operator_num TEXT, production_date TEXT, producing_wells INTEGER,
    bopd REAL, mcfpd REAL, boepd REAL, bwpd REAL, region_code TEXT
);

CREATE TABLE IF NOT EXISTS rigs (
    rig_id TEXT PRIMARY KEY, rig_name TEXT, rig_type TEXT,
    rig_func_code TEXT, construction_yr INTEGER, shipyard_name TEXT,
    refurbished_yr INTEGER, rated_water_depth INTEGER,
    rated_drill_depth INTEGER, abs_cert_expr_dt TEXT,
    cg_cert_expr_dt TEXT, anchor_flag TEXT, apd_count INTEGER DEFAULT 0,
    apm_count INTEGER DEFAULT 0, last_permit_date TEXT
);

CREATE TABLE IF NOT EXISTS lease_list (
    lease_number TEXT, district_code TEXT, appeal_flag TEXT,
    pending_flag TEXT, mineral_type TEXT, area_block TEXT,
    multi_partial TEXT, lease_status TEXT, status_date TEXT,
    order4_det TEXT, status_flag TEXT, designated_operator TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_wells_operator ON wells(operator_num);
CREATE INDEX IF NOT EXISTS idx_wells_spud ON wells(spud_date);
CREATE INDEX IF NOT EXISTS idx_wells_area ON wells(area_block);
CREATE INDEX IF NOT EXISTS idx_wells_lease ON wells(bottom_lease_number);
CREATE INDEX IF NOT EXISTS idx_leases_area ON leases(area_code, block_number);
CREATE INDEX IF NOT EXISTS idx_production_lease ON production(lease_number);
CREATE INDEX IF NOT EXISTS idx_production_date ON production(production_date);
CREATE INDEX IF NOT EXISTS idx_production_api ON production(api_well_number);
CREATE INDEX IF NOT EXISTS idx_wells_coords ON wells(surface_latitude, surface_longitude);
CREATE INDEX IF NOT EXISTS idx_wells_name ON wells(well_name);
CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(company_name);
""")

# ---- Sample Data ----
random.seed(42)

operators = [
    ("00100", "Shell Offshore Inc.", "SHELL OFFSHORE"),
    ("00200", "BP Exploration & Production", "BP EXPLORATION"),
    ("00300", "Chevron U.S.A. Inc.", "CHEVRON USA"),
    ("00400", "ExxonMobil Production Co.", "EXXONMOBIL PROD"),
    ("00500", "ConocoPhillips Co.", "CONOCOPHILLIPS"),
    ("00600", "Anadarko Petroleum Corp.", "ANADARKO PETRO"),
    ("00700", "Murphy Exploration & Prod.", "MURPHY EXPLOR"),
    ("00800", "LLOG Exploration Co.", "LLOG EXPLORATION"),
    ("00900", "W&T Offshore Inc.", "W&T OFFSHORE"),
    ("01000", "Hess Corporation", "HESS CORP"),
]

for num, name, sort_name in operators:
    c.execute("INSERT INTO companies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
              (num, name, sort_name, "2000-01-01", None, None, "Y", None, None,
               None, None, None, None, f"{num} Main St", None, "Houston", "TX", "77001", "US"))

area_codes = ["MC", "GC", "EW", "VK", "AT", "WR", "GB", "SP", "SS"]
field_codes = ["MARS", "THUNDER HORSE", "ATLANTIS", "MAD DOG", "TAHITI",
               "PERDIDO", "JACK/ST MALO", "LUCIUS", "DELTA HOUSE", "STONES"]
statuses = ["PROD", "UNIT", "RELINQ", "EXPIR", "SOP", "SOO", "SEGR"]

# Leases
for i in range(200):
    ln = f"G{str(i+1).zfill(5)}"
    ac = random.choice(area_codes)
    bn = str(random.randint(1, 999))
    status = random.choice(statuses[:4])
    wd_min = random.randint(100, 3000)
    c.execute("""INSERT INTO leases (lease_number, area_code, block_number,
        lease_status, effective_date, expiration_date, min_water_depth,
        max_water_depth, royalty_rate, bid_amount, district_code,
        first_production_date)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
              (ln, ac, bn, status, f"{random.randint(1990,2020)}-01-15",
               f"{random.randint(2025,2040)}-01-15", wd_min, wd_min + 200,
               random.choice([0.125, 0.1667, 0.1875]),
               random.randint(100000, 50000000), str(random.randint(1,4)),
               f"{random.randint(1998,2022)}-{random.randint(1,12):02d}-01" if status == "PROD" else None))

# Wells
for i in range(500):
    api = f"608{str(random.randint(10,50)):>3s}{str(i+1).zfill(5)}00"
    ac = random.choice(area_codes)
    bn = str(random.randint(1, 999))
    lat = 27.0 + random.random() * 2
    lon = -91.5 + random.random() * 3
    c.execute("""INSERT INTO wells VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (api, f"WELL-{ac}-{bn}-{i+1}", random.choice(["","ST1","ST2","BP1"]),
               random.choice(operators)[0], random.choice(field_codes),
               f"{random.randint(2000,2024)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
               f"G{str(random.randint(1,200)).zfill(5)}",
               random.randint(50,150), random.randint(8000,30000),
               random.randint(7000,25000), random.randint(100,3000),
               lon, lat, lon + random.uniform(-0.01, 0.01),
               lat + random.uniform(-0.01, 0.01),
               random.choice(["C","D","E","O","R"]),
               random.choice(["01","02","03","04"]),
               random.choice(["Oil","Gas","Injection"]),
               str(random.randint(1,4)),
               f"{ac}{bn.zfill(6)}", None, None))

# Platforms
for i in range(150):
    cid = str(10000 + i)
    ac = random.choice(area_codes)
    bn = str(random.randint(1,999))
    op = random.choice(operators)
    oil_p = random.choice(["Y","N"])
    gas_p = random.choice(["Y","N"])
    c.execute("""INSERT INTO platforms (complex_id, company_num, lease_number,
        area_code, block_number, field_name_code, district_code, water_depth,
        distance_to_shore, oil_producing, gas_producing, water_producing,
        condensate_producing, drilling, manned_24hr, heliport)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (cid, op[0], f"G{str(random.randint(1,200)).zfill(5)}", ac, bn,
               random.choice(field_codes), str(random.randint(1,4)),
               random.randint(100,3000), random.randint(10,200),
               oil_p, gas_p, random.choice(["Y","N"]),
               random.choice(["Y","N"]), random.choice(["Y","N"]),
               random.choice(["Y","N"]), random.choice(["Y","N"])))

# Pipelines
for i in range(300):
    seg = f"S{str(i+1).zfill(5)}"
    ac1 = random.choice(area_codes)
    ac2 = random.choice(area_codes)
    c.execute("""INSERT INTO pipelines (segment_num, segment_length, origin_name,
        origin_area, origin_block, dest_name, dest_area, dest_block,
        status_code, product_code, pipe_size, approved_date, construction_date,
        min_water_depth, max_water_depth, facility_operator)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (seg, random.uniform(0.5, 50.0), f"Platform-{ac1}-{random.randint(1,999)}",
               ac1, str(random.randint(1,999)),
               f"Platform-{ac2}-{random.randint(1,999)}",
               ac2, str(random.randint(1,999)),
               random.choice(["ACT","OUT","ABN","PRO"]),
               random.choice(["OIL","GAS","CON","WAT"]),
               random.choice(["6","8","10","12","16","20"]),
               f"{random.randint(1990,2023)}-{random.randint(1,12):02d}-01",
               f"{random.randint(1990,2023)}-{random.randint(1,12):02d}-01",
               random.randint(50,500), random.randint(500,3000),
               random.choice(operators)[1]))

# Production records (monthly, 2020-2025)
for year in range(2020, 2026):
    for month in range(1, 13):
        if year == 2025 and month > 9:
            break
        prod_date = f"{year}-{month:02d}"
        for lease_idx in range(50):
            ln = f"G{str(lease_idx+1).zfill(5)}"
            api = f"608{str(random.randint(10,50)):>3s}{str(lease_idx+1).zfill(5)}00"
            op = random.choice(operators)
            oil = random.uniform(1000, 500000) if random.random() > 0.1 else 0
            gas = random.uniform(5000, 2000000) if random.random() > 0.1 else 0
            water = random.uniform(500, 300000) if random.random() > 0.2 else 0
            c.execute("""INSERT INTO production (lease_number, completion_name,
                production_date, days_on_production, product_code, oil_volume,
                gas_volume, water_volume, api_well_number, well_status,
                area_block, operator_num, operator_name, field_name_code)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                      (ln, f"COMP-{lease_idx+1:03d}", prod_date,
                       random.randint(20,31), random.choice(["O","G"]),
                       oil, gas, water, api, random.choice(["FL","SI","TA"]),
                       f"{random.choice(area_codes)}{str(random.randint(1,999)).zfill(6)}",
                       op[0], op[1], random.choice(field_codes)))

# EOR records
for i in range(50):
    c.execute("""INSERT INTO eor (sn_eor, operation_cd, api_well_number,
        well_name, company_num, bus_asc_name, botm_lease_number,
        botm_area_code, botm_block_number, borehole_stat_cd, total_md)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
              (i+1, random.choice(["DRL","WO","PA","TA"]),
               f"608{str(random.randint(10,50)):>3s}{str(i+1).zfill(5)}00",
               f"WELL-{random.choice(area_codes)}-{random.randint(1,999)}",
               random.choice(operators)[0], random.choice(operators)[1],
               f"G{str(random.randint(1,200)).zfill(5)}",
               random.choice(area_codes), str(random.randint(1,999)),
               random.choice(["COM","PA","TA","DRL"]),
               random.randint(8000, 30000)))

# WAR records
for i in range(50):
    c.execute("""INSERT INTO war (sn_war, war_start_dt, war_end_dt,
        rig_name, water_depth, company_num, api_well_number, well_name,
        surf_area_code, surf_block_num, well_activity_cd, drilling_md)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
              (i+1, f"2024-{random.randint(1,12):02d}-01",
               f"2024-{random.randint(1,12):02d}-28",
               f"RIG-{random.randint(100,999)}", random.randint(100,3000),
               random.choice(operators)[0],
               f"608{str(random.randint(10,50)):>3s}{str(i+1).zfill(5)}00",
               f"WELL-{random.choice(area_codes)}-{random.randint(1,999)}",
               random.choice(area_codes), str(random.randint(1,999)),
               random.choice(["DRL","CMT","LOG","DST"]),
               random.randint(5000, 25000)))

# APD records (submissions)
for i in range(80):
    c.execute("""INSERT INTO apd (sn_apd, api_well_number, operator_num,
        well_name, permit_type, well_type_code, water_depth, req_spud_date,
        apd_status_dt, surf_area_code, surf_block_number, surf_lease_number,
        bus_asc_name)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (f"APD-{i+1:06d}",
               f"608{str(random.randint(10,50)):>3s}{str(i+1).zfill(5)}00",
               random.choice(operators)[0],
               f"WELL-{random.choice(area_codes)}-{random.randint(1,999)}",
               random.choice(["INITIAL","REVISED","SUPPLEMENTAL"]),
               random.choice(["01","02","03","04"]),
               random.randint(100,3000),
               f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
               f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
               random.choice(area_codes), str(random.randint(1,999)),
               f"G{str(random.randint(1,200)).zfill(5)}",
               random.choice(operators)[1]))

# APM records (submissions)
for i in range(60):
    c.execute("""INSERT INTO apm (sn_apm, api_well_number, operator_num,
        well_name, well_type_code, water_depth, borehole_stat_cd,
        apm_op_cd, acc_status_date, surf_area_code, surf_block_num,
        surf_lease_num, bus_asc_name)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (f"APM-{i+1:06d}",
               f"608{str(random.randint(10,50)):>3s}{str(i+1).zfill(5)}00",
               random.choice(operators)[0],
               f"WELL-{random.choice(area_codes)}-{random.randint(1,999)}",
               random.choice(["01","02","03","04"]),
               random.randint(100,3000),
               random.choice(["COM","PA","TA","DRL"]),
               random.choice(["DRL","CMT","LOG"]),
               f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
               random.choice(area_codes), str(random.randint(1,999)),
               f"G{str(random.randint(1,200)).zfill(5)}",
               random.choice(operators)[1]))

conn.commit()
conn.close()
print(f"Demo database created at {DB_PATH}")
print(f"  Companies: {len(operators)}")
print(f"  Leases: 200")
print(f"  Wells: 500")
print(f"  Platforms: 150")
print(f"  Pipelines: 300")
print(f"  Production records: ~3450")
print(f"  EOR: 50, WAR: 50, APD: 80, APM: 60")
