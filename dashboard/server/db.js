const path = require('path');
const Database = require('better-sqlite3');

const DB_PATH = path.join(__dirname, '../../boem.db');
const db = new Database(DB_PATH, { readonly: true });

// Performance pragmas
db.pragma('journal_mode = WAL');
db.pragma('cache_size = -64000');   // 64MB
db.pragma('mmap_size = 268435456'); // 256MB mmap
db.pragma('temp_store = MEMORY');

// Startup cache — precomputed stats for instant dashboard loads
const cache = {};

function buildCache() {
  console.log('Building startup cache...');
  const t0 = Date.now();

  // Table counts
  cache.counts = {
    wells: db.prepare('SELECT COUNT(*) as c FROM wells').get().c,
    leases: db.prepare('SELECT COUNT(*) as c FROM leases').get().c,
    activeLeases: db.prepare("SELECT COUNT(*) as c FROM leases WHERE lease_status NOT IN ('RELINQ','EXPIR','TERMIN')").get().c,
    platforms: db.prepare('SELECT COUNT(*) as c FROM platforms').get().c,
    producingPlatforms: db.prepare("SELECT COUNT(*) as c FROM platforms WHERE oil_producing = 'Y' OR gas_producing = 'Y'").get().c,
    pipelines: db.prepare('SELECT COUNT(*) as c FROM pipelines').get().c,
    activePipelines: db.prepare("SELECT COUNT(*) as c FROM pipelines WHERE status_code = 'ACT'").get().c,
    companies: db.prepare('SELECT COUNT(*) as c FROM companies').get().c,
    productionRecords: db.prepare('SELECT COUNT(*) as c FROM production').get().c,
  };

  // Cumulative production
  cache.cumulativeProduction = db.prepare(`
    SELECT SUM(oil_volume) as total_oil,
           SUM(gas_volume) as total_gas,
           SUM(water_volume) as total_water
    FROM production
  `).get();

  // Annual production summary
  cache.annualSummary = db.prepare(`
    SELECT SUBSTR(production_date, 1, 4) AS year,
           SUM(oil_volume) AS total_oil,
           SUM(gas_volume) AS total_gas,
           SUM(water_volume) AS total_water,
           COUNT(DISTINCT lease_number) AS lease_count,
           COUNT(DISTINCT api_well_number) AS well_count
    FROM production
    GROUP BY SUBSTR(production_date, 1, 4)
    ORDER BY year
  `).all();

  // Top 10 all-time producers
  cache.topProducers = db.prepare(`
    SELECT p.lease_number,
           MAX(p.operator_name) AS operator_name,
           MAX(p.field_name_code) AS field_name_code,
           SUM(p.oil_volume) AS total_oil,
           SUM(p.gas_volume) AS total_gas
    FROM production p
    GROUP BY p.lease_number
    ORDER BY total_oil DESC
    LIMIT 10
  `).all();

  // Latest production date
  cache.latestProductionDate = db.prepare('SELECT MAX(production_date) as d FROM production').get().d;

  // Filter options (distinct values for dropdowns)
  cache.filterOptions = {
    wellStatus: db.prepare('SELECT DISTINCT status_code FROM wells WHERE status_code IS NOT NULL ORDER BY status_code').all().map(r => r.status_code),
    wellStatusLabels: { C: 'Completed', D: 'Drilling', E: 'Plugged & Abandoned', O: 'Active/Producing', R: 'Relinquished' },
    wellType: db.prepare('SELECT DISTINCT type_code FROM wells WHERE type_code IS NOT NULL ORDER BY type_code').all().map(r => r.type_code),
    wellTypeLabels: { '01': 'Development', '02': 'Exploratory', '03': 'Dev. Sidetrack', '04': 'Expl. Sidetrack', '05': 'Other/Workover' },
    leaseStatus: db.prepare('SELECT DISTINCT lease_status FROM leases WHERE lease_status IS NOT NULL ORDER BY lease_status').all().map(r => r.lease_status),
    pipelineStatus: db.prepare('SELECT DISTINCT status_code FROM pipelines WHERE status_code IS NOT NULL ORDER BY status_code').all().map(r => r.status_code),
    pipelineProduct: db.prepare('SELECT DISTINCT product_code FROM pipelines WHERE product_code IS NOT NULL ORDER BY product_code').all().map(r => r.product_code),
    areaCodes: db.prepare('SELECT DISTINCT area_code FROM leases WHERE area_code IS NOT NULL ORDER BY area_code').all().map(r => r.area_code),
  };

  console.log(`  Cache built in ${Date.now() - t0}ms`);
}

// Ensure spatial indexes exist
function ensureIndexes() {
  const stmts = [
    'CREATE INDEX IF NOT EXISTS idx_wells_coords ON wells(surface_latitude, surface_longitude)',
    'CREATE INDEX IF NOT EXISTS idx_wells_name ON wells(well_name)',
    'CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(company_name)',
  ];
  // Can't create indexes on readonly DB — they should already exist from build_database.py
  // If not, user must run them manually. We'll just log a warning.
  try {
    for (const sql of stmts) {
      db.prepare(sql).run();
    }
  } catch {
    // Expected in readonly mode — indexes should already exist
  }
}

ensureIndexes();
buildCache();

module.exports = { db, cache };
