const express = require('express');
const { db, cache } = require('../db');
const router = express.Router();

// ——— Typeahead search endpoint ———
// GET /api/production/search?q=&type=field|lease|operator
router.get('/search', (req, res) => {
  const q = (req.query.q || '').trim();
  const type = req.query.type || 'field';
  if (!q || q.length < 2) return res.json({ data: [] });

  const pattern = `%${q}%`;
  let rows;

  switch (type) {
    case 'field':
      rows = db.prepare(`
        SELECT DISTINCT field_name_code AS code,
               field_name_code || ' (' || area_code || ' ' || block_number || ')' AS label
        FROM fields
        WHERE field_name_code LIKE @q
           OR (area_code || block_number) LIKE @q
        ORDER BY field_name_code
        LIMIT 20
      `).all({ q: pattern });
      break;

    case 'lease':
      rows = db.prepare(`
        SELECT DISTINCT lease_number AS code,
               lease_number || COALESCE(' — ' || TRIM(area_block), '') AS label
        FROM production
        WHERE lease_number IS NOT NULL
          AND (lease_number LIKE @q
           OR area_block LIKE @q
           OR operator_name LIKE @q)
        ORDER BY lease_number
        LIMIT 20
      `).all({ q: pattern });
      break;

    case 'operator':
      rows = db.prepare(`
        SELECT company_num AS code,
               company_name AS label
        FROM companies
        WHERE company_name LIKE @q
           OR company_num LIKE @q
        ORDER BY company_name
        LIMIT 20
      `).all({ q: pattern });
      break;

    default:
      return res.json({ data: [] });
  }

  res.json({ data: rows });
});

// ——— Filter helper for production endpoints ———
// `exclude` is an array of param names the caller already handles (to avoid duplication)
function buildProductionFilters(query, exclude = []) {
  const { date_from, date_to, area_block, operator_num, field_name_code, lease_number } = query;
  let where = '1=1';
  const params = {};

  if (date_from) { where += ' AND production_date >= @date_from'; params.date_from = date_from; }
  if (date_to) { where += ' AND production_date <= @date_to'; params.date_to = date_to; }
  if (area_block && !exclude.includes('area_block')) { where += ' AND area_block LIKE @area_block'; params.area_block = `%${area_block}%`; }
  if (operator_num && !exclude.includes('operator_num')) { where += ' AND operator_num = @filt_op'; params.filt_op = operator_num; }
  if (field_name_code && !exclude.includes('field_name_code')) { where += ' AND field_name_code = @filt_field'; params.filt_field = field_name_code; }
  if (lease_number && !exclude.includes('lease_number')) { where += ' AND lease_number = @filt_lease'; params.filt_lease = lease_number; }

  return { where, params };
}

// GET /api/production/annual-summary — cached
router.get('/annual-summary', (req, res) => {
  res.json({ data: cache.annualSummary });
});

// GET /api/production/by-lease?lease_number=
router.get('/by-lease', (req, res) => {
  const { lease_number } = req.query;
  if (!lease_number) return res.status(400).json({ error: 'lease_number required' });

  const f = buildProductionFilters(req.query, ['lease_number']);
  f.where += ' AND lease_number = @lease_number';
  f.params.lease_number = lease_number;

  const rows = db.prepare(`
    SELECT production_date,
           SUM(oil_volume) AS oil,
           SUM(gas_volume) AS gas,
           SUM(water_volume) AS water,
           SUM(injection_volume) AS injection,
           COUNT(DISTINCT api_well_number) AS well_count
    FROM production
    WHERE ${f.where}
    GROUP BY production_date
    ORDER BY production_date
  `).all(f.params);

  res.json({ data: rows });
});

// GET /api/production/by-well?api_well_number=
router.get('/by-well', (req, res) => {
  const { api_well_number } = req.query;
  if (!api_well_number) return res.status(400).json({ error: 'api_well_number required' });

  const rows = db.prepare(`
    SELECT production_date,
           SUM(oil_volume) AS oil,
           SUM(gas_volume) AS gas,
           SUM(water_volume) AS water,
           SUM(injection_volume) AS injection
    FROM production
    WHERE api_well_number = @api
    GROUP BY production_date
    ORDER BY production_date
  `).all({ api: api_well_number });

  res.json({ data: rows });
});

// GET /api/production/by-field?field_name_code=
router.get('/by-field', (req, res) => {
  const { field_name_code } = req.query;
  if (!field_name_code) return res.status(400).json({ error: 'field_name_code required' });

  const f = buildProductionFilters(req.query, ['field_name_code']);
  f.where += ' AND field_name_code = @field';
  f.params.field = field_name_code;

  const rows = db.prepare(`
    SELECT production_date,
           SUM(oil_volume) AS oil,
           SUM(gas_volume) AS gas,
           SUM(water_volume) AS water,
           COUNT(DISTINCT api_well_number) AS well_count,
           COUNT(DISTINCT lease_number) AS lease_count
    FROM production
    WHERE ${f.where}
    GROUP BY production_date
    ORDER BY production_date
  `).all(f.params);

  res.json({ data: rows });
});

// GET /api/production/by-operator?operator_num=
router.get('/by-operator', (req, res) => {
  const { operator_num } = req.query;
  if (!operator_num) return res.status(400).json({ error: 'operator_num required' });

  const f = buildProductionFilters(req.query, ['operator_num']);
  f.where += ' AND operator_num = @op';
  f.params.op = operator_num;

  const rows = db.prepare(`
    SELECT production_date,
           SUM(oil_volume) AS oil,
           SUM(gas_volume) AS gas,
           SUM(water_volume) AS water,
           COUNT(DISTINCT api_well_number) AS well_count,
           COUNT(DISTINCT lease_number) AS lease_count
    FROM production
    WHERE ${f.where}
    GROUP BY production_date
    ORDER BY production_date
  `).all(f.params);

  res.json({ data: rows });
});

// GET /api/production/top-producers?by=oil&limit=20&year=&year_from=&year_to=
router.get('/top-producers', (req, res) => {
  const metric = req.query.by || 'oil';
  const limit = Math.min(100, Math.max(1, parseInt(req.query.limit) || 20));
  const year = req.query.year || null;
  const yearFrom = req.query.year_from || null;
  const yearTo = req.query.year_to || null;

  const validMetrics = { oil: 'oil_volume', gas: 'gas_volume', water: 'water_volume' };
  const col = validMetrics[metric] || 'oil_volume';

  const f = buildProductionFilters(req.query);
  f.params.limit = limit;

  // Support single year or year range
  if (year) {
    f.where += " AND SUBSTR(production_date, 1, 4) = @year";
    f.params.year = year;
  } else {
    if (yearFrom) {
      f.where += " AND SUBSTR(production_date, 1, 4) >= @yearFrom";
      f.params.yearFrom = yearFrom;
    }
    if (yearTo) {
      f.where += " AND SUBSTR(production_date, 1, 4) <= @yearTo";
      f.params.yearTo = yearTo;
    }
  }

  const rows = db.prepare(`
    SELECT lease_number,
           MAX(operator_name) AS operator_name,
           MAX(field_name_code) AS field_name_code,
           MAX(area_block) AS area_block,
           SUM(oil_volume) AS total_oil,
           SUM(gas_volume) AS total_gas,
           SUM(water_volume) AS total_water
    FROM production
    WHERE ${f.where}
    GROUP BY lease_number
    ORDER BY SUM(${col}) DESC
    LIMIT @limit
  `).all(f.params);

  res.json({ data: rows });
});

// GET /api/production/comparison?leases=X,Y,Z&metric=oil
router.get('/comparison', (req, res) => {
  const leases = (req.query.leases || '').split(',').filter(Boolean).slice(0, 5);
  if (leases.length === 0) return res.status(400).json({ error: 'leases parameter required' });

  const result = {};
  const stmt = db.prepare(`
    SELECT production_date,
           SUM(oil_volume) AS oil,
           SUM(gas_volume) AS gas,
           SUM(water_volume) AS water
    FROM production
    WHERE lease_number = @lease
    GROUP BY production_date
    ORDER BY production_date
  `);

  for (const lease of leases) {
    result[lease] = stmt.all({ lease });
  }

  res.json({ data: result });
});

module.exports = router;
