const express = require('express');
const { db } = require('../db');
const { pagination, paginatedResponse } = require('../middleware/pagination');
const router = express.Router();

const ALLOWED_SORTS = ['field_name_code', 'area_code', 'designated_operator', 'effective_date'];

// GET /api/fields — paginated list
router.get('/',
  pagination(ALLOWED_SORTS),
  (req, res) => {
    const { page, limit, offset, sort, order } = req.pagination;
    const { search, area_code } = req.query;

    let where = '1=1';
    const params = {};

    if (search) {
      where += ' AND (f.field_name_code LIKE @search OR f.designated_operator LIKE @search)';
      params.search = `%${search}%`;
    }
    if (area_code) { where += ' AND f.area_code = @area'; params.area = area_code; }

    const orderBy = sort ? `${sort} ${order}` : 'f.field_name_code ASC';

    // Deduplicate by field_name_code for the list (fields table has one row per field-lease pair)
    const total = db.prepare(`
      SELECT COUNT(DISTINCT f.field_name_code) as c
      FROM fields f WHERE ${where}
    `).get(params).c;

    const rows = db.prepare(`
      SELECT f.field_name_code, f.area_code,
             MIN(f.block_number) AS block_number,
             MAX(f.designated_operator) AS designated_operator,
             MIN(f.effective_date) AS effective_date,
             MAX(f.termination_date) AS termination_date,
             COUNT(DISTINCT f.lease_number) AS lease_count,
             fp.cum_oil_volume, fp.cum_gas_volume_1 AS cum_gas_volume, fp.cum_boe
      FROM fields f
      LEFT JOIN field_production fp ON f.field_name_code = fp.field_name_code
        AND f.lease_number = fp.lease_number
      WHERE ${where}
      GROUP BY f.field_name_code
      ORDER BY ${orderBy}
      LIMIT @limit OFFSET @offset
    `).all({ ...params, limit, offset });

    res.json(paginatedResponse(rows, total, { page, limit }));
  }
);

// GET /api/fields/:id — detail with cumulative production
router.get('/:id', (req, res) => {
  const leases = db.prepare(`
    SELECT f.*, fp.cum_oil_volume, fp.cum_gas_volume_1 AS cum_gas_volume,
           fp.cum_water_volume_1 AS cum_water_volume, fp.cum_boe,
           fp.first_production_date
    FROM fields f
    LEFT JOIN field_production fp ON f.field_name_code = fp.field_name_code
      AND f.lease_number = fp.lease_number
    WHERE f.field_name_code = @id
    ORDER BY fp.cum_oil_volume DESC
  `).all({ id: req.params.id });

  if (leases.length === 0) return res.status(404).json({ error: 'Field not found' });

  const totals = db.prepare(`
    SELECT SUM(cum_oil_volume) AS total_oil,
           SUM(cum_gas_volume_1) AS total_gas,
           SUM(cum_water_volume_1) AS total_water,
           SUM(cum_boe) AS total_boe
    FROM field_production
    WHERE field_name_code = @id
  `).get({ id: req.params.id });

  res.json({ field_name_code: req.params.id, leases, totals });
});

// GET /api/fields/:id/production — annual production for chart
router.get('/:id/production', (req, res) => {
  const rows = db.prepare(`
    SELECT SUBSTR(production_date, 1, 4) AS year,
           SUM(oil_volume) AS oil,
           SUM(gas_volume) AS gas,
           SUM(water_volume) AS water,
           COUNT(DISTINCT api_well_number) AS well_count
    FROM production
    WHERE field_name_code = @id
    GROUP BY SUBSTR(production_date, 1, 4)
    ORDER BY year
  `).all({ id: req.params.id });

  res.json({ data: rows });
});

module.exports = router;
