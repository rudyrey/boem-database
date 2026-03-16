const express = require('express');
const { db } = require('../db');
const { pagination, paginatedResponse } = require('../middleware/pagination');
const router = express.Router();

const ALLOWED_SORTS = [
  'api_well_number', 'well_name', 'operator_name', 'status_code',
  'type_code', 'total_measured_depth', 'water_depth', 'spud_date',
  'area_block', 'platform_name',
];

// GET /api/wells — paginated list with filters
router.get('/',
  pagination(ALLOWED_SORTS),
  (req, res) => {
    const { page, limit, offset, sort, order } = req.pagination;
    const { status_code, type_code, operator_num, spud_from, spud_to,
            min_depth, max_depth, min_tvd, max_tvd,
            min_water_depth, max_water_depth, search } = req.query;

    let where = '1=1';
    const params = {};

    // Multi-value status and type filters — OR across groups when both set
    const statusCodes = status_code ? status_code.split(',').filter(Boolean) : [];
    const typeCodes = type_code ? type_code.split(',').filter(Boolean) : [];

    if (statusCodes.length > 0 && typeCodes.length > 0) {
      // OR across the two filter groups
      const scPh = statusCodes.map((c, i) => { params[`sc_${i}`] = c; return `@sc_${i}`; }).join(',');
      const tcPh = typeCodes.map((c, i) => { params[`tc_${i}`] = c; return `@tc_${i}`; }).join(',');
      where += ` AND (w.status_code IN (${scPh}) OR w.type_code IN (${tcPh}))`;
    } else if (statusCodes.length > 0) {
      const ph = statusCodes.map((c, i) => { params[`sc_${i}`] = c; return `@sc_${i}`; }).join(',');
      where += ` AND w.status_code IN (${ph})`;
    } else if (typeCodes.length > 0) {
      const ph = typeCodes.map((c, i) => { params[`tc_${i}`] = c; return `@tc_${i}`; }).join(',');
      where += ` AND w.type_code IN (${ph})`;
    }
    if (operator_num) { where += ' AND w.operator_num = @operator_num'; params.operator_num = operator_num; }
    if (spud_from) { where += ' AND w.spud_date >= @spud_from'; params.spud_from = spud_from; }
    if (spud_to) { where += ' AND w.spud_date <= @spud_to'; params.spud_to = spud_to; }
    if (min_depth) { where += ' AND w.total_measured_depth >= @min_depth'; params.min_depth = parseInt(min_depth); }
    if (max_depth) { where += ' AND w.total_measured_depth <= @max_depth'; params.max_depth = parseInt(max_depth); }
    if (min_tvd) { where += ' AND w.true_vertical_depth >= @min_tvd'; params.min_tvd = parseInt(min_tvd); }
    if (max_tvd) { where += ' AND w.true_vertical_depth <= @max_tvd'; params.max_tvd = parseInt(max_tvd); }
    if (min_water_depth) { where += ' AND w.water_depth >= @min_wd'; params.min_wd = parseInt(min_water_depth); }
    if (max_water_depth) { where += ' AND w.water_depth <= @max_wd'; params.max_wd = parseInt(max_water_depth); }
    if (search) {
      where += ` AND (w.api_well_number LIKE @search
                  OR w.well_name LIKE @search
                  OR c.company_name LIKE @search)`;
      params.search = `%${search}%`;
    }

    const orderBy = sort ? `${sort} ${order}` : 'w.spud_date DESC';

    const total = db.prepare(`
      SELECT COUNT(*) as c
      FROM wells w
      LEFT JOIN companies c ON w.operator_num = c.company_num
      WHERE ${where}
    `).get(params).c;

    const rows = db.prepare(`
      SELECT w.api_well_number, w.well_name, w.operator_num,
             c.company_name AS operator_name,
             w.status_code, w.type_code, w.well_class,
             w.total_measured_depth, w.true_vertical_depth, w.water_depth,
             w.spud_date, w.completion_date,
             w.surface_latitude, w.surface_longitude,
             w.area_block, w.bottom_lease_number,
             ps.structure_name AS platform_name
      FROM wells w
      LEFT JOIN companies c ON w.operator_num = c.company_num
      LEFT JOIN platforms p ON w.bottom_lease_number = p.lease_number
      LEFT JOIN platform_structures ps ON p.complex_id = ps.complex_id
        AND ps.structure_number = (SELECT MIN(s2.structure_number) FROM platform_structures s2 WHERE s2.complex_id = p.complex_id)
      WHERE ${where}
      GROUP BY w.api_well_number
      ORDER BY ${orderBy}
      LIMIT @limit OFFSET @offset
    `).all({ ...params, limit, offset });

    res.json(paginatedResponse(rows, total, { page, limit }));
  }
);

// GET /api/wells/map — compact coords for map markers
router.get('/map', (req, res) => {
  const { bounds } = req.query;
  if (!bounds) return res.json({ data: [] });

  const [south, west, north, east] = bounds.split(',').map(Number);

  const rows = db.prepare(`
    SELECT api_well_number, surface_latitude AS lat, surface_longitude AS lng,
           status_code, type_code
    FROM wells
    WHERE surface_latitude BETWEEN @south AND @north
      AND surface_longitude BETWEEN @west AND @east
      AND surface_latitude IS NOT NULL
    LIMIT 10000
  `).all({ south, west, north, east });

  res.json({ data: rows });
});

// GET /api/wells/:id — single well detail
router.get('/:id', (req, res) => {
  const well = db.prepare(`
    SELECT w.*, c.company_name AS operator_name
    FROM wells w
    LEFT JOIN companies c ON w.operator_num = c.company_num
    WHERE w.api_well_number = @id
  `).get({ id: req.params.id });

  if (!well) return res.status(404).json({ error: 'Well not found' });
  res.json(well);
});

// GET /api/wells/:id/production — production history for one well
router.get('/:id/production', (req, res) => {
  const rows = db.prepare(`
    SELECT production_date,
           SUM(oil_volume) AS oil,
           SUM(gas_volume) AS gas,
           SUM(water_volume) AS water,
           SUM(injection_volume) AS injection
    FROM production
    WHERE api_well_number = @id
    GROUP BY production_date
    ORDER BY production_date
  `).all({ id: req.params.id });

  res.json({ data: rows });
});

// GET /api/wells/:id/platforms — platforms on the same lease
router.get('/:id/platforms', (req, res) => {
  const well = db.prepare(
    'SELECT bottom_lease_number FROM wells WHERE api_well_number = @id'
  ).get({ id: req.params.id });

  if (!well || !well.bottom_lease_number) return res.json({ data: [] });

  const rows = db.prepare(`
    SELECT p.complex_id, p.area_code, p.block_number, p.water_depth,
           p.oil_producing, p.gas_producing, p.drilling,
           ps.structure_name, ps.structure_type,
           c.company_name AS operator_name
    FROM platforms p
    LEFT JOIN companies c ON p.company_num = c.company_num
    LEFT JOIN platform_structures ps ON p.complex_id = ps.complex_id
      AND ps.structure_number = (SELECT MIN(s2.structure_number) FROM platform_structures s2 WHERE s2.complex_id = p.complex_id)
    WHERE p.lease_number = @lease
    ORDER BY p.complex_id
  `).all({ lease: well.bottom_lease_number });

  res.json({ data: rows });
});

module.exports = router;
