const express = require('express');
const { db } = require('../db');
const { pagination, paginatedResponse } = require('../middleware/pagination');
const router = express.Router();

const ALLOWED_SORTS = [
  'complex_id', 'area_code', 'block_number', 'operator_name',
  'water_depth', 'structure_type', 'install_date',
];

// GET /api/platforms — paginated list
router.get('/',
  pagination(ALLOWED_SORTS),
  (req, res) => {
    const { page, limit, offset, sort, order } = req.pagination;
    const { oil_producing, gas_producing, drilling, manned_24hr, area_code, search } = req.query;

    let where = '1=1';
    const params = {};

    if (oil_producing) { where += ' AND p.oil_producing = @oil'; params.oil = oil_producing; }
    if (gas_producing) { where += ' AND p.gas_producing = @gas'; params.gas = gas_producing; }
    if (drilling) { where += ' AND p.drilling = @drill'; params.drill = drilling; }
    if (manned_24hr) { where += ' AND p.manned_24hr = @manned'; params.manned = manned_24hr; }
    if (area_code) { where += ' AND p.area_code = @area_code'; params.area_code = area_code; }
    if (search) {
      where += ` AND (p.complex_id LIKE @search
                  OR ps.structure_name LIKE @search
                  OR c.company_name LIKE @search
                  OR (p.area_code || ' ' || p.block_number) LIKE @search)`;
      params.search = `%${search}%`;
    }

    const orderBy = sort ? `${sort} ${order}` : 'p.complex_id ASC';

    const total = db.prepare(`
      SELECT COUNT(*) as c
      FROM platforms p
      LEFT JOIN companies c ON p.company_num = c.company_num
      LEFT JOIN platform_structures ps ON p.complex_id = ps.complex_id
        AND ps.structure_number = (SELECT MIN(s2.structure_number) FROM platform_structures s2 WHERE s2.complex_id = p.complex_id)
      WHERE ${where}
    `).get(params).c;

    const rows = db.prepare(`
      SELECT p.complex_id, p.area_code, p.block_number,
             p.company_num, c.company_name AS operator_name,
             p.water_depth, p.distance_to_shore,
             p.oil_producing, p.gas_producing, p.water_producing,
             p.drilling, p.manned_24hr, p.heliport,
             p.major_complex, p.rig_count, p.bed_count,
             p.lease_number, p.field_name_code,
             ps.structure_name, ps.structure_type, ps.install_date, ps.removal_date,
             ps.slot_count, ps.slot_drill_count,
             pl.longitude, pl.latitude
      FROM platforms p
      LEFT JOIN companies c ON p.company_num = c.company_num
      LEFT JOIN platform_structures ps ON p.complex_id = ps.complex_id
        AND ps.structure_number = (SELECT MIN(s2.structure_number) FROM platform_structures s2 WHERE s2.complex_id = p.complex_id)
      LEFT JOIN platform_locations pl ON p.complex_id = pl.complex_id
        AND pl.structure_number = (SELECT MIN(l2.structure_number) FROM platform_locations l2 WHERE l2.complex_id = p.complex_id)
      WHERE ${where}
      ORDER BY ${orderBy}
      LIMIT @limit OFFSET @offset
    `).all({ ...params, limit, offset });

    res.json(paginatedResponse(rows, total, { page, limit }));
  }
);

// GET /api/platforms/map — compact coords
router.get('/map', (req, res) => {
  const { bounds } = req.query;
  if (!bounds) return res.json({ data: [] });

  const [south, west, north, east] = bounds.split(',').map(Number);

  const rows = db.prepare(`
    SELECT pl.complex_id, pl.latitude AS lat, pl.longitude AS lng,
           p.oil_producing, p.gas_producing, p.drilling,
           ps.structure_type, ps.structure_name
    FROM platform_locations pl
    JOIN platforms p ON pl.complex_id = p.complex_id
    LEFT JOIN platform_structures ps ON p.complex_id = ps.complex_id
      AND ps.structure_number = (SELECT MIN(s2.structure_number) FROM platform_structures s2 WHERE s2.complex_id = p.complex_id)
    WHERE pl.latitude BETWEEN @south AND @north
      AND pl.longitude BETWEEN @west AND @east
      AND pl.latitude IS NOT NULL
    LIMIT 10000
  `).all({ south, west, north, east });

  res.json({ data: rows });
});

// GET /api/platforms/:id — detail
router.get('/:id', (req, res) => {
  const platform = db.prepare(`
    SELECT p.*, c.company_name AS operator_name
    FROM platforms p
    LEFT JOIN companies c ON p.company_num = c.company_num
    WHERE p.complex_id = @id
  `).get({ id: req.params.id });

  if (!platform) return res.status(404).json({ error: 'Platform not found' });

  const structures = db.prepare(`
    SELECT * FROM platform_structures WHERE complex_id = @id
    ORDER BY structure_number
  `).all({ id: req.params.id });

  const locations = db.prepare(`
    SELECT * FROM platform_locations WHERE complex_id = @id
    ORDER BY structure_number
  `).all({ id: req.params.id });

  res.json({ ...platform, structures, locations });
});

// GET /api/platforms/:id/wells — wells on the same lease as this platform
router.get('/:id/wells', (req, res) => {
  const platform = db.prepare(
    'SELECT lease_number FROM platforms WHERE complex_id = @id'
  ).get({ id: req.params.id });

  if (!platform || !platform.lease_number) return res.json({ data: [] });

  const rows = db.prepare(`
    SELECT w.api_well_number, w.well_name, w.status_code, w.type_code,
           w.spud_date, w.total_measured_depth, w.water_depth,
           c.company_name AS operator_name
    FROM wells w
    LEFT JOIN companies c ON w.operator_num = c.company_num
    WHERE w.bottom_lease_number = @lease
    ORDER BY w.spud_date DESC
  `).all({ lease: platform.lease_number });

  res.json({ data: rows });
});

module.exports = router;
