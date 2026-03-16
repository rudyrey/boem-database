const express = require('express');
const { db } = require('../db');
const { pagination, paginatedResponse } = require('../middleware/pagination');
const router = express.Router();

const ALLOWED_SORTS = [
  'segment_num', 'origin_name', 'dest_name', 'product_code',
  'status_code', 'pipe_size', 'segment_length', 'max_water_depth',
];

// GET /api/pipelines — paginated list
router.get('/',
  pagination(ALLOWED_SORTS),
  (req, res) => {
    const { page, limit, offset, sort, order } = req.pagination;
    const { status_code, product_code, search } = req.query;

    let where = '1=1';
    const params = {};

    if (status_code) { where += ' AND p.status_code = @status'; params.status = status_code; }
    if (product_code) { where += ' AND p.product_code = @prod'; params.prod = product_code; }
    if (search) {
      where += ` AND (p.segment_num LIKE @search
                  OR p.origin_name LIKE @search
                  OR p.dest_name LIKE @search
                  OR c.company_name LIKE @search)`;
      params.search = `%${search}%`;
    }

    const orderBy = sort ? `${sort} ${order}` : 'p.segment_num ASC';

    const total = db.prepare(`
      SELECT COUNT(*) as c
      FROM pipelines p
      LEFT JOIN companies c ON p.facility_operator = c.company_num
      WHERE ${where}
    `).get(params).c;

    const rows = db.prepare(`
      SELECT p.segment_num, p.segment_length,
             p.origin_name, p.origin_area, p.origin_block,
             p.dest_name, p.dest_area, p.dest_block,
             p.product_code, p.status_code, p.pipe_size,
             p.min_water_depth, p.max_water_depth,
             p.approved_date, p.construction_date,
             p.facility_operator, c.company_name AS operator_name
      FROM pipelines p
      LEFT JOIN companies c ON p.facility_operator = c.company_num
      WHERE ${where}
      ORDER BY ${orderBy}
      LIMIT @limit OFFSET @offset
    `).all({ ...params, limit, offset });

    res.json(paginatedResponse(rows, total, { page, limit }));
  }
);

// GET /api/pipelines/map — segments in bounds
router.get('/map', (req, res) => {
  const { bounds } = req.query;
  if (!bounds) return res.json({ data: [] });

  const [south, west, north, east] = bounds.split(',').map(Number);

  // Find segments that have at least one point in the bounds
  const rows = db.prepare(`
    SELECT DISTINCT p.segment_num, p.product_code, p.status_code,
           p.origin_name, p.dest_name, p.pipe_size
    FROM pipeline_locations pl
    JOIN pipelines p ON pl.segment_num = p.segment_num
    WHERE pl.latitude BETWEEN @south AND @north
      AND pl.longitude BETWEEN @west AND @east
    LIMIT 500
  `).all({ south, west, north, east });

  res.json({ data: rows });
});

// GET /api/pipelines/:id — detail
router.get('/:id', (req, res) => {
  const pipeline = db.prepare(`
    SELECT p.*, c.company_name AS operator_name
    FROM pipelines p
    LEFT JOIN companies c ON p.facility_operator = c.company_num
    WHERE p.segment_num = @id
  `).get({ id: req.params.id });

  if (!pipeline) return res.status(404).json({ error: 'Pipeline not found' });
  res.json(pipeline);
});

// GET /api/pipelines/:id/geometry — route coordinates
router.get('/:id/geometry', (req, res) => {
  const points = db.prepare(`
    SELECT latitude AS lat, longitude AS lng
    FROM pipeline_locations
    WHERE segment_num = @id
    ORDER BY point_seq
  `).all({ id: req.params.id });

  res.json({ data: points });
});

// POST /api/pipelines/geometry-batch — batch geometry for multiple segments
router.post('/geometry-batch', express.json(), (req, res) => {
  const { segment_nums } = req.body;
  if (!Array.isArray(segment_nums) || segment_nums.length === 0) {
    return res.status(400).json({ error: 'segment_nums array required' });
  }

  const limited = segment_nums.slice(0, 100);
  const placeholders = limited.map(() => '?').join(',');

  const points = db.prepare(`
    SELECT segment_num, latitude AS lat, longitude AS lng, point_seq
    FROM pipeline_locations
    WHERE segment_num IN (${placeholders})
    ORDER BY segment_num, point_seq
  `).all(...limited);

  // Group by segment
  const grouped = {};
  for (const p of points) {
    if (!grouped[p.segment_num]) grouped[p.segment_num] = [];
    grouped[p.segment_num].push({ lat: p.lat, lng: p.lng });
  }

  res.json({ data: grouped });
});

module.exports = router;
