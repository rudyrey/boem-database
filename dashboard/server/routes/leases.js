const express = require('express');
const { db } = require('../db');
const { pagination, paginatedResponse } = require('../middleware/pagination');
const router = express.Router();

const ALLOWED_SORTS = [
  'lease_number', 'area_code', 'block_number', 'lease_status',
  'royalty_rate', 'bid_amount', 'min_water_depth', 'max_water_depth',
  'effective_date', 'first_production_date', 'designated_operator',
];

// GET /api/leases — paginated list
router.get('/',
  pagination(ALLOWED_SORTS),
  (req, res) => {
    const { page, limit, offset, sort, order } = req.pagination;
    const { lease_status, area_code, block_number, min_water_depth, max_water_depth, search } = req.query;

    let where = '1=1';
    const params = {};

    if (lease_status) { where += ' AND l.lease_status = @lease_status'; params.lease_status = lease_status; }
    if (area_code) { where += ' AND l.area_code = @area_code'; params.area_code = area_code; }
    if (block_number) { where += ' AND l.block_number = @block_number'; params.block_number = block_number; }
    if (min_water_depth) { where += ' AND l.min_water_depth >= @min_wd'; params.min_wd = parseInt(min_water_depth); }
    if (max_water_depth) { where += ' AND l.max_water_depth <= @max_wd'; params.max_wd = parseInt(max_water_depth); }
    if (search) {
      where += ` AND (l.lease_number LIKE @search
                  OR ll.designated_operator LIKE @search
                  OR (l.area_code || ' ' || l.block_number) LIKE @search)`;
      params.search = `%${search}%`;
    }

    const orderBy = sort ? `${sort} ${order}` : 'l.lease_number ASC';

    const total = db.prepare(`
      SELECT COUNT(*) as c
      FROM leases l
      LEFT JOIN lease_list ll ON l.lease_number = ll.lease_number
      WHERE ${where}
    `).get(params).c;

    const rows = db.prepare(`
      SELECT l.lease_number, l.area_code, l.block_number,
             l.lease_status, l.effective_date, l.expiration_date,
             l.royalty_rate, l.bid_amount, l.bid_per_unit,
             l.min_water_depth, l.max_water_depth,
             l.first_production_date, l.num_platforms,
             l.planning_area_code, l.district_code,
             ll.designated_operator, ll.mineral_type
      FROM leases l
      LEFT JOIN lease_list ll ON l.lease_number = ll.lease_number
      WHERE ${where}
      ORDER BY ${orderBy}
      LIMIT @limit OFFSET @offset
    `).all({ ...params, limit, offset });

    res.json(paginatedResponse(rows, total, { page, limit }));
  }
);

// GET /api/leases/:id — single lease detail
router.get('/:id', (req, res) => {
  const lease = db.prepare(`
    SELECT l.*, ll.designated_operator, ll.mineral_type
    FROM leases l
    LEFT JOIN lease_list ll ON l.lease_number = ll.lease_number
    WHERE l.lease_number = @id
  `).get({ id: req.params.id });

  if (!lease) return res.status(404).json({ error: 'Lease not found' });
  res.json(lease);
});

// GET /api/leases/:id/owners
router.get('/:id/owners', (req, res) => {
  const rows = db.prepare(`
    SELECT lo.company_num, c.company_name, lo.assignment_pct,
           lo.assignment_approval, lo.assignment_effective,
           lo.assignment_status, lo.designated_operator
    FROM lease_owners lo
    LEFT JOIN companies c ON lo.company_num = c.company_num
    WHERE lo.lease_number = @id
    ORDER BY lo.assignment_pct DESC
  `).all({ id: req.params.id });

  res.json({ data: rows });
});

// GET /api/leases/:id/production
router.get('/:id/production', (req, res) => {
  const rows = db.prepare(`
    SELECT production_date,
           SUM(oil_volume) AS oil,
           SUM(gas_volume) AS gas,
           SUM(water_volume) AS water,
           COUNT(DISTINCT api_well_number) AS well_count
    FROM production
    WHERE lease_number = @id
    GROUP BY production_date
    ORDER BY production_date
  `).all({ id: req.params.id });

  res.json({ data: rows });
});

// GET /api/leases/:id/wells
router.get('/:id/wells', (req, res) => {
  const rows = db.prepare(`
    SELECT w.api_well_number, w.well_name, w.status_code, w.type_code,
           w.spud_date, w.total_measured_depth, w.water_depth,
           c.company_name AS operator_name
    FROM wells w
    LEFT JOIN companies c ON w.operator_num = c.company_num
    WHERE w.bottom_lease_number = @id
    ORDER BY w.spud_date DESC
  `).all({ id: req.params.id });

  res.json({ data: rows });
});

// GET /api/leases/:id/platforms
router.get('/:id/platforms', (req, res) => {
  const rows = db.prepare(`
    SELECT p.complex_id, p.area_code, p.block_number,
           p.water_depth, p.oil_producing, p.gas_producing,
           c.company_name AS operator_name,
           ps.structure_name, ps.structure_type, ps.install_date
    FROM platforms p
    LEFT JOIN companies c ON p.company_num = c.company_num
    LEFT JOIN platform_structures ps ON p.complex_id = ps.complex_id
      AND ps.structure_number = (SELECT MIN(s2.structure_number) FROM platform_structures s2 WHERE s2.complex_id = p.complex_id)
    WHERE p.lease_number = @id
  `).all({ id: req.params.id });

  res.json({ data: rows });
});

module.exports = router;
