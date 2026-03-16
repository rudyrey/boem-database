const express = require('express');
const { db } = require('../db');
const { pagination, paginatedResponse } = require('../middleware/pagination');
const router = express.Router();

const ALLOWED_SORTS = ['company_num', 'company_name', 'city', 'state_code', 'start_date'];

// GET /api/companies — paginated list
router.get('/',
  pagination(ALLOWED_SORTS),
  (req, res) => {
    const { page, limit, offset, sort, order } = req.pagination;
    const { search, gom_region } = req.query;

    let where = '1=1';
    const params = {};

    if (search) {
      where += ` AND (company_name LIKE @search OR company_num LIKE @search)`;
      params.search = `%${search}%`;
    }
    if (gom_region) { where += " AND gom_region = 'G'"; }

    const orderBy = sort ? `${sort} ${order}` : 'company_name ASC';

    const total = db.prepare(`SELECT COUNT(*) as c FROM companies WHERE ${where}`).get(params).c;

    const rows = db.prepare(`
      SELECT company_num, company_name, sort_name, start_date, term_date,
             gom_region, city, state_code, zip_code
      FROM companies
      WHERE ${where}
      ORDER BY ${orderBy}
      LIMIT @limit OFFSET @offset
    `).all({ ...params, limit, offset });

    res.json(paginatedResponse(rows, total, { page, limit }));
  }
);

// GET /api/companies/:id — detail with counts
router.get('/:id', (req, res) => {
  const company = db.prepare('SELECT * FROM companies WHERE company_num = @id').get({ id: req.params.id });
  if (!company) return res.status(404).json({ error: 'Company not found' });

  const wellCount = db.prepare('SELECT COUNT(*) as c FROM wells WHERE operator_num = @id').get({ id: req.params.id }).c;
  const platformCount = db.prepare('SELECT COUNT(*) as c FROM platforms WHERE company_num = @id').get({ id: req.params.id }).c;
  const leaseCount = db.prepare('SELECT COUNT(DISTINCT lease_number) as c FROM lease_owners WHERE company_num = @id').get({ id: req.params.id }).c;

  res.json({ ...company, wellCount, platformCount, leaseCount });
});

// GET /api/companies/:id/wells
router.get('/:id/wells',
  pagination(['api_well_number', 'well_name', 'spud_date', 'status_code']),
  (req, res) => {
    const { page, limit, offset, sort, order } = req.pagination;
    const orderBy = sort ? `${sort} ${order}` : 'spud_date DESC';

    const total = db.prepare('SELECT COUNT(*) as c FROM wells WHERE operator_num = @id').get({ id: req.params.id }).c;

    const rows = db.prepare(`
      SELECT api_well_number, well_name, status_code, type_code,
             spud_date, total_measured_depth, water_depth, area_block
      FROM wells
      WHERE operator_num = @id
      ORDER BY ${orderBy}
      LIMIT @limit OFFSET @offset
    `).all({ id: req.params.id, limit, offset });

    res.json(paginatedResponse(rows, total, { page, limit }));
  }
);

// GET /api/companies/:id/leases
router.get('/:id/leases', (req, res) => {
  const rows = db.prepare(`
    SELECT DISTINCT l.lease_number, l.area_code, l.block_number,
           l.lease_status, l.effective_date, lo.assignment_pct
    FROM lease_owners lo
    JOIN leases l ON lo.lease_number = l.lease_number
    WHERE lo.company_num = @id
    ORDER BY l.lease_number
  `).all({ id: req.params.id });

  res.json({ data: rows });
});

// GET /api/companies/:id/platforms
router.get('/:id/platforms', (req, res) => {
  const rows = db.prepare(`
    SELECT p.complex_id, p.area_code, p.block_number,
           p.water_depth, p.oil_producing, p.gas_producing,
           ps.structure_name, ps.structure_type, ps.install_date
    FROM platforms p
    LEFT JOIN platform_structures ps ON p.complex_id = ps.complex_id
      AND ps.structure_number = (SELECT MIN(s2.structure_number) FROM platform_structures s2 WHERE s2.complex_id = p.complex_id)
    WHERE p.company_num = @id
    ORDER BY p.complex_id
  `).all({ id: req.params.id });

  res.json({ data: rows });
});

module.exports = router;
