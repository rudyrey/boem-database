const express = require('express');
const { db } = require('../db');
const router = express.Router();

// GET /api/search?q=term&limit=15
router.get('/', (req, res) => {
  const { q } = req.query;
  if (!q || q.length < 2) return res.json({ wells: [], leases: [], platforms: [], companies: [] });

  const limit = Math.min(20, parseInt(req.query.limit) || 15);
  const perCategory = Math.ceil(limit / 4);
  const pattern = `%${q}%`;

  const wells = db.prepare(`
    SELECT w.api_well_number, w.well_name, w.status_code,
           c.company_name AS operator_name
    FROM wells w
    LEFT JOIN companies c ON w.operator_num = c.company_num
    WHERE w.api_well_number LIKE @q
       OR w.well_name LIKE @q
       OR c.company_name LIKE @q
    LIMIT @limit
  `).all({ q: pattern, limit: perCategory });

  const leases = db.prepare(`
    SELECT l.lease_number, l.area_code, l.block_number,
           l.lease_status, ll.designated_operator
    FROM leases l
    LEFT JOIN lease_list ll ON l.lease_number = ll.lease_number
    WHERE l.lease_number LIKE @q
       OR ll.designated_operator LIKE @q
       OR (l.area_code || ' ' || l.block_number) LIKE @q
    LIMIT @limit
  `).all({ q: pattern, limit: perCategory });

  const platforms = db.prepare(`
    SELECT p.complex_id, p.area_code, p.block_number,
           c.company_name AS operator_name,
           ps.structure_name
    FROM platforms p
    LEFT JOIN companies c ON p.company_num = c.company_num
    LEFT JOIN platform_structures ps ON p.complex_id = ps.complex_id
      AND ps.structure_number = (SELECT MIN(s2.structure_number) FROM platform_structures s2 WHERE s2.complex_id = p.complex_id)
    WHERE p.complex_id LIKE @q
       OR ps.structure_name LIKE @q
       OR c.company_name LIKE @q
    LIMIT @limit
  `).all({ q: pattern, limit: perCategory });

  const companies = db.prepare(`
    SELECT company_num, company_name, city, state_code
    FROM companies
    WHERE company_name LIKE @q OR company_num LIKE @q
    LIMIT @limit
  `).all({ q: pattern, limit: perCategory });

  res.json({ wells, leases, platforms, companies });
});

module.exports = router;
