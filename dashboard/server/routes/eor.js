const express = require('express');
const { db } = require('../db');
const { pagination, paginatedResponse } = require('../middleware/pagination');
const router = express.Router();

const ALLOWED_SORTS = [
  'api_well_number', 'well_name', 'bus_asc_name', 'borehole_stat_cd',
  'borehole_stat_dt', 'botm_area_code', 'operation_cd',
];

// GET /api/eor — paginated list
router.get('/',
  pagination(ALLOWED_SORTS),
  (req, res) => {
    try {
      const { page, limit, offset, sort, order } = req.pagination;
      const { search, area_block, operation, status, date_from, date_to } = req.query;

      let where = '1=1';
      const params = {};

      if (search) {
        where += ` AND (e.api_well_number LIKE @search
                    OR e.well_name LIKE @search
                    OR e.bus_asc_name LIKE @search
                    OR CAST(e.sn_eor AS TEXT) LIKE @search)`;
        params.search = `%${search}%`;
      }
      if (area_block) {
        where += ` AND (e.botm_area_code || ' ' || e.botm_block_number) LIKE @area_block`;
        params.area_block = `%${area_block}%`;
      }
      if (operation) {
        where += ' AND e.operation_cd = @operation';
        params.operation = operation;
      }
      if (status) {
        where += ' AND e.borehole_stat_cd = @status';
        params.status = status;
      }
      if (date_from) {
        where += ' AND e.borehole_stat_dt >= @date_from';
        params.date_from = date_from;
      }
      if (date_to) {
        where += ' AND e.borehole_stat_dt <= @date_to';
        params.date_to = date_to;
      }

      const orderBy = sort ? `e.${sort} ${order}` : 'e.borehole_stat_dt DESC';

      const total = db.prepare(`
        SELECT COUNT(*) as c FROM eor e WHERE ${where}
      `).get(params).c;

      const rows = db.prepare(`
        SELECT e.sn_eor, e.operation_cd, e.api_well_number, e.well_name,
               e.well_nm_st_sfix, e.well_nm_bp_sfix,
               e.company_num, e.bus_asc_name,
               e.botm_area_code, e.botm_block_number, e.botm_lease_number,
               e.surf_area_code, e.surf_block_number, e.surf_lease_number,
               e.borehole_stat_cd, e.borehole_stat_dt,
               e.total_md, e.well_bore_tvd
        FROM eor e
        WHERE ${where}
        ORDER BY ${orderBy}
        LIMIT @limit OFFSET @offset
      `).all({ ...params, limit, offset });

      res.json(paginatedResponse(rows, total, { page, limit }));
    } catch (e) {
      res.json(paginatedResponse([], 0, { page: 1, limit: 25 }));
    }
  }
);

// GET /api/eor/filters — distinct values for dropdowns
router.get('/filters', (req, res) => {
  try {
    const operations = db.prepare(
      `SELECT DISTINCT operation_cd FROM eor WHERE operation_cd IS NOT NULL ORDER BY operation_cd`
    ).all().map(r => r.operation_cd);

    const statuses = db.prepare(
      `SELECT DISTINCT borehole_stat_cd FROM eor WHERE borehole_stat_cd IS NOT NULL ORDER BY borehole_stat_cd`
    ).all().map(r => r.borehole_stat_cd);

    res.json({ operations, statuses });
  } catch (e) {
    res.json({ operations: [], statuses: [] });
  }
});

// GET /api/eor/:sn — full detail
router.get('/:sn', (req, res) => {
  try {
    const row = db.prepare(`
      SELECT e.*, c.company_name
      FROM eor e
      LEFT JOIN companies c ON e.company_num = c.company_num
      WHERE e.sn_eor = @sn
    `).get({ sn: req.params.sn });

    if (!row) return res.status(404).json({ error: 'EOR not found' });
    res.json(row);
  } catch (e) {
    res.status(500).json({ error: 'Server error' });
  }
});

// GET /api/eor/:sn/completions
router.get('/:sn/completions', (req, res) => {
  try {
    const rows = db.prepare(`
      SELECT * FROM eor_completions WHERE sn_eor_fk = @sn
      ORDER BY interval
    `).all({ sn: req.params.sn });
    res.json({ data: rows });
  } catch (e) {
    res.json({ data: [] });
  }
});

// GET /api/eor/:sn/casings
router.get('/:sn/casings', (req, res) => {
  try {
    const rows = db.prepare(`
      SELECT * FROM eor_cut_casings WHERE sn_eor_fk = @sn
      ORDER BY casing_cut_depth
    `).all({ sn: req.params.sn });
    res.json({ data: rows });
  } catch (e) {
    res.json({ data: [] });
  }
});

// GET /api/eor/:sn/geomarkers
router.get('/:sn/geomarkers', (req, res) => {
  try {
    const rows = db.prepare(`
      SELECT * FROM eor_geomarkers WHERE sn_eor_fk = @sn
      ORDER BY top_md
    `).all({ sn: req.params.sn });
    res.json({ data: rows });
  } catch (e) {
    res.json({ data: [] });
  }
});

// GET /api/eor/:sn/hc-intervals
router.get('/:sn/hc-intervals', (req, res) => {
  try {
    const rows = db.prepare(`
      SELECT * FROM eor_hc_intervals WHERE sn_eor_fk = @sn
      ORDER BY top_md
    `).all({ sn: req.params.sn });
    res.json({ data: rows });
  } catch (e) {
    res.json({ data: [] });
  }
});

// GET /api/eor/:sn/perforations
router.get('/:sn/perforations', (req, res) => {
  try {
    // Perforations link through completions
    const rows = db.prepare(`
      SELECT p.* FROM eor_perf_intervals p
      JOIN eor_completions c ON p.sn_eor_well_comp_fk = c.sn_eor_well_comp
      WHERE c.sn_eor_fk = @sn
      ORDER BY p.perf_top_md
    `).all({ sn: req.params.sn });
    res.json({ data: rows });
  } catch (e) {
    res.json({ data: [] });
  }
});

module.exports = router;
