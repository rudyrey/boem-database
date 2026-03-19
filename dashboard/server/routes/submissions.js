const express = require('express');
const { db } = require('../db');
const { pagination, paginatedResponse } = require('../middleware/pagination');
const router = express.Router();

const ALLOWED_SORTS = [
  'sn', 'type', 'api_well_number', 'well_name', 'operator',
  'status_date', 'area_block', 'operation', 'water_depth',
];

// GET /api/submissions — paginated, combined APD + APM list
router.get('/',
  pagination(ALLOWED_SORTS),
  (req, res) => {
    const { page, limit, offset, sort, order } = req.pagination;
    const { search, submission_type, operation, area_block,
            date_from, date_to } = req.query;

    let where = '1=1';
    const params = {};

    if (submission_type === 'APD') {
      where += " AND type = 'APD'";
    } else if (submission_type === 'APM') {
      where += " AND type = 'APM'";
    }

    if (operation) {
      where += ' AND operation LIKE @operation';
      params.operation = `%${operation}%`;
    }
    if (area_block) {
      where += ' AND area_block LIKE @area_block';
      params.area_block = `%${area_block}%`;
    }
    if (date_from) {
      where += ' AND status_date >= @date_from';
      params.date_from = date_from;
    }
    if (date_to) {
      where += ' AND status_date <= @date_to';
      params.date_to = date_to;
    }
    if (search) {
      where += ` AND (api_well_number LIKE @search
                  OR well_name LIKE @search
                  OR operator LIKE @search
                  OR sn LIKE @search)`;
      params.search = `%${search}%`;
    }

    // Map friendly sort names to actual columns
    const orderBy = sort ? `${sort} ${order}` : 'status_date DESC';

    // Combined query via UNION ALL
    const baseQuery = `
      SELECT * FROM (
        SELECT
          sn_apd AS sn,
          'APD' AS type,
          api_well_number,
          well_name,
          bus_asc_name AS operator,
          permit_type AS operation,
          apd_status_dt AS status_date,
          req_spud_date AS secondary_date,
          water_depth,
          rig_name AS rig,
          surf_area_code || ' ' || surf_block_number AS area_block,
          well_type_code
        FROM apd

        UNION ALL

        SELECT
          sn_apm AS sn,
          'APM' AS type,
          api_well_number,
          well_name,
          bus_asc_name AS operator,
          apm_op_cd AS operation,
          acc_status_date AS status_date,
          work_commences_date AS secondary_date,
          water_depth,
          NULL AS rig,
          surf_area_code || ' ' || surf_block_num AS area_block,
          well_type_code
        FROM apm
      ) combined
      WHERE ${where}
    `;

    try {
      const total = db.prepare(
        `SELECT COUNT(*) as c FROM (${baseQuery})`
      ).get(params).c;

      const rows = db.prepare(`
        ${baseQuery}
        ORDER BY ${orderBy}
        LIMIT @limit OFFSET @offset
      `).all({ ...params, limit, offset });

      res.json(paginatedResponse(rows, total, { page, limit }));
    } catch (err) {
      // Tables may not exist
      res.json(paginatedResponse([], 0, { page, limit }));
    }
  }
);

// GET /api/submissions/apd/:sn — full APD detail
router.get('/apd/:sn', (req, res) => {
  try {
    const row = db.prepare(`
      SELECT a.*, c.company_name AS operator_name
      FROM apd a
      LEFT JOIN companies c ON a.operator_num = c.company_num
      WHERE a.sn_apd = @sn
    `).get({ sn: req.params.sn });
    if (!row) return res.status(404).json({ error: 'APD not found' });
    res.json(row);
  } catch (err) {
    res.status(500).json({ error: 'Table not available' });
  }
});

// GET /api/submissions/apd/:sn/casing — casing intervals + sections for one APD
router.get('/apd/:sn/casing', (req, res) => {
  try {
    const intervals = db.prepare(`
      SELECT * FROM apd_casing_intervals
      WHERE sn_apd_fk = @sn
      ORDER BY csng_intv_num
    `).all({ sn: req.params.sn });

    // For each interval, attach its casing sections
    for (const intv of intervals) {
      intv.sections = db.prepare(`
        SELECT * FROM apd_casing_sections
        WHERE sn_apd_csng_intv_fk = @fk
        ORDER BY casing_section_num
      `).all({ fk: intv.sn_apd_csg_intv });
    }

    res.json({ data: intervals });
  } catch (err) {
    res.json({ data: [] });
  }
});

// GET /api/submissions/apd/:sn/geologic — geologic markers for one APD
router.get('/apd/:sn/geologic', (req, res) => {
  try {
    const rows = db.prepare(`
      SELECT * FROM apd_geologic
      WHERE sn_apd = @sn
      ORDER BY top_md
    `).all({ sn: req.params.sn });
    res.json({ data: rows });
  } catch (err) {
    res.json({ data: [] });
  }
});

// GET /api/submissions/apm/:sn — full APM detail
router.get('/apm/:sn', (req, res) => {
  try {
    const row = db.prepare(`
      SELECT a.*, c.company_name AS operator_name
      FROM apm a
      LEFT JOIN companies c ON a.operator_num = c.company_num
      WHERE a.sn_apm = @sn
    `).get({ sn: req.params.sn });
    if (!row) return res.status(404).json({ error: 'APM not found' });
    res.json(row);
  } catch (err) {
    res.status(500).json({ error: 'Table not available' });
  }
});

// GET /api/submissions/apm/:sn/preventers — BOP/preventer data for one APM
router.get('/apm/:sn/preventers', (req, res) => {
  try {
    const rows = db.prepare(`
      SELECT * FROM apm_preventers
      WHERE sn_apm_fk = @sn
    `).all({ sn: req.params.sn });
    res.json({ data: rows });
  } catch (err) {
    res.json({ data: [] });
  }
});

// GET /api/submissions/apm/:sn/suboperations — sub-operations for one APM
router.get('/apm/:sn/suboperations', (req, res) => {
  try {
    const rows = db.prepare(`
      SELECT * FROM apm_suboperations
      WHERE sn_apm_fk = @sn
    `).all({ sn: req.params.sn });
    res.json({ data: rows });
  } catch (err) {
    res.json({ data: [] });
  }
});

module.exports = router;
