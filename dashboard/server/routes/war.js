const express = require('express');
const { db } = require('../db');
const { pagination, paginatedResponse } = require('../middleware/pagination');
const router = express.Router();

const ALLOWED_SORTS = [
  'war_start_dt', 'war_end_dt', 'rig_name', 'api_well_number',
  'bus_asc_name', 'botm_area_code', 'well_activity_cd', 'water_depth',
];

// GET /api/war — paginated list
router.get('/',
  pagination(ALLOWED_SORTS),
  (req, res) => {
    try {
      const { page, limit, offset, sort, order } = req.pagination;
      const { search, area_block, rig, activity, date_from, date_to } = req.query;

      let where = '1=1';
      const params = {};

      if (search) {
        where += ` AND (w.api_well_number LIKE @search
                    OR w.well_name LIKE @search
                    OR w.bus_asc_name LIKE @search
                    OR w.rig_name LIKE @search)`;
        params.search = `%${search}%`;
      }
      if (area_block) {
        where += ` AND (w.botm_area_code || ' ' || w.botm_block_num) LIKE @area_block`;
        params.area_block = `%${area_block}%`;
      }
      if (rig) {
        where += ' AND w.rig_name LIKE @rig';
        params.rig = `%${rig}%`;
      }
      if (activity) {
        where += ' AND w.well_activity_cd = @activity';
        params.activity = activity;
      }
      if (date_from) {
        where += ' AND w.war_start_dt >= @date_from';
        params.date_from = date_from;
      }
      if (date_to) {
        where += ' AND w.war_end_dt <= @date_to';
        params.date_to = date_to;
      }

      const orderBy = sort ? `w.${sort} ${order}` : 'w.war_start_dt DESC';

      const total = db.prepare(`
        SELECT COUNT(*) as c FROM war w WHERE ${where}
      `).get(params).c;

      const rows = db.prepare(`
        SELECT w.sn_war, w.war_start_dt, w.war_end_dt, w.rig_name,
               w.api_well_number, w.well_name, w.bus_asc_name, w.company_num,
               w.botm_area_code, w.botm_block_num, w.botm_lease_num,
               w.water_depth, w.well_activity_cd,
               w.drilling_md, w.drilling_tvd
        FROM war w
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

// GET /api/war/filters
router.get('/filters', (req, res) => {
  try {
    const activities = db.prepare(
      `SELECT DISTINCT well_activity_cd FROM war WHERE well_activity_cd IS NOT NULL ORDER BY well_activity_cd`
    ).all().map(r => r.well_activity_cd);

    res.json({ activities });
  } catch (e) {
    res.json({ activities: [] });
  }
});

// GET /api/war/:sn — full detail
router.get('/:sn', (req, res) => {
  try {
    const row = db.prepare(`
      SELECT w.*, c.company_name
      FROM war w
      LEFT JOIN companies c ON w.company_num = c.company_num
      WHERE w.sn_war = @sn
    `).get({ sn: req.params.sn });

    if (!row) return res.status(404).json({ error: 'WAR not found' });
    res.json(row);
  } catch (e) {
    res.status(500).json({ error: 'Server error' });
  }
});

// GET /api/war/:sn/tubulars
router.get('/:sn/tubulars', (req, res) => {
  try {
    const rows = db.prepare(`
      SELECT * FROM war_tubulars WHERE sn_war_fk = @sn
      ORDER BY csng_setting_top_md
    `).all({ sn: req.params.sn });
    res.json({ data: rows });
  } catch (e) {
    res.json({ data: [] });
  }
});

module.exports = router;
