const express = require('express');
const { db } = require('../db');
const { pagination, paginatedResponse } = require('../middleware/pagination');
const router = express.Router();

const ALLOWED_SORTS = [
  'rig_name', 'report_count', 'well_count', 'first_seen', 'last_seen',
];

// GET /api/rig-timeline — paginated list of distinct rigs with stats
router.get('/',
  pagination(ALLOWED_SORTS),
  (req, res) => {
    try {
      const { page, limit, offset, sort, order } = req.pagination;
      const { search, active } = req.query;

      let where = "rig_name IS NOT NULL AND rig_name != 'N/A'";
      const params = {};

      if (search) {
        where += ' AND rig_name LIKE @search';
        params.search = `%${search}%`;
      }

      let havingClause = '';
      if (active === 'true') {
        havingClause = "HAVING MAX(war_end_dt) >= date('now', '-90 days')";
      }

      const orderBy = sort ? `${sort} ${order}` : 'last_seen DESC';

      const countSql = `
        SELECT COUNT(*) as c FROM (
          SELECT rig_name FROM war WHERE ${where}
          GROUP BY rig_name ${havingClause}
        )
      `;
      const total = db.prepare(countSql).get(params).c;

      const rows = db.prepare(`
        SELECT rig_name,
               COUNT(*) as report_count,
               COUNT(DISTINCT api_well_number) as well_count,
               MIN(war_start_dt) as first_seen,
               MAX(war_end_dt) as last_seen,
               GROUP_CONCAT(DISTINCT botm_area_code) as areas
        FROM war
        WHERE ${where}
        GROUP BY rig_name
        ${havingClause}
        ORDER BY ${orderBy}
        LIMIT @limit OFFSET @offset
      `).all({ ...params, limit, offset });

      res.json(paginatedResponse(rows, total, { page, limit }));
    } catch (e) {
      res.json(paginatedResponse([], 0, { page: 1, limit: 25 }));
    }
  }
);

// GET /api/rig-timeline/gantt — jobs for multiple rigs (Gantt chart data)
router.get('/gantt', (req, res) => {
  try {
    const { search, hide_generic, date_from, date_to } = req.query;
    const rigLimit = Math.min(parseInt(req.query.rig_limit) || 500, 5000);
    const rigOffset = Math.max(parseInt(req.query.rig_offset) || 0, 0);

    let where = "rig_name IS NOT NULL AND rig_name != 'N/A'";
    const params = {};

    if (hide_generic === 'true') {
      where += " AND rig_name NOT LIKE '*%'";
    }
    if (search) {
      where += ' AND rig_name LIKE @search';
      params.search = `%${search}%`;
    }

    // Count total matching rigs
    const totalRigs = db.prepare(`
      SELECT COUNT(*) as c FROM (
        SELECT rig_name FROM war WHERE ${where} GROUP BY rig_name
      )
    `).get(params).c;

    // Get top N rigs by most recent activity
    const rigRows = db.prepare(`
      SELECT rig_name, MAX(war_end_dt) as last_seen
      FROM war WHERE ${where}
      GROUP BY rig_name
      ORDER BY last_seen DESC
      LIMIT @rigLimit OFFSET @rigOffset
    `).all({ ...params, rigLimit, rigOffset });

    const rigNames = rigRows.map(r => r.rig_name);

    if (rigNames.length === 0) {
      return res.json({ rigs: [], dateRange: {}, totalRigs: 0, rigLimit, rigOffset });
    }

    // Build IN clause with named params
    const rigParams = {};
    const rigPlaceholders = rigNames.map((name, i) => {
      rigParams[`rig${i}`] = name;
      return `@rig${i}`;
    }).join(',');

    // Date filter
    let jobDateFilter = '';
    const jobParams = { ...rigParams };
    if (date_from) {
      jobDateFilter += ' AND war_end_dt >= @dateFrom';
      jobParams.dateFrom = date_from;
    }
    if (date_to) {
      jobDateFilter += ' AND war_start_dt <= @dateTo';
      jobParams.dateTo = date_to;
    }

    // Get all jobs using consecutive grouping by base well (first 10 chars of API)
    // The last 2 digits are sidetrack/completion codes — same physical well location
    const jobs = db.prepare(`
      WITH ordered AS (
        SELECT *, SUBSTR(api_well_number, 1, 10) as base_api,
               ROW_NUMBER() OVER (PARTITION BY rig_name ORDER BY war_start_dt, sn_war) as rn,
               LAG(SUBSTR(api_well_number, 1, 10)) OVER (PARTITION BY rig_name ORDER BY war_start_dt, sn_war) as prev_well
        FROM war
        WHERE rig_name IN (${rigPlaceholders}) ${jobDateFilter}
      ),
      grouped AS (
        SELECT *,
               SUM(CASE WHEN base_api != prev_well OR prev_well IS NULL THEN 1 ELSE 0 END)
                 OVER (PARTITION BY rig_name ORDER BY rn) as grp
        FROM ordered
      )
      SELECT rig_name,
             MAX(api_well_number) as api_well_number,
             GROUP_CONCAT(DISTINCT api_well_number) as all_apis,
             MAX(well_name) as well_name,
             MIN(war_start_dt) as start_dt,
             MAX(war_end_dt) as end_dt,
             GROUP_CONCAT(DISTINCT well_activity_cd) as activities,
             GROUP_CONCAT(well_activity_cd || ':' || war_start_dt || ':' || war_end_dt, '|') as activity_timeline,
             MAX(botm_area_code) as area_code,
             MAX(botm_block_num) as block_num,
             MAX(botm_lease_num) as lease_num,
             MAX(water_depth) as water_depth,
             MAX(drilling_md) as max_md,
             MAX(bus_asc_name) as operator,
             MAX(company_num) as company_num,
             COUNT(*) as report_count
      FROM grouped
      GROUP BY rig_name, grp
      ORDER BY rig_name, MIN(war_start_dt) ASC
    `).all(jobParams);

    // Batch enrich with APD/APM/EOR counts using all API numbers (including sidetracks)
    const apis = [...new Set(jobs.flatMap(j => j.all_apis ? j.all_apis.split(',') : [j.api_well_number]).filter(Boolean))];
    const enrichMap = {};
    if (apis.length > 0) {
      const ph = apis.map(() => '?').join(',');
      try {
        for (const r of db.prepare(`SELECT SUBSTR(api_well_number, 1, 10) as base_api, COUNT(*) as c FROM apd WHERE api_well_number IN (${ph}) GROUP BY base_api`).all(...apis))
          enrichMap[r.base_api] = { ...enrichMap[r.base_api], apd: (enrichMap[r.base_api]?.apd || 0) + r.c };
      } catch (e) { /* table may not exist */ }
      try {
        for (const r of db.prepare(`SELECT SUBSTR(api_well_number, 1, 10) as base_api, COUNT(*) as c FROM apm WHERE api_well_number IN (${ph}) GROUP BY base_api`).all(...apis))
          enrichMap[r.base_api] = { ...enrichMap[r.base_api], apm: (enrichMap[r.base_api]?.apm || 0) + r.c };
      } catch (e) { /* table may not exist */ }
      try {
        for (const r of db.prepare(`SELECT SUBSTR(api_well_number, 1, 10) as base_api, COUNT(*) as c, GROUP_CONCAT(sn_eor) as sns FROM eor WHERE api_well_number IN (${ph}) GROUP BY base_api`).all(...apis)) {
          const prev = enrichMap[r.base_api] || {};
          enrichMap[r.base_api] = {
            ...prev,
            eor: (prev.eor || 0) + r.c,
            eorSns: [...(prev.eorSns || []), ...(r.sns ? String(r.sns).split(',') : [])],
          };
        }
      } catch (e) { /* table may not exist */ }
    }

    // Merge same-base-well bars that are close in time (interleaved returns)
    const mergedJobs = [];
    const byRig = {};
    for (const job of jobs) {
      if (!byRig[job.rig_name]) byRig[job.rig_name] = [];
      byRig[job.rig_name].push(job);
    }
    for (const rigName of Object.keys(byRig)) {
      const rigJobs = byRig[rigName];

      // Pass 1: same-base-well merge (within 30 days)
      const wellLast = {};
      const pass1 = [];
      for (const job of rigJobs) {
        const baseApi = job.api_well_number ? job.api_well_number.slice(0, 10) : '';
        const prev = wellLast[baseApi];
        if (prev) {
          const gap = (new Date(job.start_dt) - new Date(prev.end_dt)) / 86400000;
          if (gap <= 30) {
            prev.end_dt = job.end_dt > prev.end_dt ? job.end_dt : prev.end_dt;
            prev.report_count += job.report_count;
            const acts = job.activities ? job.activities.split(',') : [];
            const prevActs = prev._raw_activities || (prev.activities ? prev.activities.split(',') : []);
            prev._raw_activities = [...new Set([...prevActs, ...acts])];
            prev.activities = prev._raw_activities.join(',');
            if (job.max_md && (!prev.max_md || job.max_md > prev.max_md)) prev.max_md = job.max_md;
            if (job.all_apis) {
              const prevApis = prev.all_apis ? prev.all_apis.split(',') : [];
              prev.all_apis = [...new Set([...prevApis, ...job.all_apis.split(',')])].join(',');
            }
            continue;
          }
        }
        pass1.push(job);
        wellLast[baseApi] = job;
      }

      // Pass 2: absorb minor bars that are fully contained within a larger bar
      // (sporadic monitoring reports for nearby wells while rig works on primary well)
      pass1.sort((a, b) => a.start_dt.localeCompare(b.start_dt) || b.report_count - a.report_count);
      const absorbed = new Set();
      for (let i = 0; i < pass1.length; i++) {
        if (absorbed.has(i)) continue;
        const major = pass1[i];
        const majorStart = new Date(major.start_dt).getTime();
        const majorEnd = new Date(major.end_dt).getTime();
        const majorDays = (majorEnd - majorStart) / 86400000;
        for (let j = 0; j < pass1.length; j++) {
          if (i === j || absorbed.has(j)) continue;
          const minor = pass1[j];
          const minorStart = new Date(minor.start_dt).getTime();
          const minorEnd = new Date(minor.end_dt).getTime();
          const minorDays = (minorEnd - minorStart) / 86400000;
          // Minor must be fully contained within major and have far fewer reports
          if (minorStart >= majorStart && minorEnd <= majorEnd && minor.report_count <= major.report_count * 0.35) {
            absorbed.add(j);
            major.report_count += minor.report_count;
          }
        }
      }
      for (let i = 0; i < pass1.length; i++) {
        if (!absorbed.has(i)) mergedJobs.push(pass1[i]);
      }
    }

    // Group by rig and process
    const rigMap = {};
    for (const r of rigRows) rigMap[r.rig_name] = { rig_name: r.rig_name, jobs: [] };

    let minDate = null, maxDate = null;
    for (const job of mergedJobs) {
      const baseApi = job.api_well_number ? job.api_well_number.slice(0, 10) : '';
      const e = enrichMap[baseApi] || {};
      job.apd_count = e.apd || 0;
      job.apm_count = e.apm || 0;
      job.eor_count = e.eor || 0;
      job.eor_sns = e.eorSns || [];
      job.activities = job.activities ? job.activities.split(',').filter(Boolean) : [];
      // Parse activity_timeline into segments for colored bar rendering
      if (job.activity_timeline) {
        const entries = job.activity_timeline.split('|').map(e => {
          const [act, start, end] = e.split(':');
          return { act, start, end };
        }).filter(e => e.act && e.start && e.end);
        // GROUP_CONCAT does not guarantee row order — sort chronologically
        // before merging, or segments can come out interleaved/misordered
        entries.sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0));
        // Merge consecutive entries with the same activity into segments
        const segments = [];
        for (const entry of entries) {
          const last = segments.length > 0 ? segments[segments.length - 1] : null;
          if (last && last.activity === entry.act) {
            if (entry.end > last.end) last.end = entry.end;
          } else {
            segments.push({ activity: entry.act, start: entry.start, end: entry.end });
          }
        }
        job.segments = segments;
      } else {
        job.segments = [];
      }
      delete job.activity_timeline;
      delete job._raw_activities;
      job.area_code = job.area_code || null;
      job.block_num = job.block_num || null;
      if (job.start_dt && (!minDate || job.start_dt < minDate)) minDate = job.start_dt;
      if (job.end_dt && (!maxDate || job.end_dt > maxDate)) maxDate = job.end_dt;
      if (rigMap[job.rig_name]) rigMap[job.rig_name].jobs.push(job);
    }

    res.json({
      rigs: rigRows.map(r => rigMap[r.rig_name]),
      dateRange: { min: minDate, max: maxDate },
      totalRigs,
      rigLimit,
      rigOffset,
    });
  } catch (e) {
    console.error('Gantt error:', e);
    res.status(500).json({ error: 'Server error' });
  }
});

// GET /api/rig-timeline/wars — individual WAR records for a rig+well+date range
router.get('/wars', (req, res) => {
  try {
    const { rig, api, start, end } = req.query;
    if (!rig) return res.json({ data: [] });

    let where = 'rig_name = @rig';
    const params = { rig };

    if (api) { where += ' AND api_well_number = @api'; params.api = api; }
    if (start) { where += ' AND war_end_dt >= @start'; params.start = start; }
    if (end) { where += ' AND war_start_dt <= @end'; params.end = end; }

    const rows = db.prepare(`
      SELECT sn_war, war_start_dt, war_end_dt, rig_name,
             api_well_number, well_name, well_activity_cd,
             well_actv_start_dt, well_actv_end_dt,
             bus_asc_name, company_num,
             water_depth, drilling_md, drilling_tvd, drill_fluid_wgt,
             rkb_elevation, bop_test_date, ram_tst_prss, annular_tst_prss,
             surf_area_code, surf_block_num, surf_lease_num,
             botm_area_code, botm_block_num, botm_lease_num,
             total_depth_date, contact_name
      FROM war
      WHERE ${where}
      ORDER BY war_start_dt ASC, sn_war ASC
      LIMIT 500
    `).all(params);

    res.json({ data: rows });
  } catch (e) {
    console.error('WAR detail error:', e);
    res.json({ data: [] });
  }
});

// GET /api/rig-timeline/:rig — timeline for one rig (WAR records grouped by well)
router.get('/:rig', (req, res) => {
  try {
    const rigName = req.params.rig;
    const jobLimit = Math.min(parseInt(req.query.limit) || 200, 500);
    const jobOffset = Math.max(parseInt(req.query.offset) || 0, 0);

    // Get rig summary
    const summary = db.prepare(`
      SELECT rig_name,
             COUNT(*) as report_count,
             COUNT(DISTINCT api_well_number) as well_count,
             MIN(war_start_dt) as first_seen,
             MAX(war_end_dt) as last_seen
      FROM war WHERE rig_name = @rig
    `).get({ rig: rigName });

    // Get jobs using consecutive grouping by base well (first 10 chars of API)
    const jobs = db.prepare(`
      WITH ordered AS (
        SELECT *, SUBSTR(api_well_number, 1, 10) as base_api,
               ROW_NUMBER() OVER (ORDER BY war_start_dt, sn_war) as rn,
               LAG(SUBSTR(api_well_number, 1, 10)) OVER (ORDER BY war_start_dt, sn_war) as prev_well
        FROM war
        WHERE rig_name = @rig
      ),
      grouped AS (
        SELECT *,
               SUM(CASE WHEN base_api != prev_well OR prev_well IS NULL THEN 1 ELSE 0 END)
                 OVER (ORDER BY rn) as grp
        FROM ordered
      )
      SELECT MAX(api_well_number) as api_well_number,
             GROUP_CONCAT(DISTINCT api_well_number) as all_apis,
             MAX(well_name) as well_name,
             MIN(war_start_dt) as start_dt,
             MAX(war_end_dt) as end_dt,
             GROUP_CONCAT(DISTINCT well_activity_cd) as activities,
             GROUP_CONCAT(DISTINCT botm_area_code) as area_code,
             GROUP_CONCAT(DISTINCT botm_block_num) as block_num,
             MAX(botm_lease_num) as lease_num,
             MAX(water_depth) as water_depth,
             MAX(drilling_md) as max_md,
             MAX(bus_asc_name) as operator,
             MAX(company_num) as company_num,
             COUNT(*) as report_count
      FROM grouped
      GROUP BY grp
      ORDER BY MAX(war_end_dt) DESC
      LIMIT @limit OFFSET @offset
    `).all({ rig: rigName, limit: jobLimit, offset: jobOffset });

    // Batch-enrich with APD/APM/EOR counts using base API
    const apis = [...new Set(jobs.flatMap(j => j.all_apis ? j.all_apis.split(',') : [j.api_well_number]).filter(Boolean))];
    const enrichMap = {};
    if (apis.length > 0) {
      const placeholders = apis.map(() => '?').join(',');
      try {
        const apdRows = db.prepare(`SELECT SUBSTR(api_well_number, 1, 10) as base_api, COUNT(*) as c FROM apd WHERE api_well_number IN (${placeholders}) GROUP BY base_api`).all(...apis);
        for (const r of apdRows) enrichMap[r.base_api] = { ...enrichMap[r.base_api], apd: (enrichMap[r.base_api]?.apd || 0) + r.c };
      } catch (e) {}
      try {
        const apmRows = db.prepare(`SELECT SUBSTR(api_well_number, 1, 10) as base_api, COUNT(*) as c FROM apm WHERE api_well_number IN (${placeholders}) GROUP BY base_api`).all(...apis);
        for (const r of apmRows) enrichMap[r.base_api] = { ...enrichMap[r.base_api], apm: (enrichMap[r.base_api]?.apm || 0) + r.c };
      } catch (e) {}
      try {
        const eorRows = db.prepare(`SELECT SUBSTR(api_well_number, 1, 10) as base_api, COUNT(*) as c FROM eor WHERE api_well_number IN (${placeholders}) GROUP BY base_api`).all(...apis);
        for (const r of eorRows) enrichMap[r.base_api] = { ...enrichMap[r.base_api], eor: (enrichMap[r.base_api]?.eor || 0) + r.c };
      } catch (e) {}
    }

    for (const job of jobs) {
      const baseApi = job.api_well_number ? job.api_well_number.slice(0, 10) : '';
      const e = enrichMap[baseApi] || {};
      job.apd_count = e.apd || 0;
      job.apm_count = e.apm || 0;
      job.eor_count = e.eor || 0;
      job.activities = job.activities ? job.activities.split(',').filter(Boolean) : [];
      job.area_code = job.area_code ? job.area_code.split(',')[0] : null;
      job.block_num = job.block_num ? job.block_num.split(',')[0] : null;
    }

    let specs = null;
    try {
      specs = db.prepare('SELECT * FROM rigs WHERE rig_name = @rig').get({ rig: rigName });
    } catch (e) {}

    const totalJobs = summary ? summary.well_count : 0;
    res.json({ summary, jobs, specs, totalJobs, jobLimit, jobOffset });
  } catch (e) {
    res.status(500).json({ error: 'Server error' });
  }
});

module.exports = router;
