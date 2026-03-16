const express = require('express');
const { cache } = require('../db');
const router = express.Router();

router.get('/', (req, res) => {
  res.json({
    counts: cache.counts,
    cumulativeProduction: cache.cumulativeProduction,
    annualSummary: cache.annualSummary,
    topProducers: cache.topProducers,
    latestProductionDate: cache.latestProductionDate,
    filterOptions: cache.filterOptions,
  });
});

module.exports = router;
