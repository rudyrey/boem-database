const express = require('express');
const cors = require('cors');
const path = require('path');
const { errorHandler } = require('./server/middleware/error-handler');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());

// Static files
app.use(express.static(path.join(__dirname, 'public')));

// API routes
app.use('/api/stats', require('./server/routes/stats'));
app.use('/api/wells', require('./server/routes/wells'));
app.use('/api/leases', require('./server/routes/leases'));
app.use('/api/platforms', require('./server/routes/platforms'));
app.use('/api/pipelines', require('./server/routes/pipelines'));
app.use('/api/production', require('./server/routes/production'));
app.use('/api/companies', require('./server/routes/companies'));
app.use('/api/fields', require('./server/routes/fields'));
app.use('/api/submissions', require('./server/routes/submissions'));
app.use('/api/eor', require('./server/routes/eor'));
app.use('/api/war', require('./server/routes/war'));
app.use('/api/search', require('./server/routes/search'));

// Error handler
app.use(errorHandler);

app.listen(PORT, () => {
  console.log(`BOEM Data Dashboard running at http://localhost:${PORT}`);
});
