#!/usr/bin/env node
/**
 * Downloads boem.db from GitHub Release if it doesn't exist locally.
 * Used for Railway/cloud deployments where the DB can't be in the repo (963MB).
 */
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const DB_PATH = path.join(__dirname, '..', 'boem.db');
const RELEASE_URL = process.env.BOEM_DB_URL;

if (fs.existsSync(DB_PATH)) {
  console.log('boem.db already exists, skipping download.');
  process.exit(0);
}

if (!RELEASE_URL) {
  console.error('ERROR: boem.db not found and BOEM_DB_URL environment variable is not set.');
  console.error('Set BOEM_DB_URL to the GitHub Release download URL for boem.db');
  process.exit(1);
}

console.log('Downloading boem.db from release...');
try {
  execSync(`curl -L -f -o "${DB_PATH}" "${RELEASE_URL}"`, { stdio: 'inherit' });
  const size = fs.statSync(DB_PATH).size;
  console.log(`Download complete. Size: ${(size / 1e6).toFixed(0)}MB`);
} catch (err) {
  // Clean up partial download
  if (fs.existsSync(DB_PATH)) fs.unlinkSync(DB_PATH);
  console.error('Failed to download database.');
  process.exit(1);
}
