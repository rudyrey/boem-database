#!/usr/bin/env node
/**
 * Downloads boem.db from GitHub Release if it doesn't exist locally.
 * Used for Railway/cloud deployments where the DB can't be in the repo (963MB).
 */
const fs = require('fs');
const path = require('path');
const https = require('https');

const DB_PATH = path.join(__dirname, '..', 'boem.db');
const RELEASE_URL = process.env.BOEM_DB_URL;

function follow(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'boem-dashboard' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return follow(res.headers.location).then(resolve, reject);
      }
      if (res.statusCode !== 200) {
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      resolve(res);
    }).on('error', reject);
  });
}

async function download(url, dest) {
  console.log(`Downloading boem.db from release...`);
  const res = await follow(url);
  const total = parseInt(res.headers['content-length'], 10) || 0;
  let downloaded = 0;
  const file = fs.createWriteStream(dest);

  return new Promise((resolve, reject) => {
    res.on('data', (chunk) => {
      downloaded += chunk.length;
      file.write(chunk);
      if (total) {
        const pct = ((downloaded / total) * 100).toFixed(1);
        process.stdout.write(`\r  ${pct}% (${(downloaded / 1e6).toFixed(0)}MB / ${(total / 1e6).toFixed(0)}MB)`);
      }
    });
    res.on('end', () => {
      file.end();
      console.log('\n  Download complete.');
      resolve();
    });
    res.on('error', reject);
    file.on('error', reject);
  });
}

async function main() {
  if (fs.existsSync(DB_PATH)) {
    console.log('boem.db already exists, skipping download.');
    return;
  }

  if (!RELEASE_URL) {
    console.error('ERROR: boem.db not found and BOEM_DB_URL environment variable is not set.');
    console.error('Set BOEM_DB_URL to the GitHub Release download URL for boem.db');
    process.exit(1);
  }

  await download(RELEASE_URL, DB_PATH);
}

main().catch((err) => {
  console.error('Failed to set up database:', err.message);
  process.exit(1);
});
