#!/usr/bin/env node
/**
 * Downloads boem.db from the latest GitHub Release.
 * Used for Railway/cloud deployments where the DB can't be in the repo (963MB).
 *
 * Set BOEM_DB_REPO to override the repository (default: rudyrey/boem-database).
 * Set BOEM_DB_URL to bypass auto-detection and use an explicit download URL.
 */
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const DB_PATH = path.join(__dirname, '..', 'boem.db');
const REPO = process.env.BOEM_DB_REPO || 'rudyrey/boem-database';

if (fs.existsSync(DB_PATH)) {
  console.log('boem.db already exists, skipping download.');
  process.exit(0);
}

let url = process.env.BOEM_DB_URL;

if (!url) {
  // Resolve latest release download URL via GitHub API
  console.log(`Resolving latest release from ${REPO}...`);
  try {
    const json = execSync(
      `curl -sL "https://api.github.com/repos/${REPO}/releases/latest"`,
      { encoding: 'utf8' }
    );
    const release = JSON.parse(json);
    const asset = release.assets && release.assets.find(a => a.name === 'boem.db');
    if (!asset) {
      console.error(`ERROR: No boem.db asset found in release ${release.tag_name}`);
      process.exit(1);
    }
    url = asset.browser_download_url;
    console.log(`Latest release: ${release.tag_name}`);
  } catch (err) {
    console.error('ERROR: Could not resolve latest release from GitHub.');
    console.error('Set BOEM_DB_URL to a direct download URL as a fallback.');
    process.exit(1);
  }
}

console.log(`Downloading boem.db from ${url}...`);
try {
  execSync(`curl -L -f -o "${DB_PATH}" "${url}"`, { stdio: 'inherit' });
  const size = fs.statSync(DB_PATH).size;
  console.log(`Download complete. Size: ${(size / 1e6).toFixed(0)}MB`);
} catch (err) {
  // Clean up partial download
  if (fs.existsSync(DB_PATH)) fs.unlinkSync(DB_PATH);
  console.error('Failed to download database.');
  process.exit(1);
}
