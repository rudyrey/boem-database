#Requires -Version 5.0
<#
.SYNOPSIS
  BOEM CSV Updater for Windows.

.DESCRIPTION
  Downloads the latest prebuilt boem.db from GitHub releases and exports
  every table to CSV files under .\exports\. Intended for environments
  where SQLite .db files aren't practical (e.g., PowerBI on an enterprise
  workstation) — point PowerBI at the .\exports\ folder and refresh.

.PARAMETER ExportOnly
  Skip the download and export from the existing boem.db.

.EXAMPLE
  .\update_csvs.ps1
  Download latest boem.db + export all tables to CSV.

.EXAMPLE
  .\update_csvs.ps1 -ExportOnly
  Export from existing boem.db without re-downloading.
#>
[CmdletBinding()]
param(
  [switch]$ExportOnly
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$DbPath = Join-Path $ScriptDir 'boem.db'
$Repo = 'rudyrey/boem-database'

function Log($msg) {
  Write-Host "[$(Get-Date -Format HH:mm:ss)] $msg"
}

function Assert-Python {
  if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "python not found in PATH. Install Python 3 from python.org or the Microsoft Store, then re-run this script."
  }
}

function Download-LatestDb {
  Log '=========================================='
  Log "Fetching latest release from github.com/$Repo"
  Log '=========================================='

  $release = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest" -Headers @{ 'User-Agent' = 'boem-csv-updater' }
  $asset = $release.assets | Where-Object { $_.name -eq 'boem.db' } | Select-Object -First 1
  if (-not $asset) {
    throw "No boem.db asset found in release $($release.tag_name)"
  }

  $sizeMb = [math]::Round($asset.size / 1MB, 1)
  Log "  Release: $($release.tag_name)"
  Log "  Size:    $sizeMb MB"
  Log ''
  Log 'Downloading boem.db (this may take a few minutes)...'

  Invoke-WebRequest -Uri $asset.browser_download_url -OutFile $DbPath -Headers @{ 'User-Agent' = 'boem-csv-updater' }
  Log "  Saved to: $DbPath"
}

function Export-Csvs {
  Log '=========================================='
  Log 'Exporting all tables to CSV'
  Log '=========================================='
  if (-not (Test-Path $DbPath)) {
    throw "$DbPath not found. Run without -ExportOnly to download it first."
  }
  Push-Location $ScriptDir
  try {
    python export_csv.py
    if ($LASTEXITCODE -ne 0) {
      throw "export_csv.py exited with code $LASTEXITCODE"
    }
  } finally {
    Pop-Location
  }
}

Assert-Python
if (-not $ExportOnly) {
  Download-LatestDb
}
Export-Csvs

Log 'Done!'
