@echo off
REM Double-click launcher for update_csvs.ps1.
REM Bypasses the default execution policy so enterprise machines can run
REM the unsigned PowerShell script without elevated permissions.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0update_csvs.ps1" %*
echo.
pause
