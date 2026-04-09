@echo off
cd /d "%~dp0"
echo Starting Zero-Gap Harvesting Engine...
.venv\Scripts\python.exe backend/manage.py sync_api
echo Sync Complete!
pause
