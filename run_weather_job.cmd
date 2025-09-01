@echo off
setlocal

REM Kör alltid från mappen där scriptet ligger
cd /d "%~dp0"

REM UTF-8 så å/ä/ö blir rätt i loggar
chcp 65001 >nul
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

REM Se till att mappar finns
if not exist "logs"    mkdir "logs"
if not exist "exports" mkdir "exports"

REM Kör huvudjobbet (ingen aktivering av venv behövs)
"%~dp0venv\Scripts\python.exe" -X utf8 "%~dp0main.py" >> "%~dp0logs\task.log" 2>&1
set "RC=%ERRORLEVEL%"

REM Skapa/uppdatera en rullande 30-dagars daglig-CSV om huvudjobbet lyckades
if "%RC%"=="0" (
  "%~dp0venv\Scripts\python.exe" -X utf8 "%~dp0export_aggregate.py" --days 30 --location Kungsbacka --out "exports\daily_latest_30d.csv" >> "%~dp0logs\task.log" 2>&1
  if not "%ERRORLEVEL%"=="0" set "RC=%ERRORLEVEL%"
)

endlocal & exit /b %RC%
