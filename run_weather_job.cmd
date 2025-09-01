@echo off
setlocal

REM Kör alltid från mappen där scriptet ligger
cd /d "%~dp0"

REM Säkerställ att loggmappen finns
if not exist "logs" mkdir "logs"

REM Kör ditt venvs Python direkt (ingen aktivering behövs) och logga allt
"%~dp0venv\Scripts\python.exe" "%~dp0main.py" >> "%~dp0logs\task.log" 2>&1

endlocal
