@echo off
setlocal
cd /d "%~dp0"
title AI Detriots Lead Pipeline

rem Starts the web dashboard; it opens in your default browser
rem (Chrome, Edge, Firefox, ...). Keep this window open while you work.
if exist "venv\Scripts\python.exe" (
  "venv\Scripts\python.exe" app.py
) else if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" app.py
) else (
  python app.py
)

pause
