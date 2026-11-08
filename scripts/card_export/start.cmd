@echo off
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe py -3 -m venv .venv
if errorlevel 1 goto failed
.venv\Scripts\python.exe -c "import dissect.extfs, dissect.volume" >nul 2>&1
if errorlevel 1 .venv\Scripts\python.exe -m pip install --no-index --find-links wheels -r requirements.txt
if errorlevel 1 goto failed
.venv\Scripts\python.exe export.py %*
goto end
:failed
echo Setup failed. Install Python 3.10 or newer first.
:end
pause
