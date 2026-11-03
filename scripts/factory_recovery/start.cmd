@echo off
cd /d "%~dp0"
py -3 recover.py %*
pause
