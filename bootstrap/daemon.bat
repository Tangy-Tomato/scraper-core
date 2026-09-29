@echo off
setlocal
cd /d "%~dp0.."
:loop
python bootstrap\updater.py
python worker\main_worker.py
if errorlevel 1 python bootstrap\updater.py --record-failure
timeout /t 5 /nobreak >nul
goto loop
