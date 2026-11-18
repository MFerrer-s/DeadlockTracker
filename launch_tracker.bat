@echo off
cd /d "%~dp0"
where py >nul 2>nul && (py -3 deadlock_tracker.py) || (python deadlock_tracker.py)
pause
