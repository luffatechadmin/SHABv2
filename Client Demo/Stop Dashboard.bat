@echo off
setlocal
echo Stopping SHAB Attendance Dashboard...
taskkill /IM WL10Middleware.exe /F >nul 2>nul
echo Done.
endlocal

