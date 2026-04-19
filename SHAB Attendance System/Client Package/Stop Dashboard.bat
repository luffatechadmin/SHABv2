@echo off
setlocal EnableExtensions
echo Stopping SHAB Attendance Dashboard...
taskkill /IM WL10Middleware.exe /F >nul 2>nul
if errorlevel 1 (
  echo NOTE: WL10Middleware.exe is not running or permission denied.
  endlocal & exit /b 0
)
echo OK: Stopped.
endlocal & exit /b 0
