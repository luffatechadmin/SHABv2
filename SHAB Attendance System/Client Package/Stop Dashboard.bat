@echo off
setlocal EnableExtensions
echo Stopping SHAB Attendance Dashboard...
set "KILLED="
taskkill /IM SHABMiddleware.exe /F >nul 2>nul && set "KILLED=1"
taskkill /IM WL10Middleware.exe /F >nul 2>nul && set "KILLED=1"
if not defined KILLED (
  echo NOTE: Middleware is not running or permission denied.
  endlocal & exit /b 0
)
echo OK: Stopped.
endlocal & exit /b 0
