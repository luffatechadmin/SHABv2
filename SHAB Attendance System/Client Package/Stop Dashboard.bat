@echo off
setlocal EnableExtensions
echo Stopping SHAB Attendance Dashboard...
taskkill /IM SHABMiddleware.exe /F >nul 2>nul
taskkill /IM WL10Middleware.exe /F >nul 2>nul
if errorlevel 1 (
  echo NOTE: Middleware is not running or permission denied.
  endlocal & exit /b 0
)
echo OK: Stopped.
endlocal & exit /b 0
