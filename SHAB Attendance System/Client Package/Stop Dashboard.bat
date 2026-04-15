@echo off
setlocal EnableExtensions
if /I "%~1" NEQ "__interactive" (
  start "" "%ComSpec%" /k ""%~f0" __interactive"
  exit /b
)

echo Stopping SHAB Attendance Dashboard...
taskkill /IM WL10Middleware.exe /F >nul 2>nul
if errorlevel 1 (
  echo NOTE: WL10Middleware.exe is not running or permission denied.
) else (
  echo OK: Stopped.
)
endlocal

