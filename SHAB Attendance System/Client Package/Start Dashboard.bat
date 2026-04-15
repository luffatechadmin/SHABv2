@echo off
setlocal
cd /d "%~dp0App\win-x86"
echo Launching SHAB Attendance Dashboard...
start "" "%cd%\WL10Middleware.exe" --dashboard --dashboard-port 5099
echo.
echo If a browser does not open automatically, open http://127.0.0.1:5099/ manually.
echo Default login: superadmin / abcd1234
endlocal

