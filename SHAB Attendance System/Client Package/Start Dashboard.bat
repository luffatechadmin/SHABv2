@echo off
setlocal
set "ROOT=%~dp0"
set "APP_DIR=%ROOT%App\win-x86"
set "SDK_INSTALL=%ROOT%ZKTecoSDK\x86\Auto-install_sdk.bat"
set "DASH_URL=http://127.0.0.1:5099/login"

echo.
echo ============================================================
echo SHAB Attendance System - Start Dashboard
echo ============================================================

if not exist "%APP_DIR%\WL10Middleware.exe" (
  echo ERROR: WL10Middleware.exe not found in:
  echo   %APP_DIR%
  echo.
  pause
  exit /b 1
)

cd /d "%APP_DIR%"

reg query "HKCR\zkemkeeper.CZKEM" >nul 2>&1
if errorlevel 1 (
  echo ZKTeco SDK not detected. Installing now (requires Administrator)...
  if not exist "%SDK_INSTALL%" (
    echo ERROR: SDK installer not found:
    echo   %SDK_INSTALL%
    echo.
    pause
    exit /b 1
  )
  powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "Start-Process -FilePath 'cmd.exe' -ArgumentList @('/c','\"%SDK_INSTALL%\"') -Verb RunAs -Wait"
  reg query "HKCR\zkemkeeper.CZKEM" >nul 2>&1
  if errorlevel 1 (
    echo ERROR: ZKTeco SDK install may have failed or was cancelled.
    echo Please run this as Administrator:
    echo   %SDK_INSTALL%
    echo.
    pause
    exit /b 1
  )
) else (
  echo ZKTeco SDK detected.
)

if not exist "C:\Program Files (x86)\dotnet\shared\Microsoft.NETCore.App\" (
  echo.
  echo NOTE: .NET Runtime (x86) not detected. The app may fail to start.
  echo Opening the download page...
  start "" "https://dotnet.microsoft.com/en-us/download/dotnet/8.0"
)
if not exist "C:\Program Files (x86)\dotnet\shared\Microsoft.AspNetCore.App\" (
  echo.
  echo NOTE: ASP.NET Core Runtime (x86) not detected. The dashboard may fail to start.
  echo Opening the download page...
  start "" "https://dotnet.microsoft.com/en-us/download/dotnet/8.0"
)

echo.
echo Launching SHAB Attendance Dashboard...
start "SHAB Attendance Middleware" "%CD%\WL10Middleware.exe" --dashboard --dashboard-port 5099

echo Waiting for dashboard to be ready...
set "READY="
for /L %%i in (1,1,60) do (
  powershell -NoProfile -Command ^
    "$c=New-Object Net.Sockets.TcpClient; try{$c.Connect('127.0.0.1',5099); $c.Close(); exit 0}catch{exit 1}" >nul 2>&1
  if not errorlevel 1 (
    set "READY=1"
    goto :OPEN_BROWSER
  )
  timeout /t 1 /nobreak >nul
)

:OPEN_BROWSER
echo.
if defined READY (
  echo Opening browser: %DASH_URL%
  start "" "%DASH_URL%"
) else (
  echo Dashboard did not become reachable on port 5099.
  echo If it started successfully, open %DASH_URL% manually.
)

echo.
echo Default login: superadmin / abcd1234
endlocal
