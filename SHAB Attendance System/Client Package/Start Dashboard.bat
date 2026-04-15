@echo off
setlocal EnableExtensions
if /I "%~1" NEQ "__interactive" (
  echo %cmdcmdline% | find /i "/c" >nul 2>&1
  if not errorlevel 1 (
    start "" cmd.exe /k ""%~f0" __interactive"
    exit /b
  )
)

set "ROOT=%~dp0"
set "APP_DIR=%ROOT%App\win-x86"
set "SDK_INSTALL=%ROOT%ZKTecoSDK\x86\Auto-install_sdk.bat"
set "DASH_URL=http://127.0.0.1:5099/login"
set "SHORTCUT_NAME=SHAB Attendance Dashboard.lnk"
set "SHORTCUT_ICON=%ROOT%Assets\SHAB Attendance Dashboard.ico"
set "LOG_DIR=%ROOT%Logs"
set "LOG_FILE=%LOG_DIR%\attendance-middleware.log"
set "WIN_DIR=%SystemRoot%"
if not defined WIN_DIR set "WIN_DIR=%windir%"
set "PS_EXE=%WIN_DIR%\System32\WindowsPowerShell\v1.0\powershell.exe"

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

echo.
echo Checking if dashboard is already running...
call :CHECK_PORT
if "%errorlevel%"=="0" (
  set "READY=1"
  goto :OPEN_BROWSER
)

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
  "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command ^
    "Start-Process -FilePath 'cmd.exe' -ArgumentList @('/c','call ""%SDK_INSTALL%""') -Verb RunAs -Wait"
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
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%" >nul 2>&1
if exist "%LOG_FILE%" del /f /q "%LOG_FILE%" >nul 2>&1
start "SHAB Attendance Middleware" /min cmd.exe /c ^
  "\"%CD%\WL10Middleware.exe\" --dashboard --dashboard-port 5099 1>>\"%LOG_FILE%\" 2>>&1"

echo Waiting for dashboard to be ready...
set "READY="
call :WAIT_FOR_PORT 60
if "%errorlevel%"=="0" set "READY=1"

:OPEN_BROWSER
echo.
if defined READY (
  echo Opening browser: %DASH_URL%
  start "" "%DASH_URL%"
  echo Creating desktop shortcut...
  call :CREATE_SHORTCUT
) else (
  echo Dashboard did not become reachable on port 5099.
  echo If it started successfully, open %DASH_URL% manually.
  echo.
  if exist "%LOG_FILE%" (
    echo Log file:
    echo   %LOG_FILE%
    echo.
    start "" "%LOG_FILE%"
  )
  echo.
  pause
)

echo.
echo Default login: superadmin / abcd1234
endlocal
exit /b 0

:CHECK_PORT
if not exist "%PS_EXE%" exit /b 2
"%PS_EXE%" -NoProfile -Command "$c=New-Object Net.Sockets.TcpClient; try{$c.Connect('127.0.0.1',5099); $c.Close(); exit 0}catch{exit 1}" >nul 2>&1
exit /b %errorlevel%

:WAIT_FOR_PORT
set "MAX=%~1"
if "%MAX%"=="" set "MAX=60"
for /L %%i in (1,1,%MAX%) do (
  call :CHECK_PORT
  if "%errorlevel%"=="0" exit /b 0
  timeout /t 1 /nobreak >nul
)
exit /b 1

:CREATE_SHORTCUT
set "DESKTOP_DIR=%USERPROFILE%\Desktop"
if not exist "%DESKTOP_DIR%" exit /b 0

set "CMD_SHORTCUT=%DESKTOP_DIR%\SHAB Attendance Dashboard.cmd"
> "%CMD_SHORTCUT%" echo @echo off
>> "%CMD_SHORTCUT%" echo start "" "%ROOT%Start Dashboard.bat" __interactive

set "URL_SHORTCUT=%DESKTOP_DIR%\SHAB Attendance Dashboard.url"
> "%URL_SHORTCUT%" echo [InternetShortcut]
>> "%URL_SHORTCUT%" echo URL=%DASH_URL%
>> "%URL_SHORTCUT%" echo IconFile=%SHORTCUT_ICON%
>> "%URL_SHORTCUT%" echo IconIndex=0
exit /b 0
