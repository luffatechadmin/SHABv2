@echo off
setlocal EnableExtensions
if /I "%~1" NEQ "__interactive" start "" "%ComSpec%" /k ""%~f0" __interactive" & exit /b

set "ROOT=%~dp0"
set "APP_DIR=%ROOT%App\win-x86"
set "SDK_INSTALL=%ROOT%ZKTecoSDK\x86\Auto-install_sdk.bat"
set "DASH_URL=http://127.0.0.1:5099/login"
set "SHORTCUT_ICON=%ROOT%Assets\SHAB Attendance Dashboard.ico"
set "LOG_DIR=%ROOT%Logs"
set "LOG_FILE=%LOG_DIR%\attendance-middleware.log"
set "WIN_DIR=%SystemRoot%"
if not defined WIN_DIR set "WIN_DIR=%windir%"
set "PS_EXE=%WIN_DIR%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "ZK_TARGET=%WIN_DIR%\SysWOW64"
if not exist "%ZK_TARGET%\regsvr32.exe" set "ZK_TARGET=%WIN_DIR%\System32"
set "DOTNET_SHARED1=C:\Program Files (x86)\dotnet\shared"
set "DOTNET_SHARED2=C:\Program Files\dotnet\shared"

echo.
echo ============================================================
echo SHAB Attendance System - Start Dashboard
echo ============================================================

if not exist "%APP_DIR%\WL10Middleware.exe" echo ERROR: WL10Middleware.exe not found in: & echo   %APP_DIR% & echo. & pause & exit /b 1

cd /d "%APP_DIR%"

echo.
echo Checking if dashboard is already running...
call :CHECK_PORT
if not errorlevel 1 set "READY=1" & goto :OPEN_BROWSER

reg query "HKCR\zkemkeeper.CZKEM" >nul 2>&1
if not errorlevel 1 goto :SDK_OK
call :IS_ADMIN
if not errorlevel 1 goto :SDK_INSTALL
echo Requesting Administrator access for SDK install...
call :ELEVATE_SELF
exit /b

:SDK_INSTALL
echo ZKTeco SDK not detected. Installing now - requires Administrator...
if not exist "%SDK_INSTALL%" echo ERROR: SDK installer not found: & echo   %SDK_INSTALL% & echo. & pause & exit /b 1
call :RUN_AS_ADMIN "%SDK_INSTALL%"
if errorlevel 1 echo ERROR: Administrator request was cancelled or blocked. & echo Please run this as Administrator: & echo   %SDK_INSTALL% & echo. & pause & exit /b 1
reg query "HKCR\zkemkeeper.CZKEM" >nul 2>&1
if not errorlevel 1 goto :SDK_OK
if exist "%ZK_TARGET%\zkemkeeper.dll" echo WARNING: ZKTeco SDK dll is present but COM registration was not detected. & echo Device functions may not work until the SDK is registered. & goto :SDK_OK
echo ERROR: ZKTeco SDK install may have failed or was cancelled. & echo Please run this as Administrator: & echo   %SDK_INSTALL% & echo. & pause & exit /b 1

:SDK_OK
echo ZKTeco SDK detected.

call :CHECK_DOTNET
if errorlevel 1 (
  echo.
  echo ERROR: Required .NET runtimes are missing.
  echo This app requires:
  echo - .NET 8 Runtime for Windows x86
  echo - ASP.NET Core 8 Runtime for Windows x86
  echo.
  echo Download .NET 8 here and install the Windows x86 runtimes:
  echo https://dotnet.microsoft.com/en-us/download/dotnet/8.0
  call :OPEN_URL "https://dotnet.microsoft.com/en-us/download/dotnet/8.0"
  echo.
  pause
  endlocal
  exit /b 1
)

echo.
echo Launching SHAB Attendance Dashboard...
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%" >nul 2>&1
if exist "%LOG_FILE%" del /f /q "%LOG_FILE%" >nul 2>&1
start "SHAB Attendance Middleware" /min "%ComSpec%" /c "\"%CD%\WL10Middleware.exe\" --dashboard --dashboard-port 5099 1>>\"%LOG_FILE%\" 2>>&1"

timeout /t 2 /nobreak >nul
tasklist /fi "imagename eq WL10Middleware.exe" | find /i "WL10Middleware.exe" >nul 2>&1
if errorlevel 1 goto :START_FAILED

echo Waiting for dashboard to be ready...
set /a tries=60
:WAIT_LOOP
call :CHECK_PORT
if not errorlevel 1 set "READY=1" & goto :OPEN_BROWSER
set /a tries-=1
if %tries% LEQ 0 goto :NOT_READY
timeout /t 1 /nobreak >nul
goto :WAIT_LOOP

:OPEN_BROWSER
echo.
if defined READY (
  echo Opening browser: %DASH_URL%
  start "" "%DASH_URL%"
  echo Creating desktop shortcut...
  call :CREATE_SHORTCUT
  echo.
  echo Default login: superadmin / abcd1234
  endlocal
  exit /b 0
)

:NOT_READY
echo Dashboard did not become reachable on port 5099.
echo If it started successfully, open %DASH_URL% manually.
echo.
echo Port status:
netstat -ano | findstr /R /C:":5099 .*LISTENING"
echo.
tasklist /fi "imagename eq WL10Middleware.exe"
echo.
if exist "%LOG_FILE%" echo Log file: & echo   %LOG_FILE% & echo. & start "" notepad.exe "%LOG_FILE%"
if exist "%LOG_FILE%" findstr /i /c:"You must install or update .NET" "%LOG_FILE%" >nul 2>&1 & call :OPEN_URL "https://dotnet.microsoft.com/en-us/download/dotnet/8.0"
echo.
pause
echo.
echo Default login: superadmin / abcd1234
endlocal
exit /b 1

:START_FAILED
echo Middleware process did not start.
echo This is usually caused by missing .NET runtime, antivirus blocking the EXE, or missing permissions.
echo.
if exist "%LOG_FILE%" echo Log file: & echo   %LOG_FILE% & echo. & start "" notepad.exe "%LOG_FILE%"
if exist "%LOG_FILE%" findstr /i /c:"You must install or update .NET" "%LOG_FILE%" >nul 2>&1 & call :OPEN_URL "https://dotnet.microsoft.com/en-us/download/dotnet/8.0"
echo Required runtimes:
echo - .NET 8 Runtime for Windows x86
echo - ASP.NET Core 8 Runtime for Windows x86
echo.
echo Download .NET 8 here:
echo https://dotnet.microsoft.com/en-us/download/dotnet/8.0
call :OPEN_URL "https://dotnet.microsoft.com/en-us/download/dotnet/8.0"
echo.
pause
echo.
echo Default login: superadmin / abcd1234
endlocal
exit /b 1

:CHECK_PORT
netstat -ano | findstr /R /C:":5099 .*LISTENING" >nul 2>&1
if not errorlevel 1 exit /b 0
exit /b 1

:IS_ADMIN
net session >nul 2>&1
if not errorlevel 1 exit /b 0
fltmc >nul 2>&1
if not errorlevel 1 exit /b 0
exit /b 1

:ELEVATE_SELF
if exist "%PS_EXE%" "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -Verb RunAs -WorkingDirectory '%ROOT%' -FilePath '%~f0' -ArgumentList '__interactive'" >nul 2>&1 & exit /b 0
mshta "javascript:var sh=new ActiveXObject('Shell.Application'); sh.ShellExecute('%~f0','__interactive','','runas',1); close();" >nul 2>&1 & exit /b 0
echo ERROR: Could not request Administrator access automatically.
pause
exit /b 1

:RUN_AS_ADMIN
if exist "%PS_EXE%" "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "$p=Start-Process -Verb RunAs -WorkingDirectory '%~dp1' -FilePath '%~1' -Wait -PassThru; exit $p.ExitCode" >nul 2>&1 & exit /b
mshta "javascript:var sh=new ActiveXObject('Shell.Application'); sh.ShellExecute('%~1','','','runas',1); close();" >nul 2>&1 & exit /b 0
exit /b 1

:CHECK_DOTNET
set "NETCORE_OK="
set "ASPNET_OK="
for /f "delims=" %%d in ('dir /b "%DOTNET_SHARED1%\Microsoft.NETCore.App\8.*" 2^>nul') do set "NETCORE_OK=1"
for /f "delims=" %%d in ('dir /b "%DOTNET_SHARED2%\Microsoft.NETCore.App\8.*" 2^>nul') do set "NETCORE_OK=1"
for /f "delims=" %%d in ('dir /b "%DOTNET_SHARED1%\Microsoft.AspNetCore.App\8.*" 2^>nul') do set "ASPNET_OK=1"
for /f "delims=" %%d in ('dir /b "%DOTNET_SHARED2%\Microsoft.AspNetCore.App\8.*" 2^>nul') do set "ASPNET_OK=1"
if defined NETCORE_OK if defined ASPNET_OK exit /b 0
exit /b 1

:OPEN_URL
rundll32 url.dll,FileProtocolHandler "%~1" >nul 2>&1
exit /b 0

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
