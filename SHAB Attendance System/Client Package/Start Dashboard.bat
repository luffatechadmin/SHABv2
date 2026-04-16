@echo off
setlocal EnableExtensions

set "SHAB_START_VERSION=2026-04-16"
set "ROOT=%~dp0"
set "APP_DIR=%ROOT%App\win-x86"
set "SDK_INSTALL=%ROOT%ZKTecoSDK\x86\Auto-install_sdk.bat"
set "DASH_URL=http://127.0.0.1:5099/login"
set "SHORTCUT_ICON=%ROOT%Assets\SHAB Attendance Dashboard.ico"
set "LOG_DIR=%ROOT%Logs"
set "LOG_FILE=%LOG_DIR%\attendance-middleware.log"
set "MIDDLE_OUT=%LOG_DIR%\middleware-stdout.log"
set "MIDDLE_ERR=%LOG_DIR%\middleware-stderr.log"
set "WIN_DIR=%SystemRoot%"
if not defined WIN_DIR set "WIN_DIR=%windir%"
set "PS_EXE=%WIN_DIR%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "ZK_TARGET=%WIN_DIR%\SysWOW64"
if not exist "%ZK_TARGET%\regsvr32.exe" set "ZK_TARGET=%WIN_DIR%\System32"
set "DOTNET_SHARED1=C:\Program Files (x86)\dotnet\shared"
set "DOTNET_SHARED2=C:\Program Files\dotnet\shared"
set "DOTNET_X86_EXE=C:\Program Files (x86)\dotnet\dotnet.exe"
set "DOTNET_X64_EXE=C:\Program Files\dotnet\dotnet.exe"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%" >nul 2>&1

call :LOG ============================================================
call :LOG SHAB Attendance System - Start Dashboard
call :LOG Version: %SHAB_START_VERSION%
call :LOG Root: %ROOT%
call :LOG AppDir: %APP_DIR%
call :LOG User: %USERNAME%
call :LOG Machine: %COMPUTERNAME%
call :LOG ============================================================

echo.
echo ============================================================
echo SHAB Attendance System - Start Dashboard
echo Version: %SHAB_START_VERSION%
echo ============================================================

if not exist "%APP_DIR%\WL10Middleware.exe" call :LOG ERROR: WL10Middleware.exe not found in: %APP_DIR% & echo ERROR: WL10Middleware.exe not found in: & echo   %APP_DIR% & echo. & pause & exit /b 1

cd /d "%APP_DIR%"

echo.
echo Checking if dashboard is already running...
call :LOG Checking if dashboard is already running (port 5099)...
call :CHECK_PORT
if not errorlevel 1 set "READY=1" & goto :OPEN_BROWSER

reg query "HKCR\zkemkeeper.CZKEM" >nul 2>&1
if not errorlevel 1 goto :SDK_OK
call :IS_ADMIN
if not errorlevel 1 goto :SDK_INSTALL
echo Requesting Administrator access for SDK install...
call :LOG Requesting Administrator access for SDK install...
call :ELEVATE_SELF
exit /b

:SDK_INSTALL
echo ZKTeco SDK not detected. Installing now - requires Administrator...
call :LOG ZKTeco SDK not detected. Installing now - requires Administrator...
if not exist "%SDK_INSTALL%" echo ERROR: SDK installer not found: & echo   %SDK_INSTALL% & echo. & pause & exit /b 1
call :RUN_AS_ADMIN "%SDK_INSTALL%"
if errorlevel 1 echo ERROR: Administrator request was cancelled or blocked. & echo Please run this as Administrator: & echo   %SDK_INSTALL% & echo. & pause & exit /b 1
reg query "HKCR\zkemkeeper.CZKEM" >nul 2>&1
if not errorlevel 1 goto :SDK_OK
if exist "%ZK_TARGET%\zkemkeeper.dll" echo WARNING: ZKTeco SDK dll is present but COM registration was not detected. & echo Device functions may not work until the SDK is registered. & goto :SDK_OK
echo ERROR: ZKTeco SDK install may have failed or was cancelled. & echo Please run this as Administrator: & echo   %SDK_INSTALL% & echo. & pause & exit /b 1

:SDK_OK
echo ZKTeco SDK detected.
call :LOG ZKTeco SDK detected (or dll present).
echo Preparing app files and security settings...
call :LOG Preparing app files and security settings...
call :PREPARE_SECURITY

call :LOG Checking .NET runtimes...
call :CHECK_DOTNET
if errorlevel 1 goto :DOTNET_MISSING
call :LOG .NET runtimes detected.

echo Creating desktop shortcuts...
call :LOG Creating desktop shortcuts...
call :CREATE_SHORTCUT

echo.
echo Launching SHAB Attendance Dashboard...
call :LOG Launching SHAB Attendance Dashboard...
call :LOG Middleware: WL10Middleware.exe --dashboard --dashboard-port 5099
call :LOG LogFile: %LOG_FILE%
call :LOG MiddlewareStdout: %MIDDLE_OUT%
call :LOG MiddlewareStderr: %MIDDLE_ERR%

if exist "%MIDDLE_OUT%" del /f /q "%MIDDLE_OUT%" >nul 2>&1
if exist "%MIDDLE_ERR%" del /f /q "%MIDDLE_ERR%" >nul 2>&1

if exist "%PS_EXE%" (
  "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command ^
    "$exe=Join-Path (Get-Location) 'WL10Middleware.exe';" ^
    "$out='%MIDDLE_OUT%';" ^
    "$err='%MIDDLE_ERR%';" ^
    "$p=Start-Process -FilePath $exe -ArgumentList @('--dashboard','--dashboard-port','5099') -RedirectStandardOutput $out -RedirectStandardError $err -PassThru;" ^
    "Write-Output ('PID=' + $p.Id);" ^
    "Start-Sleep -Seconds 2;" ^
    "if ($p.HasExited) { Write-Output ('EXITED=' + $p.ExitCode); exit 100 } else { exit 0 }" >>"%LOG_FILE%" 2>>&1
  if not errorlevel 1 goto :STARTED_OK
  call :LOG Middleware exited immediately after start attempt. See middleware stderr/stdout.
) else (
  start "SHAB Attendance Middleware" /min ""%CD%\WL10Middleware.exe" --dashboard --dashboard-port 5099
)

timeout /t 2 /nobreak >nul
tasklist /fi "imagename eq WL10Middleware.exe" | find /i "WL10Middleware.exe" >nul 2>&1
if errorlevel 1 (
  call :LOG Middleware not found in tasklist after initial start attempt.
  goto :START_FAILED
)

:STARTED_OK
echo Waiting for dashboard to be ready...
call :LOG Waiting for dashboard to be ready (port 5099)...
set /a tries=120
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
  call :LOG Dashboard reachable. Opening browser: %DASH_URL%
  call :OPEN_URL "%DASH_URL%"
  echo.
  echo Default login: superadmin / abcd1234
  call :LOG Done.
  endlocal
  exit /b 0
)

:NOT_READY
echo Dashboard did not become reachable on port 5099.
echo If it started successfully, open %DASH_URL% manually.
call :LOG ERROR: Dashboard did not become reachable on port 5099.
echo.
echo Port status:
netstat -ano | findstr /R /C:":5099 .*LISTENING"
echo.
tasklist /fi "imagename eq WL10Middleware.exe"
echo.
echo Desktop path:
echo   %DESKTOP_DIR%
echo.
call :LOG Port status:
netstat -ano | findstr /R /C:":5099 .*LISTENING" >>"%LOG_FILE%" 2>>&1
call :LOG Tasklist for WL10Middleware.exe:
tasklist /fi "imagename eq WL10Middleware.exe" >>"%LOG_FILE%" 2>>&1
call :LOG Desktop path: %DESKTOP_DIR%
call :LOG HTTP probe:
call :HTTP_PROBE
call :LOG Recent crash events (Application log):
call :LOG_EVENT_ERRORS
if exist "%LOG_FILE%" echo Log file: & echo   %LOG_FILE% & echo. & start "" notepad.exe "%LOG_FILE%"
if exist "%LOG_FILE%" findstr /i /c:"You must install or update .NET" "%LOG_FILE%" >nul 2>&1 & call :OPEN_URL "https://dotnet.microsoft.com/en-us/download/dotnet/8.0"
for %%A in ("%LOG_FILE%") do set "LOG_SIZE=%%~zA"
if "%LOG_SIZE%"=="0" echo NOTE: Log file is empty. Windows Defender or SmartScreen may be blocking WL10Middleware.exe. & echo Try: Right-click WL10Middleware.exe ^> Properties ^> Unblock, then run again.
echo.
pause
echo.
echo Default login: superadmin / abcd1234
endlocal
exit /b 1

:START_FAILED
echo Middleware process did not start.
echo This is usually caused by missing .NET runtime, antivirus blocking the EXE, or missing permissions.
call :LOG ERROR: Middleware process did not start.
echo.
echo Desktop path:
echo   %DESKTOP_DIR%
echo.
call :LOG Desktop path: %DESKTOP_DIR%
call :LOG Recent crash events (Application log):
call :LOG_EVENT_ERRORS
if exist "%LOG_FILE%" echo Log file: & echo   %LOG_FILE% & echo. & start "" notepad.exe "%LOG_FILE%"
if exist "%LOG_FILE%" findstr /i /c:"You must install or update .NET" "%LOG_FILE%" >nul 2>&1 & call :OPEN_URL "https://dotnet.microsoft.com/en-us/download/dotnet/8.0"
for %%A in ("%LOG_FILE%") do set "LOG_SIZE=%%~zA"
if "%LOG_SIZE%"=="0" echo NOTE: Log file is empty. Windows Defender or SmartScreen may be blocking WL10Middleware.exe. & echo Try: Right-click WL10Middleware.exe ^> Properties ^> Unblock, then run again.
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

:DOTNET_MISSING
echo.
echo ERROR: Required .NET runtimes are missing for this app.
echo This app requires Windows x86 runtimes:
echo - .NET 8 Runtime for Windows x86
echo - ASP.NET Core 8 Runtime for Windows x86
call :LOG ERROR: Required .NET runtimes are missing for this app.
echo.
echo Download .NET 8 here and install the Windows x86 runtimes:
echo https://dotnet.microsoft.com/en-us/download/dotnet/8.0
call :OPEN_URL "https://dotnet.microsoft.com/en-us/download/dotnet/8.0"
echo.
pause
endlocal
exit /b 1

:CHECK_PORT
if exist "%PS_EXE%" (
  "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command ^
    "$c=New-Object Net.Sockets.TcpClient; try{$c.Connect('127.0.0.1',5099); $c.Close(); exit 0}catch{exit 1}" >nul 2>&1
  if not errorlevel 1 exit /b 0
)
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

:PREPARE_SECURITY
if not exist "%PS_EXE%" exit /b 0
"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command ^
  "$app='%APP_DIR%';" ^
  "Write-Output ('Security prep app path: ' + $app);" ^
  "try { $files = Get-ChildItem -Path $app -Recurse -File -Include *.exe,*.dll,*.bat -ErrorAction SilentlyContinue; foreach($f in $files){ Unblock-File -Path $f.FullName -ErrorAction SilentlyContinue }; Write-Output ('Unblocked files: ' + ($files | Measure-Object).Count) } catch { Write-Output ('Unblock failed: ' + $_.Exception.Message) };" ^
  "try { $isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator); Write-Output ('IsAdmin: ' + $isAdmin); if ($isAdmin) { Add-MpPreference -ExclusionPath $app -ErrorAction SilentlyContinue | Out-Null; Write-Output 'Defender exclusion attempted.' } else { Write-Output 'Defender exclusion skipped (not admin).' } } catch { Write-Output ('Defender exclusion failed: ' + $_.Exception.Message) };" ^
  "exit 0" >>"%LOG_FILE%" 2>>&1
exit /b 0

:CHECK_DOTNET
set "NETCORE_OK="
set "ASPNET_OK="

if exist "C:\Program Files (x86)\" (
  if not exist "%DOTNET_X86_EXE%" exit /b 1
  for /f "delims=" %%d in ('dir /b "%DOTNET_SHARED1%\Microsoft.NETCore.App\8.*" 2^>nul') do set "NETCORE_OK=1"
  for /f "delims=" %%d in ('dir /b "%DOTNET_SHARED1%\Microsoft.AspNetCore.App\8.*" 2^>nul') do set "ASPNET_OK=1"
) else (
  if exist "%DOTNET_SHARED2%\Microsoft.NETCore.App\8.*" set "NETCORE_OK=1"
  if exist "%DOTNET_SHARED2%\Microsoft.AspNetCore.App\8.*" set "ASPNET_OK=1"
)

if defined NETCORE_OK if defined ASPNET_OK exit /b 0
exit /b 1

:LOG
setlocal EnableExtensions
set "MSG=%*"
>>"%LOG_FILE%" echo [%date% %time%] %MSG%
endlocal & exit /b 0

:HTTP_PROBE
if not exist "%PS_EXE%" exit /b 0
"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command ^
  "try { $r=Invoke-WebRequest -Uri 'http://127.0.0.1:5099/login' -UseBasicParsing -TimeoutSec 2; Write-Output ('HTTP ' + [int]$r.StatusCode) } catch { Write-Output ('HTTP_ERROR ' + $_.Exception.GetType().FullName + ' ' + $_.Exception.Message) }" >>"%LOG_FILE%" 2>>&1
exit /b 0

:LOG_EVENT_ERRORS
wevtutil qe Application /c:20 /f:text /rd:true /q:"*[System[(EventID=1000 or EventID=1026) and TimeCreated[timediff(@SystemTime) <= 900000]]]" >>"%LOG_FILE%" 2>>&1
exit /b 0

:OPEN_URL
if exist "%PS_EXE%" "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "Start-Process '%~1'" >nul 2>&1
start "" "%~1" >nul 2>&1
rundll32 url.dll,FileProtocolHandler "%~1" >nul 2>&1
exit /b 0

:CREATE_SHORTCUT
set "DESKTOP_RAW="
for /f "tokens=2*" %%A in ('reg query "HKCU\Software\Microsoft\Windows\CurrentVersion\Explorer\User Shell Folders" /v Desktop 2^>nul ^| find /i "Desktop"') do set "DESKTOP_RAW=%%B"
if defined DESKTOP_RAW call set "DESKTOP_DIR=%DESKTOP_RAW%"
if not defined DESKTOP_DIR set "DESKTOP_DIR=%USERPROFILE%\Desktop"
if not exist "%DESKTOP_DIR%" exit /b 0
set "CMD_SHORTCUT=%DESKTOP_DIR%\SHAB Attendance Dashboard.cmd"
> "%CMD_SHORTCUT%" echo @echo off
>> "%CMD_SHORTCUT%" echo start "" "%ROOT%Start Dashboard.bat" __interactive

set "URL_SHORTCUT=%DESKTOP_DIR%\SHAB Attendance Dashboard.url"
> "%URL_SHORTCUT%" echo [InternetShortcut]
>> "%URL_SHORTCUT%" echo URL=%DASH_URL%
>> "%URL_SHORTCUT%" echo IconFile=%SHORTCUT_ICON%
>> "%URL_SHORTCUT%" echo IconIndex=0
echo Shortcut created:
echo   %CMD_SHORTCUT%
echo Shortcut created:
echo   %URL_SHORTCUT%
call :LOG Shortcut created: %CMD_SHORTCUT%
call :LOG Shortcut created: %URL_SHORTCUT%
exit /b 0
