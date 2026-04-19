@echo off
setlocal EnableExtensions

set "RUN_MODE=background"
if /I "%~1"=="--console" ( set "RUN_MODE=console" & shift )
if /I "%~1"=="--foreground" ( set "RUN_MODE=console" & shift )
if /I "%~1"=="--debug" ( set "RUN_MODE=console" & shift )
set "FORCE_RESTART="
if /I "%~1"=="--restart" ( set "FORCE_RESTART=1" & shift )
if /I "%~1"=="--force" ( set "FORCE_RESTART=1" & shift )
set "OPEN_LINKS="
if /I "%~1"=="--open-links" ( set "OPEN_LINKS=1" & shift )

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
set "DATA_DIR=%ROOT%Data"
set "EXPORT_DIR=%ROOT%Exports"
set "REF_DIR=%ROOT%Reference"
set "ATTLOG_FILE=%REF_DIR%\1_attlog.dat"
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
if not exist "%DATA_DIR%" mkdir "%DATA_DIR%" >nul 2>&1
if not exist "%EXPORT_DIR%" mkdir "%EXPORT_DIR%" >nul 2>&1
if not exist "%REF_DIR%" mkdir "%REF_DIR%" >nul 2>&1
if not exist "%ATTLOG_FILE%" type nul > "%ATTLOG_FILE%" 2>nul
set "DESKTOP_DIR=%USERPROFILE%\Desktop"

call :LOG ============================================================
call :LOG SHAB Attendance System - Start Dashboard
call :LOG Version: %SHAB_START_VERSION%
call :LOG Root: %ROOT%
call :LOG AppDir: %APP_DIR%
call :LOG User: %USERNAME%
call :LOG Machine: %COMPUTERNAME%
call :LOG RunMode: %RUN_MODE%
call :LOG ============================================================
call :LOG_SYSTEM_INFO

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
if defined FORCE_RESTART (
  echo Restart requested. Stopping existing middleware...
  call :LOG Restart requested. Stopping existing middleware...
  taskkill /IM WL10Middleware.exe /F >nul 2>nul
  timeout /t 1 /nobreak >nul
)
call :CHECK_PORT
if not errorlevel 1 set "READY=1" & goto OPEN_BROWSER

call :CHECK_ZKEMKEEPER
if not errorlevel 1 goto SDK_OK
call :IS_ADMIN
if not errorlevel 1 goto SDK_INSTALL
echo Requesting Administrator access for SDK install...
call :LOG Requesting Administrator access for SDK install...
call :ELEVATE_SELF
exit /b

:SDK_INSTALL
echo ZKTeco SDK not detected. Installing now - requires Administrator...
call :LOG ZKTeco SDK not detected. Installing now - requires Administrator...
if not exist "%SDK_INSTALL%" echo ERROR: SDK installer not found: & echo   %SDK_INSTALL% & echo. & pause & exit /b 1
set "SHAB_SDK_INSTALL_SILENT=1"
call :RUN_AS_ADMIN "%SDK_INSTALL%"
set "SHAB_SDK_INSTALL_SILENT="
if errorlevel 1 echo ERROR: Administrator request was cancelled or blocked. & echo Please run this as Administrator: & echo   %SDK_INSTALL% & echo. & pause & exit /b 1
call :CHECK_ZKEMKEEPER
if not errorlevel 1 goto SDK_OK
if exist "%ZK_TARGET%\zkemkeeper.dll" echo WARNING: ZKTeco SDK dll is present but COM registration was not detected or COM could not be loaded. & echo Device functions may not work until the SDK is registered. & goto SDK_OK
echo ERROR: ZKTeco SDK install may have failed or was cancelled. & echo Please run this as Administrator: & echo   %SDK_INSTALL% & echo. & pause & exit /b 1

:SDK_OK
echo ZKTeco SDK detected.
call :LOG ZKTeco SDK detected (or dll present).
echo Preparing app files and security settings...
call :LOG Preparing app files and security settings...
call :PREPARE_SECURITY

REM Force local state/export paths so the package is self-contained (no dependency on other folders)
set "WL10_STATE_PATH=%DATA_DIR%\state.json"
set "WL10_ATTLOG_EXPORT_PATH=%ATTLOG_FILE%"
set "WL10_ATTLOG_FILE_PATH=%ATTLOG_FILE%"
call :LOG WL10_STATE_PATH=%WL10_STATE_PATH%
call :LOG WL10_ATTLOG_EXPORT_PATH=%WL10_ATTLOG_EXPORT_PATH%
call :LOG WL10_ATTLOG_FILE_PATH=%WL10_ATTLOG_FILE_PATH%

if exist "%APP_DIR%\coreclr.dll" goto DOTNET_OK
call :LOG Checking .NET runtimes...
call :CHECK_DOTNET
if errorlevel 1 goto DOTNET_MISSING
:DOTNET_OK
call :LOG Dotnet check OK (or self-contained runtime detected).

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
type nul > "%MIDDLE_OUT%" 2>nul
type nul > "%MIDDLE_ERR%" 2>nul
if not exist "%MIDDLE_OUT%" (
  set "MIDDLE_OUT=%TEMP%\middleware-stdout.log"
  type nul > "%MIDDLE_OUT%" 2>nul
  call :LOG WARNING: Could not create stdout log in Logs folder. Using: %MIDDLE_OUT%
)
if not exist "%MIDDLE_ERR%" (
  set "MIDDLE_ERR=%TEMP%\middleware-stderr.log"
  type nul > "%MIDDLE_ERR%" 2>nul
  call :LOG WARNING: Could not create stderr log in Logs folder. Using: %MIDDLE_ERR%
)
echo Logs:
echo   %LOG_FILE%
echo   %MIDDLE_OUT%
echo   %MIDDLE_ERR%

if /I "%RUN_MODE%"=="console" goto RUN_CONSOLE

call :LOG Starting middleware in background...
start "SHAB Attendance Middleware" /min cmd /c "\"%CD%\WL10Middleware.exe\" --dashboard --dashboard-port 5099 1>\"%MIDDLE_OUT%\" 2>\"%MIDDLE_ERR%\""
goto STARTED_OK

:RUN_CONSOLE
echo.
echo Running middleware in foreground (console mode)...
echo Close this window to stop the dashboard.
call :LOG Running middleware in foreground (console mode)...
WL10Middleware.exe --dashboard --dashboard-port 5099
set "MW_EXIT=%errorlevel%"
call :LOG Middleware exited (console mode). ExitCode=%MW_EXIT%
echo.
echo Middleware exited with code: %MW_EXIT%
echo Check logs:
echo   %MIDDLE_OUT%
echo   %MIDDLE_ERR%
echo   %LOG_FILE%
echo.
pause
endlocal & exit /b %MW_EXIT%

:STARTED_OK
echo Waiting for dashboard to be ready...
call :LOG Waiting for dashboard to be ready (port 5099)...
set /a TRIES_MAX=180
set /a tries=%TRIES_MAX%
set /a tick=0
:WAIT_LOOP
set /a tick+=1
if %tick%==1 call :LOG Waiting... (up to 180 seconds)
if %tick%==5 echo Still waiting... & set /a tick=0
call :CHECK_READY
if not errorlevel 1 set "READY=1" & goto OPEN_BROWSER
call :CHECK_PROC
if errorlevel 1 (
  set /a elapsed=%TRIES_MAX%-%tries%
  if %elapsed% GEQ 10 (
    call :LOG Middleware process not detected after %elapsed% seconds.
    goto START_FAILED
  )
)
set /a tries-=1
if %tries% LEQ 0 goto NOT_READY
timeout /t 1 /nobreak >nul
goto WAIT_LOOP

:CHECK_READY
call :CHECK_PORT
if not errorlevel 1 exit /b 0
if exist "%MIDDLE_OUT%" (
  findstr /i /c:"Dashboard running at" "%MIDDLE_OUT%" >nul 2>&1
  if not errorlevel 1 exit /b 0
)
call :CHECK_HTTP
if not errorlevel 1 exit /b 0
exit /b 1

:CHECK_PROC
tasklist /fi "imagename eq WL10Middleware.exe" | find /i "WL10Middleware.exe" >nul 2>&1
if not errorlevel 1 exit /b 0
exit /b 1

:CHECK_HTTP
if not exist "%PS_EXE%" exit /b 1
"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command ^
  "try { $r=Invoke-WebRequest -Uri 'http://127.0.0.1:5099/login' -UseBasicParsing -TimeoutSec 1; if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) { exit 0 } else { exit 1 } } catch { exit 1 }" >nul 2>&1
if not errorlevel 1 exit /b 0
exit /b 1

:OPEN_BROWSER
echo.
if not defined READY goto NOT_READY
echo Opening browser: %DASH_URL%
call :LOG Dashboard reachable. Opening browser: %DASH_URL%
call :OPEN_URL "%DASH_URL%"
echo.
echo Default login: superadmin / abcd1234
call :LOG Done.
endlocal
exit /b 0

:START_FAILED_EARLY_EXIT
call :LOG Middleware exited immediately after start attempt within 2 seconds.
goto START_FAILED

:START_FAILED_TASKLIST
call :LOG Middleware not found in tasklist after initial start attempt.
goto START_FAILED

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
call :LOG Middleware stdout (tail):
call :TAIL "%MIDDLE_OUT%"
call :LOG Middleware stderr (tail):
call :TAIL "%MIDDLE_ERR%"
if not exist "%MIDDLE_ERR%" goto NOT_READY_AFTER_ERR_OPEN
for %%A in ("%MIDDLE_ERR%") do set "ERR_SIZE=%%~zA"
if not "%ERR_SIZE%"=="0" start "" notepad.exe "%MIDDLE_ERR%"
:NOT_READY_AFTER_ERR_OPEN
for %%A in ("%LOG_FILE%") do set "LOG_SIZE=%%~zA"
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
echo This is usually caused by antivirus blocking the EXE, missing permissions, or a missing dependency.
call :LOG ERROR: Middleware process did not start.
call :LOG Middleware stdout (tail):
call :TAIL "%MIDDLE_OUT%"
call :LOG Middleware stderr (tail):
call :TAIL "%MIDDLE_ERR%"
if not exist "%MIDDLE_ERR%" goto START_FAILED_AFTER_ERR_OPEN
for %%A in ("%MIDDLE_ERR%") do set "ERR_SIZE=%%~zA"
if not "%ERR_SIZE%"=="0" start "" notepad.exe "%MIDDLE_ERR%"
:START_FAILED_AFTER_ERR_OPEN
echo.
echo Desktop path:
echo   %DESKTOP_DIR%
echo.
call :LOG Desktop path: %DESKTOP_DIR%
call :LOG Recent crash events (Application log):
call :LOG_EVENT_ERRORS
if exist "%LOG_FILE%" echo Log file: & echo   %LOG_FILE% & echo. & start "" notepad.exe "%LOG_FILE%"
for %%A in ("%LOG_FILE%") do set "LOG_SIZE=%%~zA"
if "%LOG_SIZE%"=="0" echo NOTE: Log file is empty. Windows Defender or SmartScreen may be blocking WL10Middleware.exe. & echo Try: Right-click WL10Middleware.exe ^> Properties ^> Unblock, then run again.
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
if defined OPEN_LINKS call :OPEN_URL "https://dotnet.microsoft.com/en-us/download/dotnet/8.0"
echo.
pause
endlocal
exit /b 1

:CHECK_PORT
if exist "%PS_EXE%" goto CHECK_PORT_PS
goto CHECK_PORT_NETSTAT

:CHECK_PORT_PS
"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command ^
  "try { $ok=$false; foreach($h in @('127.0.0.1','localhost')){ try { $r = Test-NetConnection -ComputerName $h -Port 5099 -InformationLevel Quiet -WarningAction SilentlyContinue; if ($r) { $ok=$true; break } } catch { } }; if($ok){ exit 0 } else { exit 1 } } catch { exit 1 }" >nul 2>&1
if not errorlevel 1 exit /b 0

:CHECK_PORT_NETSTAT
netstat -ano | findstr /R /C:":5099 .*LISTENING" >nul 2>&1
if not errorlevel 1 exit /b 0
exit /b 1

:CHECK_ZKEMKEEPER
reg query "HKCR\zkemkeeper.CZKEM" /reg:32 >nul 2>&1
if not errorlevel 1 (
  if exist "%WIN_DIR%\SysWOW64\WindowsPowerShell\v1.0\powershell.exe" (
    "%WIN_DIR%\SysWOW64\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -Command "try{New-Object -ComObject zkemkeeper.CZKEM | Out-Null; exit 0}catch{exit 1}" >nul 2>&1
    if not errorlevel 1 exit /b 0
  ) else (
    exit /b 0
  )
)
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

:LOG_SYSTEM_INFO
setlocal EnableExtensions
>>"%LOG_FILE%" echo [%date% %time%] OS_VER: %OS% (ver: %CMDEXTVERSION%)
ver >>"%LOG_FILE%" 2>>&1
if exist "%PS_EXE%" goto LOG_SYSTEM_INFO_PS
goto LOG_SYSTEM_INFO_DONE

:LOG_SYSTEM_INFO_PS
"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command ^
  "try { Write-Output ('PSVersion=' + $PSVersionTable.PSVersion.ToString()); Write-Output ('Is64BitOS=' + [Environment]::Is64BitOperatingSystem); Write-Output ('Is64BitProc=' + [Environment]::Is64BitProcess); Write-Output ('OSVersion=' + [Environment]::OSVersion.VersionString) } catch { }" >>"%LOG_FILE%" 2>>&1

:LOG_SYSTEM_INFO_DONE
endlocal & exit /b 0

:LOG
setlocal EnableExtensions
set "MSG=%*"
>>"%LOG_FILE%" echo [%date% %time%] %MSG%
endlocal & exit /b 0

:HTTP_PROBE
if not exist "%PS_EXE%" exit /b 0
"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command ^
  "try { $r=Invoke-WebRequest -Uri 'http://localhost:5099/login' -UseBasicParsing -TimeoutSec 2; Write-Output ('HTTP ' + [int]$r.StatusCode) } catch { Write-Output ('HTTP_ERROR ' + $_.Exception.GetType().FullName + ' ' + $_.Exception.Message) }" >>"%LOG_FILE%" 2>>&1
exit /b 0

:TAIL
setlocal EnableExtensions
set "F=%~1"
if not exist "%F%" goto TAIL_MISSING
if not exist "%PS_EXE%" goto TAIL_NO_PS
"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command ^
  "try { $p='%F%'; if (Test-Path $p) { $t = Get-Content -LiteralPath $p -Tail 80 -ErrorAction SilentlyContinue; foreach($l in $t){ Write-Output $l } } } catch { }" >>"%LOG_FILE%" 2>>&1
endlocal & exit /b 0

:TAIL_MISSING
>>"%LOG_FILE%" echo (missing) "%F%"
endlocal & exit /b 0

:TAIL_NO_PS
>>"%LOG_FILE%" echo (no powershell) "%F%"
endlocal & exit /b 0

:LOG_EVENT_ERRORS
wevtutil qe Application /c:20 /f:text /rd:true /q:"*[System[(EventID=1000 or EventID=1026) and TimeCreated[timediff(@SystemTime) <= 900000]]]" >>"%LOG_FILE%" 2>>&1
exit /b 0

:OPEN_URL
if exist "%PS_EXE%" (
  "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "Start-Process '%~1'" >nul 2>&1
  exit /b 0
)
start "" "%~1" >nul 2>&1
exit /b 0

:CREATE_SHORTCUT
set "DESKTOP_RAW="
for /f "tokens=2*" %%A in ('reg query "HKCU\Software\Microsoft\Windows\CurrentVersion\Explorer\User Shell Folders" /v Desktop 2^>nul ^| find /i "Desktop"') do set "DESKTOP_RAW=%%B"
if defined DESKTOP_RAW call set "DESKTOP_DIR=%DESKTOP_RAW%"
if not defined DESKTOP_DIR set "DESKTOP_DIR=%USERPROFILE%\Desktop"
if not exist "%DESKTOP_DIR%" exit /b 0
del /f /q "%DESKTOP_DIR%\SHAB Attendance Dashboard.cmd" >nul 2>&1
del /f /q "%DESKTOP_DIR%\SHAB Attendance Dashboard.url" >nul 2>&1
del /f /q "%DESKTOP_DIR%\SHAB Attendance Dashboard (Browser Only).url" >nul 2>&1
del /f /q "%DESKTOP_DIR%\SHAB Attendance Dashboard.lnk" >nul 2>&1

set "LNK_SHORTCUT=%DESKTOP_DIR%\SHAB Attendance Dashboard.lnk"
if exist "%PS_EXE%" goto CREATE_SHORTCUT_PS
goto CREATE_SHORTCUT_FALLBACK

:CREATE_SHORTCUT_PS
"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "$desktop='%DESKTOP_DIR%'; $lnk=Join-Path $desktop 'SHAB Attendance Dashboard.lnk'; $root='%ROOT%'; $target=Join-Path $root 'Start Dashboard.bat'; $w=New-Object -ComObject WScript.Shell; $s=$w.CreateShortcut($lnk); $s.TargetPath=$target; $s.WorkingDirectory=$root; $s.Description='SHAB Attendance Dashboard'; if (Test-Path '%SHORTCUT_ICON%') { $s.IconLocation='%SHORTCUT_ICON%,0' }; $s.Save();" >nul 2>&1

:CREATE_SHORTCUT_FALLBACK

if not exist "%LNK_SHORTCUT%" goto CREATE_SHORTCUT_CMD

echo Shortcut created:
echo   %LNK_SHORTCUT%
call :LOG Shortcut created: %LNK_SHORTCUT%
exit /b 0

:CREATE_SHORTCUT_CMD
set "CMD_SHORTCUT=%DESKTOP_DIR%\SHAB Attendance Dashboard.cmd"
> "%CMD_SHORTCUT%" echo @echo off
>> "%CMD_SHORTCUT%" echo start "" "%ROOT%Start Dashboard.bat"
echo Shortcut created:
echo   %CMD_SHORTCUT%
call :LOG Shortcut created: %CMD_SHORTCUT%
exit /b 0
