@echo off
setlocal EnableExtensions

set "RUN_MODE=background"
set "FORCE_RESTART="
set "NO_PAUSE="
set "OPEN_LINKS="

:PARSE_ARGS
if "%~1"=="" goto PARSE_ARGS_DONE
if /I "%~1"=="--console" set "RUN_MODE=console"
if /I "%~1"=="--foreground" set "RUN_MODE=console"
if /I "%~1"=="--debug" set "RUN_MODE=console"
if /I "%~1"=="--restart" set "FORCE_RESTART=1"
if /I "%~1"=="--force" set "FORCE_RESTART=1"
if /I "%~1"=="--no-pause" set "NO_PAUSE=1"
if /I "%~1"=="--open-links" set "OPEN_LINKS=1"
shift
goto PARSE_ARGS
:PARSE_ARGS_DONE
if defined FORCE_RESTART set "NO_PAUSE=1"
if /I "%RUN_MODE%"=="console" set "NO_PAUSE=1"

set "SHAB_START_VERSION=2026-05-05"
set "ROOT=%~dp0"
set "APP_DIR=%ROOT%App\win-x86"
set "DASH_URL=http://127.0.0.1:5099/login"
set "LOG_DIR=%ROOT%Logs"
set "DATA_DIR=%ROOT%Data"
set "EXPORT_DIR=%ROOT%Exports"
set "REF_DIR=%ROOT%Reference"
set "ATTLOG_FILE=%REF_DIR%\1_attlog.dat"
set "LOG_FILE=%LOG_DIR%\start-dashboard.log"
set "WIN_DIR=%SystemRoot%"
if not defined WIN_DIR set "WIN_DIR=%windir%"
set "PS_EXE=%WIN_DIR%\System32\WindowsPowerShell\v1.0\powershell.exe"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%" >nul 2>&1
if not exist "%DATA_DIR%" mkdir "%DATA_DIR%" >nul 2>&1
if not exist "%EXPORT_DIR%" mkdir "%EXPORT_DIR%" >nul 2>&1
if not exist "%REF_DIR%" mkdir "%REF_DIR%" >nul 2>&1
if not exist "%ATTLOG_FILE%" type nul > "%ATTLOG_FILE%" 2>nul

call :LOG ============================================================
call :LOG SHAB Attendance System - Start Dashboard
call :LOG Version: %SHAB_START_VERSION%
call :LOG Root: "%ROOT%"
call :LOG AppDir: "%APP_DIR%"
call :LOG User: "%USERNAME%"
call :LOG Machine: "%COMPUTERNAME%"
call :LOG RunMode: %RUN_MODE%
call :LOG ============================================================

echo.
echo ============================================================
echo SHAB Attendance System - Start Dashboard
echo Version: %SHAB_START_VERSION%
echo ============================================================

set "MW_EXE=SHABMiddleware.exe"
if not exist "%APP_DIR%\%MW_EXE%" set "MW_EXE=WL10Middleware.exe"
if not exist "%APP_DIR%\%MW_EXE%" (
  echo ERROR: Middleware EXE not found in:
  echo   %APP_DIR%
  echo.
  call :LOG ERROR: Middleware EXE not found in: %APP_DIR%
  if not defined NO_PAUSE pause
  exit /b 1
)

call :CHECK_DOTNET
if errorlevel 1 goto DOTNET_MISSING

pushd "%APP_DIR%" >nul 2>&1

if defined FORCE_RESTART (
  echo Restart requested. Stopping existing middleware...
  call :LOG Restart requested. Stopping existing middleware...
  taskkill /IM SHABMiddleware.exe /F >nul 2>nul
  taskkill /IM WL10Middleware.exe /F >nul 2>nul
  timeout /t 1 /nobreak >nul
)

call :CHECK_PORT
if not errorlevel 1 (
  echo ERROR: Port 5099 is already in use.
  echo Tip: Run "Stop Dashboard.bat" then retry, or stop the process using port 5099.
  call :LOG ERROR: Port 5099 is already in use.
  netstat -ano | findstr /R /C:":5099 .*LISTENING"
  if not defined NO_PAUSE pause
  popd >nul 2>&1
  exit /b 1
)

set "TS=%RANDOM%%RANDOM%"
set "MIDDLE_OUT=%LOG_DIR%\middleware-stdout-%TS%.log"
set "MIDDLE_ERR=%LOG_DIR%\middleware-stderr-%TS%.log"

echo.
echo Launching SHAB Attendance Dashboard...
call :LOG Launching SHAB Attendance Dashboard...
call :LOG Middleware: %MW_EXE% --dashboard --dashboard-port 5099
call :LOG MiddlewareStdout: %MIDDLE_OUT%
call :LOG MiddlewareStderr: %MIDDLE_ERR%

if exist "%MIDDLE_OUT%" del /f /q "%MIDDLE_OUT%" >nul 2>&1
if exist "%MIDDLE_ERR%" del /f /q "%MIDDLE_ERR%" >nul 2>&1
type nul > "%MIDDLE_OUT%" 2>nul
type nul > "%MIDDLE_ERR%" 2>nul

echo Logs:
echo   %LOG_FILE%
echo   %MIDDLE_OUT%
echo   %MIDDLE_ERR%

if /I "%RUN_MODE%"=="console" goto RUN_CONSOLE

start "SHAB Attendance Middleware" /min "%CD%\%MW_EXE%" --dashboard --dashboard-port 5099 1>"%MIDDLE_OUT%" 2>"%MIDDLE_ERR%"

echo Waiting for dashboard to be ready...
call :WAIT_HTTP
if errorlevel 1 goto NOT_READY

goto OPEN_BROWSER

:RUN_CONSOLE
echo.
echo Running middleware in foreground (console mode)...
echo Close this window to stop the dashboard.
call :LOG Running middleware in foreground (console mode)...
"%CD%\%MW_EXE%" --dashboard --dashboard-port 5099
set "MW_EXIT=%errorlevel%"
call :LOG Middleware exited (console mode). ExitCode=%MW_EXIT%
echo.
echo Middleware exited with code: %MW_EXIT%
echo Check logs:
echo   %MIDDLE_OUT%
echo   %MIDDLE_ERR%
echo   %LOG_FILE%
echo.
if not defined NO_PAUSE pause
popd >nul 2>&1
endlocal & exit /b %MW_EXIT%

:OPEN_BROWSER
echo.
echo Opening browser: %DASH_URL%
call :LOG Dashboard reachable. Opening browser: %DASH_URL%
call :OPEN_URL "%DASH_URL%"
popd >nul 2>&1
endlocal & exit /b 0

:NOT_READY
echo.
echo Dashboard did not become reachable on port 5099.
echo If middleware started, open %DASH_URL% manually.
call :LOG ERROR: Dashboard did not become reachable on port 5099.
echo.
echo Port status:
netstat -ano | findstr /R /C:":5099 .*LISTENING"
echo.
echo Middleware process:
tasklist /fi "imagename eq %MW_EXE%"
echo.
call :LOG Port status:
netstat -ano | findstr /R /C:":5099 .*LISTENING" >>"%LOG_FILE%" 2>>&1
call :LOG Middleware process:
tasklist /fi "imagename eq %MW_EXE%" >>"%LOG_FILE%" 2>>&1
if exist "%MIDDLE_OUT%" start "" notepad.exe "%MIDDLE_OUT%"
if exist "%MIDDLE_ERR%" start "" notepad.exe "%MIDDLE_ERR%"
if exist "%LOG_FILE%" start "" notepad.exe "%LOG_FILE%"
if not defined NO_PAUSE pause
popd >nul 2>&1
endlocal & exit /b 1

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
if not defined NO_PAUSE pause
endlocal & exit /b 1

:CHECK_DOTNET
setlocal EnableExtensions
set "NETCORE_OK="
set "ASPNET_OK="
set "DOTNET_SHARED="
if defined ProgramFiles(x86) set "DOTNET_SHARED=%ProgramFiles(x86)%\dotnet\shared"
if not defined DOTNET_SHARED set "DOTNET_SHARED=%ProgramFiles%\dotnet\shared"

dir /b "%DOTNET_SHARED%\Microsoft.NETCore.App\8.*" >nul 2>&1 && set "NETCORE_OK=1"
dir /b "%DOTNET_SHARED%\Microsoft.AspNetCore.App\8.*" >nul 2>&1 && set "ASPNET_OK=1"

if defined NETCORE_OK if defined ASPNET_OK (endlocal & exit /b 0)
endlocal & exit /b 1

:CHECK_PORT
netstat -ano | findstr /R /C:":5099 .*LISTENING" >nul 2>&1
if not errorlevel 1 exit /b 0
exit /b 1

:WAIT_HTTP
if not exist "%PS_EXE%" exit /b 1
setlocal EnableExtensions
set /a tries=180
set /a tick=0
set /a missing=0
echo Waiting up to %tries% seconds...
:WAIT_HTTP_LOOP
set /a tick+=1
if %tick%==5 echo Still waiting...
if %tick%==10 set /a tick=0

call :CHECK_PROC
if errorlevel 1 (
  set /a missing+=1
  if %missing% GEQ 5 (endlocal & exit /b 1)
) else (
  set /a missing=0
)

call :CHECK_PORT
if not errorlevel 1 (endlocal & exit /b 0)

"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "try { if (Get-Command Invoke-WebRequest -ErrorAction SilentlyContinue) { $r=Invoke-WebRequest -Uri 'http://127.0.0.1:5099/login' -UseBasicParsing -TimeoutSec 1; if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) { exit 0 } else { exit 1 } } else { $c = New-Object System.Net.Sockets.TcpClient; $iar = $c.BeginConnect('127.0.0.1',5099,$null,$null); if(-not $iar.AsyncWaitHandle.WaitOne(500)) { $c.Close(); exit 1 }; $c.EndConnect($iar); $c.Close(); exit 0 } } catch { exit 1 }" >nul 2>&1
if not errorlevel 1 (endlocal & exit /b 0)

set /p "=." <nul
set /a tries-=1
if %tries% LEQ 0 echo.
if %tries% LEQ 0 (endlocal & exit /b 1)
timeout /t 1 /nobreak >nul
goto WAIT_HTTP_LOOP

:CHECK_PROC
tasklist /fi "imagename eq %MW_EXE%" | find /i "%MW_EXE%" >nul 2>&1
if not errorlevel 1 exit /b 0
exit /b 1

:OPEN_URL
if exist "%PS_EXE%" (
  "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "Start-Process '%~1'" >nul 2>&1
  exit /b 0
)
start "" "%~1" >nul 2>&1
exit /b 0

:LOG
setlocal EnableExtensions
set "MSG=%*"
>>"%LOG_FILE%" echo [%date% %time%] %MSG%
endlocal & exit /b 0
