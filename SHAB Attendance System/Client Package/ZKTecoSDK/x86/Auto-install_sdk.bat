@echo off
setlocal EnableExtensions
cd /d "%~dp0"

set "WIN_DIR=%SystemRoot%"
if not defined WIN_DIR set "WIN_DIR=%windir%"
set "PS_EXE=%WIN_DIR%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "LOG_FILE=%TEMP%\shab-zkteco-sdk-install.log"
set "INTERACTIVE=1"
if /I "%SHAB_SDK_INSTALL_SILENT%"=="1" set "INTERACTIVE=0"
set "FORCE_CLEAN=0"
if /I "%SHAB_SDK_FORCE_CLEAN%"=="1" set "FORCE_CLEAN=1"

net session >nul 2>&1
if errorlevel 1 (
  if exist "%PS_EXE%" (
    "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "$p=Start-Process -Verb RunAs -WorkingDirectory '%~dp0' -FilePath $env:ComSpec -ArgumentList @('/c','\"\"%~f0\"\"') -Wait -PassThru; exit $p.ExitCode" >nul 2>&1
    exit /b %errorlevel%
  )
  echo ERROR: Administrator privileges are required to install/register the ZKTeco SDK.
  echo Please right-click and choose "Run as administrator".
  pause
  exit /b 1
)

set "TARGET=%WIN_DIR%\SysWOW64"
if not exist "%TARGET%\regsvr32.exe" set "TARGET=%WIN_DIR%\System32"

> "%LOG_FILE%" echo [%date% %time%] Starting ZKTeco SDK install from: %~dp0
>>"%LOG_FILE%" echo [%date% %time%] Target folder: %TARGET%
>>"%LOG_FILE%" echo [%date% %time%] Copying DLLs...

copy /y ".\sdk\*.dll" "%TARGET%\" >nul
if errorlevel 1 (
  >>"%LOG_FILE%" echo [%date% %time%] ERROR: Copy failed.
  exit /b 1
)

>>"%LOG_FILE%" echo [%date% %time%] Registering COM: %TARGET%\zkemkeeper.dll
"%TARGET%\regsvr32.exe" /s "%TARGET%\zkemkeeper.dll"
if errorlevel 1 (
  >>"%LOG_FILE%" echo [%date% %time%] ERROR: regsvr32 returned non-zero.
  if "%INTERACTIVE%"=="1" echo ERROR: regsvr32 failed. See: %LOG_FILE% & pause
  exit /b 2
)

>>"%LOG_FILE%" echo [%date% %time%] Checking registry (32-bit view): HKCR\zkemkeeper.CZKEM
reg query "HKCR\zkemkeeper.CZKEM" /reg:32 >nul 2>&1
set "HAS_PROGID=%errorlevel%"
>>"%LOG_FILE%" echo [%date% %time%] Checking registry (32-bit view): HKCR\CLSID\{00853A19-BD51-419B-9269-2DABE57EB61F}
reg query "HKCR\CLSID\{00853A19-BD51-419B-9269-2DABE57EB61F}" /reg:32 >nul 2>&1
set "HAS_CLSID=%errorlevel%"

if "%HAS_PROGID%"=="0" goto :REG_OK
if "%HAS_CLSID%"=="0" goto :REG_OK

>>"%LOG_FILE%" echo [%date% %time%] ERROR: Neither ProgID nor CLSID was registered.
>>"%LOG_FILE%" echo [%date% %time%] Running non-silent regsvr32 to show the exact error...
"%TARGET%\regsvr32.exe" "%TARGET%\zkemkeeper.dll"
>>"%LOG_FILE%" echo [%date% %time%] regsvr32 (non-silent) exit code: %errorlevel%
reg query "HKCR\zkemkeeper.CZKEM" /reg:32 >>"%LOG_FILE%" 2>>&1
reg query "HKCR\CLSID\{00853A19-BD51-419B-9269-2DABE57EB61F}" /reg:32 >>"%LOG_FILE%" 2>>&1

if "%INTERACTIVE%"=="1" echo ERROR: COM registration failed. Open log: %LOG_FILE% & pause
exit /b 3

:REG_OK

set "PROGID_CLSID="
for /f "tokens=2*" %%A in ('reg query "HKCR\zkemkeeper.CZKEM\CLSID" /ve /reg:32 2^>nul ^| find /i "REG_SZ"') do set "PROGID_CLSID=%%B"
if defined PROGID_CLSID (
  >>"%LOG_FILE%" echo [%date% %time%] ProgID CLSID: %PROGID_CLSID%
) else (
  >>"%LOG_FILE%" echo [%date% %time%] WARNING: ProgID has no CLSID mapping.
)

if defined PROGID_CLSID (
  set "INPROC="
  for /f "tokens=2*" %%A in ('reg query "HKCR\CLSID\%PROGID_CLSID%\InprocServer32" /ve /reg:32 2^>nul ^| find /i "REG_SZ"') do set "INPROC=%%B"
  >>"%LOG_FILE%" echo [%date% %time%] InprocServer32: %INPROC%
)

if exist "%WIN_DIR%\SysWOW64\WindowsPowerShell\v1.0\powershell.exe" (
  >>"%LOG_FILE%" echo [%date% %time%] Checking COM activation in 32-bit PowerShell...
  "%WIN_DIR%\SysWOW64\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -Command "try{New-Object -ComObject 'zkemkeeper.ZKEM.1' | Out-Null; exit 0}catch{Write-Output ('COM_FAIL: ' + $PSItem.Exception.Message); exit 5}" >>"%LOG_FILE%" 2>>&1
  if errorlevel 1 (
    >>"%LOG_FILE%" echo [%date% %time%] ERROR: COM activation failed.
    >>"%LOG_FILE%" echo [%date% %time%] Hint: This is commonly caused by missing VC++ x86 runtimes or a broken registry mapping.
    >>"%LOG_FILE%" echo [%date% %time%] Dumping ProgID and CLSID registry entries...
    reg query "HKCR\zkemkeeper.CZKEM" /s /reg:32 >>"%LOG_FILE%" 2>>&1
    reg query "HKCR\CLSID\{00853A19-BD51-419B-9269-2DABE57EB61F}" /s /reg:32 >>"%LOG_FILE%" 2>>&1
    if "%FORCE_CLEAN%"=="1" (
      >>"%LOG_FILE%" echo [%date% %time%] FORCE_CLEAN=1, deleting existing keys and re-registering...
      reg delete "HKCR\zkemkeeper.CZKEM" /f /reg:32 >>"%LOG_FILE%" 2>>&1
      reg delete "HKCR\CLSID\{00853A19-BD51-419B-9269-2DABE57EB61F}" /f /reg:32 >>"%LOG_FILE%" 2>>&1
      "%TARGET%\regsvr32.exe" "%TARGET%\zkemkeeper.dll"
      >>"%LOG_FILE%" echo [%date% %time%] Re-register exit code: %errorlevel%
    )
    if "%INTERACTIVE%"=="1" echo ERROR: COM activation failed. Open log: %LOG_FILE% & pause
    exit /b 5
  )
)

>>"%LOG_FILE%" echo [%date% %time%] OK: Installed and verified.
exit /b 0
