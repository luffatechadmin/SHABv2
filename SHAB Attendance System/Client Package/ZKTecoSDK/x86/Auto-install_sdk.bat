@echo off
setlocal EnableExtensions
cd /d "%~dp0"

set "WIN_DIR=%SystemRoot%"
if not defined WIN_DIR set "WIN_DIR=%windir%"
set "PS_EXE=%WIN_DIR%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "LOG_FILE=%TEMP%\shab-zkteco-sdk-install.log"

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
  exit /b 2
)

>>"%LOG_FILE%" echo [%date% %time%] Checking registry key (32-bit view): HKCR\zkemkeeper.CZKEM
reg query "HKCR\zkemkeeper.CZKEM" /reg:32 >nul 2>&1
if errorlevel 1 (
  >>"%LOG_FILE%" echo [%date% %time%] ERROR: Registry key not found after registration.
  exit /b 3
)

if exist "%WIN_DIR%\SysWOW64\WindowsPowerShell\v1.0\powershell.exe" (
  >>"%LOG_FILE%" echo [%date% %time%] Checking COM activation in 32-bit PowerShell...
  "%WIN_DIR%\SysWOW64\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -Command "try{New-Object -ComObject 'zkemkeeper.CZKEM' | Out-Null; exit 0}catch{Write-Output ('COM_FAIL: ' + $_.Exception.Message); exit 5}" >>"%LOG_FILE%" 2>>&1
  if errorlevel 1 (
    >>"%LOG_FILE%" echo [%date% %time%] ERROR: COM activation failed.
    exit /b 5
  )
)

>>"%LOG_FILE%" echo [%date% %time%] OK: Installed and verified.
exit /b 0
