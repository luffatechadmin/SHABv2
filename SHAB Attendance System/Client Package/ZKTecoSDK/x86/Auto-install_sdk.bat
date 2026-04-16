@echo off
setlocal EnableExtensions
cd /d "%~dp0"

net session >nul 2>&1
if errorlevel 1 (
  set "WIN_DIR=%SystemRoot%"
  if not defined WIN_DIR set "WIN_DIR=%windir%"
  set "PS_EXE=%WIN_DIR%\System32\WindowsPowerShell\v1.0\powershell.exe"
  if exist "%PS_EXE%" (
    "%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -Verb RunAs -WorkingDirectory '%~dp0' -FilePath '%~f0' -Wait" >nul 2>&1
    exit /b %errorlevel%
  )
  echo ERROR: Administrator privileges are required to install/register the ZKTeco SDK.
  echo Please right-click and choose "Run as administrator".
  pause
  exit /b 1
)

set "WIN_DIR=%SystemRoot%"
if not defined WIN_DIR set "WIN_DIR=%windir%"
set "TARGET=%WIN_DIR%\SysWOW64"
if not exist "%TARGET%\regsvr32.exe" set "TARGET=%WIN_DIR%\System32"
set "PS_EXE=%WIN_DIR%\System32\WindowsPowerShell\v1.0\powershell.exe"

copy /y ".\sdk\*.dll" "%TARGET%\" >nul
if errorlevel 1 exit /b 1

"%TARGET%\regsvr32.exe" /s "%TARGET%\zkemkeeper.dll"
if errorlevel 1 exit /b 2

reg query "HKCR\zkemkeeper.CZKEM" >nul 2>&1
if errorlevel 1 exit /b 3

if exist "%WIN_DIR%\SysWOW64\WindowsPowerShell\v1.0\powershell.exe" (
  "%WIN_DIR%\SysWOW64\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -Command "try{New-Object -ComObject zkemkeeper.CZKEM | Out-Null; exit 0}catch{exit 5}" >nul 2>&1
  if errorlevel 1 exit /b 5
)

exit /b 0
