@echo off
setlocal EnableExtensions
cd /d "%~dp0"

set "WIN_DIR=%SystemRoot%"
if not defined WIN_DIR set "WIN_DIR=%windir%"
set "TARGET=%WIN_DIR%\SysWOW64"
if not exist "%TARGET%\regsvr32.exe" set "TARGET=%WIN_DIR%\System32"

copy /y ".\sdk\*.dll" "%TARGET%\" >nul
if errorlevel 1 exit /b 1

"%TARGET%\regsvr32.exe" /s "%TARGET%\zkemkeeper.dll"
if errorlevel 1 exit /b 2

exit /b 0
