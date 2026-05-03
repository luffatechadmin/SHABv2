@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
set "PKG_DIR=%ROOT%SHAB Attendance System\Client Package"
set "TARGET=%PKG_DIR%\Stop Dashboard.bat"

if not exist "%TARGET%" (
  echo ERROR: Could not find:
  echo   %TARGET%
  exit /b 1
)

pushd "%PKG_DIR%" >nul 2>&1
call "%TARGET%" %*
set "EC=%errorlevel%"
popd >nul 2>&1
exit /b %EC%
