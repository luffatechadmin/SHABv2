cd /d %~dp0
copy .\sdk\*.dll %windir%\SysWOW64\
%windir%\SysWOW64\regsvr32.exe %windir%\SysWOW64\zkemkeeper.dll
