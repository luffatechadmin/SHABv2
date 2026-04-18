If the browser shows “This site can’t be reached” (localhost/127.0.0.1 refused to connect):

1) Check if the middleware started
   - Open: Logs\attendance-middleware.log
   - Open: Logs\middleware-stderr.log
   - Open: Logs\middleware-stdout.log

2) Common causes
   - Antivirus/Defender blocked WL10Middleware.exe (try running Start Dashboard.bat as Administrator)
   - Missing Microsoft Visual C++ runtime on the PC (install “Microsoft Visual C++ Redistributable (x86)”)
   - Port 5099 already used by another app

3) Quick checks
   - Run Stop Dashboard.bat, then run Start Dashboard.bat again.
   - In a command prompt:
     - netstat -ano | findstr :5099

4) Dashboard URL
   - http://localhost:5099/login
   - If needed: http://127.0.0.1:5099/login

