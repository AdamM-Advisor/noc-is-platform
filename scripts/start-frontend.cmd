@echo off
setlocal

powershell.exe -NoExit -ExecutionPolicy Bypass -File "%~dp0start-frontend.ps1" %*

endlocal
