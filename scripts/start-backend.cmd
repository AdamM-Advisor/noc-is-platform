@echo off
setlocal

powershell.exe -NoExit -ExecutionPolicy Bypass -File "%~dp0start-backend.ps1" %*

endlocal
