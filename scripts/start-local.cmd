@echo off
setlocal

powershell.exe -NoExit -ExecutionPolicy Bypass -File "%~dp0start-local.ps1" %*

endlocal
