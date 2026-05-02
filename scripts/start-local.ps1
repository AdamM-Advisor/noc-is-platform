param(
    [switch]$InstallDeps
)

$ErrorActionPreference = "Stop"
$Root = Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")

if (-not (Test-Path -LiteralPath (Join-Path $Root ".env.local"))) {
    & (Join-Path $PSScriptRoot "setup-local.ps1") -SkipInstall
}

if ($InstallDeps) {
    & (Join-Path $PSScriptRoot "setup-local.ps1")
}

$backend = Join-Path $PSScriptRoot "start-backend.ps1"
$frontend = Join-Path $PSScriptRoot "start-frontend.ps1"

Start-Process -FilePath powershell.exe -ArgumentList @(
    "-NoExit",
    "-ExecutionPolicy", "Bypass",
    "-File", "`"$backend`""
) -WorkingDirectory $Root

Start-Process -FilePath powershell.exe -ArgumentList @(
    "-NoExit",
    "-ExecutionPolicy", "Bypass",
    "-File", "`"$frontend`""
) -WorkingDirectory $Root

Write-Host "Backend : http://127.0.0.1:8000"
Write-Host "Frontend: http://127.0.0.1:5173"
Write-Host "Login lokal default dibuat oleh setup-local.ps1 bila .env.local belum ada."
