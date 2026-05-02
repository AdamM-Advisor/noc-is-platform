param(
    [switch]$InstallDeps,
    [switch]$RestartExisting
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
$backendArgs = @(
    "-NoExit",
    "-ExecutionPolicy", "Bypass",
    "-File", "`"$backend`""
)
$frontendArgs = @(
    "-NoExit",
    "-ExecutionPolicy", "Bypass",
    "-File", "`"$frontend`""
)

if ($RestartExisting) {
    $backendArgs += "-RestartExisting"
    $frontendArgs += "-RestartExisting"
}

Start-Process -FilePath powershell.exe -ArgumentList $backendArgs -WorkingDirectory $Root

Start-Process -FilePath powershell.exe -ArgumentList $frontendArgs -WorkingDirectory $Root

Write-Host "Backend : http://127.0.0.1:8000"
Write-Host "Frontend: http://127.0.0.1:5173"
Write-Host "Restart bersih: .\scripts\start-local.cmd -RestartExisting"
Write-Host "Login lokal default dibuat oleh setup-local.ps1 bila .env.local belum ada."
