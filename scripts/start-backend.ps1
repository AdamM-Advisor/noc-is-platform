param(
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 8000,
    [switch]$InstallDeps
)

$ErrorActionPreference = "Stop"
$Root = Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")
Set-Location -LiteralPath $Root

function Import-DotEnv {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }
    Get-Content -LiteralPath $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) {
            return
        }
        $parts = $line.Split("=", 2)
        [Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), "Process")
    }
}

function Get-LocalPython {
    $venvPython = Join-Path $Root ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $venvPython) {
        return $venvPython
    }
    if ($env:NOCIS_PYTHON) {
        return $env:NOCIS_PYTHON
    }
    if (Get-Command python -ErrorAction SilentlyContinue) {
        return "python"
    }
    if (Get-Command py -ErrorAction SilentlyContinue) {
        return "py"
    }
    throw "Python tidak ditemukan. Jalankan .\scripts\setup-local.ps1 terlebih dahulu."
}

if (-not (Test-Path -LiteralPath ".env.local")) {
    & (Join-Path $PSScriptRoot "setup-local.ps1") -SkipInstall
}

if ($InstallDeps) {
    & (Join-Path $PSScriptRoot "setup-local.ps1")
}

Import-DotEnv -Path ".env.local"
$python = Get-LocalPython
$args = @("-m", "uvicorn", "backend.main:app", "--host", $BindHost, "--port", "$Port")
if ($python -eq "py") {
    & py -3 @args
} else {
    & $python @args
}
