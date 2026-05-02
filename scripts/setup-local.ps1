param(
    [string]$AdminEmail = "admin@example.com",
    [string]$AdminPassword = "Admin@12345",
    [switch]$SkipInstall
)

$ErrorActionPreference = "Stop"
$Root = Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")
Set-Location -LiteralPath $Root

function Invoke-HostPython {
    param([string[]]$PythonArgs)
    if ($env:NOCIS_PYTHON) {
        & $env:NOCIS_PYTHON @PythonArgs
        return
    }
    if (Get-Command python -ErrorAction SilentlyContinue) {
        & python @PythonArgs
        return
    }
    if (Get-Command py -ErrorAction SilentlyContinue) {
        & py -3 @PythonArgs
        return
    }
    throw "Python tidak ditemukan. Install Python 3.11+ atau set env NOCIS_PYTHON."
}

function Get-Sha256Hex {
    param([string]$Text)
    $sha = [System.Security.Cryptography.SHA256]::Create()
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
    $hash = $sha.ComputeHash($bytes)
    return ([System.BitConverter]::ToString($hash) -replace "-", "").ToLowerInvariant()
}

function New-SecretHex {
    $bytes = New-Object byte[] 32
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $rng.GetBytes($bytes)
    return ([System.BitConverter]::ToString($bytes) -replace "-", "").ToLowerInvariant()
}

$backendEnv = Join-Path $Root ".env.local"
$frontendEnv = Join-Path $Root "frontend\.env.local"
$passwordHash = Get-Sha256Hex $AdminPassword
$sessionSecret = New-SecretHex

if (-not (Test-Path -LiteralPath $backendEnv)) {
    @"
APP_ENV=development
CORS_ALLOW_ORIGINS=http://127.0.0.1:5173,http://localhost:5173
COOKIE_SECURE=0
COOKIE_SAMESITE=lax
SESSION_COOKIE_DOMAIN=
SESSION_SECRET=$sessionSecret
ADMIN_EMAIL=$AdminEmail
ADMIN_PASSWORD_HASH=$passwordHash
LOCAL_AUTH_SHOW_2FA_CODE=1
NOCIS_DATA_DIR=.data
NOCIS_DB_PATH=.data/noc_analytics.duckdb
NOCIS_RAW_DIR=.data/raw
NOCIS_UPLOAD_DIR=.uploads
NOCIS_BACKUP_DIR=.data/backups
NOCIS_CHUNK_DIR=temp_chunks
NOCIS_EXPORT_DIR=exports
NOCIS_LAKE_ROOT=.parquet_lake
NOCIS_DUCKDB_MEMORY_LIMIT=4GB
NOCIS_DUCKDB_THREADS=4
NOCIS_DUCKDB_INSTALL_EXTENSIONS=0
NOCIS_UPLOAD_PIPELINE_MODE=parquet
NOCIS_ARCHIVE_RAW_FILES=0
NOCIS_DELETE_UPLOAD_AFTER_PROCESS=1
"@ | Set-Content -LiteralPath $backendEnv -Encoding UTF8
    Write-Host "Created .env.local"
} else {
    Write-Host ".env.local sudah ada, tidak ditimpa."
}

if (-not (Test-Path -LiteralPath $frontendEnv)) {
    "VITE_API_ORIGIN=http://127.0.0.1:8000" | Set-Content -LiteralPath $frontendEnv -Encoding UTF8
    Write-Host "Created frontend\.env.local"
} else {
    Write-Host "frontend\.env.local sudah ada, tidak ditimpa."
}

if (-not $SkipInstall) {
    $venvDir = Join-Path $Root ".venv"
    if (-not (Test-Path -LiteralPath $venvDir)) {
        Invoke-HostPython -PythonArgs @("-m", "venv", ".venv")
    }
    $venvPython = Join-Path $Root ".venv\Scripts\python.exe"
    & $venvPython -m pip install --upgrade pip
    & $venvPython -m pip install -r backend\requirements.txt

    Push-Location -LiteralPath (Join-Path $Root "frontend")
    try {
        npm install
    } finally {
        Pop-Location
    }
}

Write-Host ""
Write-Host "Local admin email   : $AdminEmail"
Write-Host "Local admin password: $AdminPassword"
Write-Host "2FA lokal akan ditampilkan di halaman login saat APP_ENV=development."
Write-Host ""
Write-Host "Start backend : .\scripts\start-backend.ps1"
Write-Host "Start frontend: .\scripts\start-frontend.ps1"
