param(
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 5173,
    [switch]$InstallDeps
)

$ErrorActionPreference = "Stop"
$Root = Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")
$Frontend = Join-Path $Root "frontend"

if (-not (Test-Path -LiteralPath (Join-Path $Frontend ".env.local"))) {
    "VITE_API_ORIGIN=http://127.0.0.1:8000" | Set-Content -LiteralPath (Join-Path $Frontend ".env.local") -Encoding UTF8
}

Push-Location -LiteralPath $Frontend
try {
    if ($InstallDeps -or -not (Test-Path -LiteralPath "node_modules")) {
        npm install
    }
    npm run dev -- --host $BindHost --port $Port
} finally {
    Pop-Location
}
