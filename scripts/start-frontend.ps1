param(
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 5173,
    [switch]$InstallDeps,
    [switch]$RestartExisting
)

$ErrorActionPreference = "Stop"
$Root = Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")
$Frontend = Join-Path $Root "frontend"

function Get-PortListeners {
    param([int]$LocalPort)
    $pids = @()
    try {
        $pids = @(Get-NetTCPConnection -LocalPort $LocalPort -State Listen -ErrorAction Stop |
            Select-Object -ExpandProperty OwningProcess -Unique)
    } catch {
        $pids = @()
    }
    if ($pids.Count -gt 0) {
        return $pids
    }

    $pattern = ":{0}$" -f $LocalPort
    return @(
        netstat -ano -p tcp |
            Select-String "LISTENING" |
            ForEach-Object {
                $parts = $_.Line.Trim() -split "\s+"
                if ($parts.Count -ge 5 -and $parts[1] -match $pattern) {
                    [int]$parts[4]
                }
            } |
            Select-Object -Unique
    )
}

function Stop-PortListeners {
    param([int]$LocalPort)
    $listeners = Get-PortListeners -LocalPort $LocalPort
    foreach ($processId in $listeners) {
        if ($processId -and $processId -ne $PID) {
            Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
        }
    }
}

if (-not (Test-Path -LiteralPath (Join-Path $Frontend ".env.local"))) {
    "VITE_API_ORIGIN=http://127.0.0.1:8000" | Set-Content -LiteralPath (Join-Path $Frontend ".env.local") -Encoding UTF8
}

$listeners = Get-PortListeners -LocalPort $Port
if ($listeners.Count -gt 0) {
    if (-not $RestartExisting) {
        Write-Host "Frontend sudah berjalan di http://$BindHost`:$Port. Gunakan -RestartExisting untuk restart bersih."
        exit 0
    }
    Stop-PortListeners -LocalPort $Port
    Start-Sleep -Seconds 2
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
