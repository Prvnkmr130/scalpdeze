<#
.SYNOPSIS
    launch.ps1 - One-command PowerShell launcher for Windows Thin Client Signal Engine.
#>

[CmdletBinding()]
param (
    [Parameter(Position=0)]
    [string]$Instance = "prod",
    [string]$Mode = "",
    [switch]$NoTray,
    [switch]$NoFailover,
    [switch]$NoPower,
    [string]$Symbols = "SPY,QQQ,AAPL"
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

# 1. Discover Python
$PythonCandidates = @(
    "$ScriptDir\.venv\Scripts\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python314\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
    "C:\Program Files\Python314\python.exe",
    "C:\Program Files\Python313\python.exe",
    "C:\Program Files\Python312\python.exe",
    "python"
)

$PyExe = $null
foreach ($cand in $PythonCandidates) {
    try {
        $ver = & $cand --version 2>&1
        if ($LASTEXITCODE -eq 0 -and $ver -match "Python\s+3\.") {
            $PyExe = $cand
            break
        }
    } catch {
        continue
    }
}

if (-not $PyExe) {
    Write-Host "Error: Python 3 was not found! Please install Python 3.14." -ForegroundColor Red
    exit 1
}

$PyDir = Split-Path -Parent $PyExe
$env:PATH = "$PyDir;$PyDir\Scripts;" + $env:PATH

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host " DeltaZero26 — Launching Windows Thin Client Signal Engine" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "Python: $PyExe ($(& $PyExe --version 2>&1))" -ForegroundColor Green

# 2. Ensure .env exists
$EnvFile = Join-Path $ScriptDir ".env"
$EnvExample = Join-Path $ScriptDir ".env.example"
if (-not (Test-Path $EnvFile) -and (Test-Path $EnvExample)) {
    Copy-Item $EnvExample $EnvFile
    Write-Host "Created .env from .env.example" -ForegroundColor Yellow
}

# 3. Launch watchdog
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$ScriptDir\run_watchdog.ps1" -Instance $Instance -Mode $Mode -PythonExe $PyExe
