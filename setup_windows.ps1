# -*- coding: utf-8 -*-
<#
.SYNOPSIS
    setup_windows.ps1 - Automated Setup for Windows Thin Client Quantitative Signal Engine.
.DESCRIPTION
    Installs Python dependencies, Playwright/Patchright Chromium browser binaries,
    creates local runtime directories, and optionally registers the Windows Task Scheduler
    RTC Wake task to wake the PC automatically before market open.
#>

[CmdletBinding()]
param (
    [string]$PythonExe = "",
    [switch]$SkipPlaywrightInstall,
    [switch]$SkipTaskScheduler
)

$ErrorActionPreference = "Continue"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host " Windows Thin Client Quantitative Signal Engine -- Setup" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan

# 1. Verify and auto-discover Python
Write-Host "`n[1/5] Verifying Python..." -ForegroundColor Yellow
$PythonCandidates = @(
    $PythonExe,
    "$env:LOCALAPPDATA\Programs\Python\Python314\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
    "C:\Program Files\Python314\python.exe",
    "C:\Program Files\Python313\python.exe",
    "C:\Program Files\Python312\python.exe",
    "python"
)

$ResolvedPython = $null
$PyVersion = ""
foreach ($cand in $PythonCandidates) {
    if ([string]::IsNullOrWhiteSpace($cand)) { continue }
    try {
        $ver = & $cand --version 2>&1
        if ($LASTEXITCODE -eq 0 -and $ver -match "Python\s+3\.") {
            $ResolvedPython = $cand
            $PyVersion = $ver
            break
        }
    } catch {
        continue
    }
}

if (-not $ResolvedPython) {
    Write-Host "  Error: Python 3 executable not found!" -ForegroundColor Red
    Write-Host "  Please install Python 3.14." -ForegroundColor Red
    exit 1
}

$PythonExe = $ResolvedPython
$PyDir = Split-Path -Parent $PythonExe
$PyScripts = Join-Path $PyDir "Scripts"
$env:Path = "$PyDir;$PyScripts;" + $env:Path
Write-Host "  Found Python: $PyVersion ($PythonExe)" -ForegroundColor Green

# 2. Create local directory structure
Write-Host "`n[2/5] Creating local runtime directories..." -ForegroundColor Yellow
$Dirs = @("logs", "profiles", "reports")
foreach ($d in $Dirs) {
    $p = Join-Path $ScriptDir $d
    if (-not (Test-Path $p)) {
        New-Item -ItemType Directory -Path $p -Force | Out-Null
        Write-Host "  Created directory: $d" -ForegroundColor Green
    } else {
        Write-Host "  Directory already exists: $d" -ForegroundColor DarkGray
    }
}

# 3. Install Python dependencies
Write-Host "`n[3/5] Installing Python dependencies..." -ForegroundColor Yellow
try {
    & $PythonExe -m pip install --upgrade pip
    & $PythonExe -m pip install -e .
    Write-Host "  Dependencies installed successfully." -ForegroundColor Green
} catch {
    Write-Host "  Warning: pip install encountered an issue: $_" -ForegroundColor Yellow
}

# 4. Install Playwright / Patchright Chromium
if (-not $SkipPlaywrightInstall) {
    Write-Host "`n[4/5] Installing Playwright Chromium browser binaries..." -ForegroundColor Yellow
    try {
        & $PythonExe -m playwright install chromium
        Write-Host "  Playwright Chromium installed." -ForegroundColor Green
    } catch {
        Write-Host "  Notice: Could not run playwright install directly; attempting patchright..." -ForegroundColor Yellow
        try {
            & $PythonExe -m patchright install chromium
            Write-Host "  Patchright Chromium installed." -ForegroundColor Green
        } catch {
            Write-Host "  Warning: Browser installation failed. Run 'playwright install chromium' manually." -ForegroundColor Red
        }
    }
} else {
    Write-Host "`n[4/5] Skipping Playwright install as requested." -ForegroundColor DarkGray
}

# 5. Register Task Scheduler RTC Wake Task
if (-not $SkipTaskScheduler) {
    Write-Host "`n[5/5] Registering Windows Task Scheduler RTC Wake Task..." -ForegroundColor Yellow
    $TaskName = "DeltaZero26_Market_Wake"
    $WakeTime = "09:15"

    try {
        $WatchdogScript = Join-Path $ScriptDir "run_watchdog.ps1"
        $Action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-ExecutionPolicy Bypass -File `'$WatchdogScript`'" -WorkingDirectory $ScriptDir
        $Trigger = New-ScheduledTaskTrigger -Daily -At $WakeTime
        $Settings = New-ScheduledTaskSettingsSet -WakeToRun -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
        $Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
        Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -Principal $Principal | Out-Null
        Write-Host "  Task '$TaskName' registered successfully (WakeToRun at $WakeTime daily)." -ForegroundColor Green
    } catch {
        Write-Host "  Warning: Could not register scheduled task (Administrator required): $_" -ForegroundColor Yellow
    }
} else {
    Write-Host "`n[5/5] Skipping Task Scheduler registration as requested." -ForegroundColor DarkGray
}

Write-Host "`n==================================================================" -ForegroundColor Cyan
Write-Host " Setup complete! To start the engine, run: .\run_watchdog.ps1 or .\launch.bat" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
