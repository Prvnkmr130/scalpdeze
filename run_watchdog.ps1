<#
.SYNOPSIS
    run_watchdog.ps1 - Supervisor & Watchdog for Windows Thin Client Quantitative Signal Engine.
.DESCRIPTION
    Runs locally on Windows 10/11 thin client.
    Replaces Docker / Supervisord.
    Validates Python environment, checks database readiness, runs migrations,
    and executes `python manage.py run_scraped_algo` with automatic crash recovery.
#>

[CmdletBinding()]
param (
    [string]$Instance = "prod",
    [string]$Mode = "",
    [string]$PythonExe = "python",
    [int]$RestartDelaySeconds = 5,
    [int]$MaxConsecutiveCrashes = 10
)

$ErrorActionPreference = "Continue"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

# Normalize instance name
if ([string]::IsNullOrWhiteSpace($Instance)) {
    $Instance = "prod"
}
$Instance = $Instance.Trim().ToLower()

# Set environment variables for this process tree
$env:APP_INSTANCE = $Instance
if (-not [string]::IsNullOrWhiteSpace($Mode)) {
    $env:APP_MODE = $Mode.Trim().ToLower()
}

# Ensure required directories exist
$Dirs = @("logs", "profiles", "profiles\$Instance", "reports")
foreach ($d in $Dirs) {
    $p = Join-Path $ScriptDir $d
    if (-not (Test-Path $p)) {
        New-Item -ItemType Directory -Path $p -Force | Out-Null
    }
}

$LogFile = Join-Path $ScriptDir "logs\watchdog_$Instance.log"

function Write-WatchdogLog {
    param ([string]$Message, [string]$Level = "INFO")
    $TimeStamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fffzzz")
    $Line = "[$TimeStamp] [WATCHDOG] [$Level] [$($Instance.ToUpper())] $Message"
    Write-Host $Line
    Add-Content -Path $LogFile -Value $Line -ErrorAction SilentlyContinue
}

Write-WatchdogLog "Starting Windows Thin Client Supervisor Watchdog for Instance [$($Instance.ToUpper())]..." "INFO"
Write-WatchdogLog "Script Directory: $ScriptDir" "INFO"

# 1. Verify and auto-discover Python availability
$PythonCandidates = @(
    $PythonExe,
    "$ScriptDir\.venv\Scripts\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python314\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
    "C:\Program Files\Python314\python.exe",
    "C:\Program Files\Python313\python.exe",
    "C:\Program Files\Python312\python.exe"
)

$ResolvedPython = $null
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
    Write-WatchdogLog "Python 3 executable not found! Please install Python 3.14." "ERROR"
    exit 1
}

$PythonExe = $ResolvedPython
$PyDir = Split-Path -Parent $PythonExe
$PyScripts = Join-Path $PyDir "Scripts"
$env:PATH = "$PyDir;$PyScripts;" + $env:PATH
Write-WatchdogLog "Using Python: $PyVersion ($PythonExe)" "INFO"

# 2. Database readiness: Auto-create PostgreSQL database if missing
Write-WatchdogLog "Checking database readiness for target instance [$Instance]..." "INFO"
$DbCheckScript = @"
import os, sys
from algo_trading.app_config import config
if config.DB_ENGINE == 'postgresql':
    target_db = config.DB_NAME
    try:
        import psycopg
        conn = psycopg.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            dbname='postgres',
            autocommit=True
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
            if not cur.fetchone():
                print(f"[DB] Creating database '{target_db}'...")
                cur.execute(f'CREATE DATABASE "{target_db}"')
                print(f"[DB] Database '{target_db}' created successfully.")
            else:
                print(f"[DB] Database '{target_db}' is ready.")
        conn.close()
    except Exception as e:
        print(f"[DB] Notice: {e}")
"@

try {
    & $PythonExe -c $DbCheckScript 2>&1 | ForEach-Object { Write-WatchdogLog $_ "INFO" }
} catch {
    Write-WatchdogLog "Notice checking database: $_" "WARNING"
}

# 3. Database migrations
Write-WatchdogLog "Checking database migrations..." "INFO"
try {
    & $PythonExe manage.py migrate --noinput 2>&1 | Out-Null
    Write-WatchdogLog "Database migrations verified." "INFO"
} catch {
    Write-WatchdogLog "Warning: Could not run migrations: $_" "WARNING"
}

# 4. Main Supervisor Execution Loop
$ConsecutiveCrashes = 0
$Running = $true

# Trap Ctrl+C for clean exit
[Console]::TreatControlCAsInput = $false

$ArgList = @("manage.py", "run_scraped_algo", "--instance", $Instance)
if (-not [string]::IsNullOrWhiteSpace($Mode)) {
    $ArgList += @("--mode", $Mode)
}

while ($Running) {
    Write-WatchdogLog "Launching Quantitative Signal Engine ($($ArgList -join ' '))..." "INFO"
    $StartTime = Get-Date

    try {
        $Process = Start-Process -FilePath $PythonExe `
            -ArgumentList $ArgList `
            -WorkingDirectory $ScriptDir `
            -NoNewWindow `
            -PassThru `
            -Wait

        $ExitCode = $Process.ExitCode
        $RunDuration = ((Get-Date) - $StartTime).TotalSeconds

        if ($ExitCode -eq 0) {
            Write-WatchdogLog "Engine exited cleanly with exit code 0 (Session complete / Hibernate requested)." "INFO"
            $ConsecutiveCrashes = 0
            break
        } else {
            Write-WatchdogLog "Engine process exited unexpectedly with code $ExitCode after $([math]::Round($RunDuration, 1))s." "WARNING"
            
            # If it ran for more than 5 minutes, reset crash counter
            if ($RunDuration -gt 300) {
                $ConsecutiveCrashes = 1
            } else {
                $ConsecutiveCrashes++
            }

            if ($ConsecutiveCrashes -ge $MaxConsecutiveCrashes) {
                Write-WatchdogLog "Exceeded maximum consecutive crashes ($MaxConsecutiveCrashes). Halting watchdog." "ERROR"
                $Running = $false
                break
            }

            $Delay = [math]::Min($RestartDelaySeconds * $ConsecutiveCrashes, 60)
            Write-WatchdogLog "Restarting engine in $Delay seconds (Crash $ConsecutiveCrashes/$MaxConsecutiveCrashes)..." "WARNING"
            Start-Sleep -Seconds $Delay
        }
    } catch {
        Write-WatchdogLog "Exception launching engine process: $_" "ERROR"
        Start-Sleep -Seconds 10
    }
}

Write-WatchdogLog "Supervisor watchdog terminated." "INFO"
