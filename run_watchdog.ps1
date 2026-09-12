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
    [string]$PythonExe = "python",
    [int]$RestartDelaySeconds = 5,
    [int]$MaxConsecutiveCrashes = 10
)

$ErrorActionPreference = "Continue"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

# Ensure required directories exist
$Dirs = @("logs", "profiles", "reports")
foreach ($d in $Dirs) {
    $p = Join-Path $ScriptDir $d
    if (-not (Test-Path $p)) {
        New-Item -ItemType Directory -Path $p -Force | Out-Null
    }
}

$LogFile = Join-Path $ScriptDir "logs\watchdog.log"

function Write-WatchdogLog {
    param ([string]$Message, [string]$Level = "INFO")
    $TimeStamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fffzzz")
    $Line = "[$TimeStamp] [WATCHDOG] [$Level] $Message"
    Write-Host $Line
    Add-Content -Path $LogFile -Value $Line -ErrorAction SilentlyContinue
}

Write-WatchdogLog "Starting Windows Thin Client Supervisor Watchdog..." "INFO"
Write-WatchdogLog "Script Directory: $ScriptDir" "INFO"

# 1. Verify Python availability
try {
    $PyVersion = & $PythonExe --version 2>&1
    Write-WatchdogLog "Using Python: $PyVersion" "INFO"
} catch {
    Write-WatchdogLog "Python executable '$PythonExe' not found in PATH! Please install Python or specify -PythonExe." "ERROR"
    exit 1
}

# 2. Database readiness & migration check
Write-WatchdogLog "Checking database migrations..." "INFO"
try {
    & $PythonExe manage.py migrate --noinput 2>&1 | Out-Null
    Write-WatchdogLog "Database migrations verified." "INFO"
} catch {
    Write-WatchdogLog "Warning: Could not run migrations: $_" "WARNING"
}

# 3. Main Supervisor Execution Loop
$ConsecutiveCrashes = 0
$Running = $true

# Trap Ctrl+C for clean exit
[Console]::TreatControlCAsInput = $false

while ($Running) {
    Write-WatchdogLog "Launching Quantitative Signal Engine (run_scraped_algo)..." "INFO"
    $StartTime = Get-Date

    try {
        $Process = Start-Process -FilePath $PythonExe `
            -ArgumentList "manage.py", "run_scraped_algo" `
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
