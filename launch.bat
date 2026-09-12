@echo off
setlocal enabledelayedexpansion

:: 0. Parse instance argument (%1) defaulting to prod
set "INST=%~1"
if "%INST%"=="" set "INST=prod"

title DeltaZero26 - [%INST%] Signal Engine

echo ==================================================================
echo  DeltaZero26 Quantitative Signal Engine [Instance: %INST%]
echo ==================================================================
echo.

cd /d "%~dp0"

:: 1. Auto-discover Python (prefer project .venv, then system Python 3.14)
set "PY_EXE="
if exist "%~dp0.venv\Scripts\python.exe" (
    set "PY_EXE=%~dp0.venv\Scripts\python.exe"
) else if exist "%LOCALAPPDATA%\Programs\Python\Python314\python.exe" (
    set "PY_EXE=%LOCALAPPDATA%\Programs\Python\Python314\python.exe"
) else if exist "%ProgramFiles%\Python314\python.exe" (
    set "PY_EXE=%ProgramFiles%\Python314\python.exe"
) else if exist "%LOCALAPPDATA%\Programs\Python\Python313\python.exe" (
    set "PY_EXE=%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
) else if exist "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" (
    set "PY_EXE=%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
) else (
    where python >nul 2>&1
    if !errorlevel! equ 0 (
        set "PY_EXE=python"
    )
)

if "%PY_EXE%"=="" (
    echo [ERROR] Python was not found on your system!
    echo Please install Python 3.14 and run this file again.
    pause
    exit /b 1
)

:: Update PATH in current session to include Python and Scripts
for %%I in ("%PY_EXE%") do set "PY_DIR=%%~dpI"
set "PATH=%PY_DIR%;%PY_DIR%Scripts;%PATH%"

echo [*] Using Python: %PY_EXE%
"%PY_EXE%" --version
echo.

:: 2. Ensure .env exists
if not exist ".env" (
    echo [*] Creating .env from .env.example...
    copy ".env.example" ".env" >nul
    echo [*] Created .env file.
)

:: 3. Launch Supervisor Watchdog for target instance
echo [*] Starting Supervisor Watchdog for [%INST%]...
powershell -NoProfile -ExecutionPolicy Bypass -File ".\run_watchdog.ps1" -Instance "%INST%" -PythonExe "%PY_EXE%"

if %errorlevel% neq 0 (
    echo.
    echo [!] Engine exited with code %errorlevel%.
    pause
)
