@echo off
setlocal enabledelayedexpansion

:: Set colors for better user experience
color 0A
title AWACS - Starting Application

echo.
echo ========================================
echo    AWACS AI Annotation Tool - Startup
echo ========================================
echo.

:: Get the script directory
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

:: Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH!
    echo Please install Python 3.8 or higher from https://www.python.org/
    pause
    exit /b 1
)

:: Check if Node.js is installed
node --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Node.js is not installed or not in PATH!
    echo Please install Node.js from https://nodejs.org/
    pause
    exit /b 1
)

echo [INFO] Python and Node.js are installed.
echo.

:: ============================================
:: Python Virtual Environment Setup
:: ============================================
echo [STEP 1/4] Setting up Python Virtual Environment...
if not exist "venv" (
    echo [INFO] Virtual environment not found. Creating new venv...
    python -m venv venv
    if errorlevel 1 (
        echo [ERROR] Failed to create virtual environment!
        pause
        exit /b 1
    )
    echo [SUCCESS] Virtual environment created.
) else (
    echo [INFO] Virtual environment already exists. Skipping creation.
)

:: Activate virtual environment
echo [INFO] Activating virtual environment...
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo [ERROR] Failed to activate virtual environment!
    pause
    exit /b 1
)

:: ============================================
:: Install Python Requirements
:: ============================================
echo.
echo [STEP 2/4] Checking Python Dependencies...

:: Upgrade pip first
echo [INFO] Upgrading pip...
pip install --upgrade pip --quiet

:: Check and install root requirements.txt (or requirments.txt with typo)
set "ROOT_REQUIREMENTS=requirments.txt"
if not exist "%ROOT_REQUIREMENTS%" (
    set "ROOT_REQUIREMENTS=requirements.txt"
)

set "NEED_ROOT_INSTALL=0"
if exist "%ROOT_REQUIREMENTS%" (
    echo [INFO] Checking root requirements from %ROOT_REQUIREMENTS%...
    python -c "import pandas, openpyxl, selenium, requests, google.generativeai, tqdm" >nul 2>&1
    if errorlevel 1 (
        set "NEED_ROOT_INSTALL=1"
    )
    
    if !NEED_ROOT_INSTALL! equ 1 (
        echo [INFO] Installing root requirements from %ROOT_REQUIREMENTS%...
        pip install -r "%ROOT_REQUIREMENTS%"
        if errorlevel 1 (
            echo [ERROR] Failed to install root requirements from %ROOT_REQUIREMENTS%!
            pause
            exit /b 1
        )
        echo [SUCCESS] Root requirements installed from %ROOT_REQUIREMENTS%.
    ) else (
        echo [INFO] Root requirements already installed. Skipping installation.
    )
) else (
    echo [WARNING] Root requirements file not found (checked requirments.txt and requirements.txt)
)

:: Check and install backend requirements.txt
set "BACKEND_REQUIREMENTS=backend\requirements.txt"
set "NEED_BACKEND_INSTALL=0"
if exist "%BACKEND_REQUIREMENTS%" (
    echo [INFO] Checking backend requirements from %BACKEND_REQUIREMENTS%...
    python -c "import fastapi, uvicorn" >nul 2>&1
    if errorlevel 1 (
        set "NEED_BACKEND_INSTALL=1"
    )
    
    if !NEED_BACKEND_INSTALL! equ 1 (
        echo [INFO] Installing backend requirements from %BACKEND_REQUIREMENTS%...
        pip install -r "%BACKEND_REQUIREMENTS%"
        if errorlevel 1 (
            echo [ERROR] Failed to install backend requirements from %BACKEND_REQUIREMENTS%!
            pause
            exit /b 1
        )
        echo [SUCCESS] Backend requirements installed from %BACKEND_REQUIREMENTS%.
    ) else (
        echo [INFO] Backend requirements already installed. Skipping installation.
    )
) else (
    echo [WARNING] Backend requirements file not found at %BACKEND_REQUIREMENTS%
)

:: ============================================
:: Install Frontend Dependencies
:: ============================================
echo.
echo [STEP 3/4] Checking Frontend Dependencies...
set "FRONTEND_DIR=frontend\frontend"
if exist "%FRONTEND_DIR%\package.json" (
    cd /d "%FRONTEND_DIR%"
    if not exist "node_modules" (
        echo [INFO] Installing frontend dependencies (this may take a few minutes)...
        call npm install
        if errorlevel 1 (
            echo [ERROR] Failed to install frontend dependencies!
            cd /d "%SCRIPT_DIR%"
            pause
            exit /b 1
        )
        echo [SUCCESS] Frontend dependencies installed.
    ) else (
        echo [INFO] Frontend dependencies already installed. Skipping installation.
    )
    cd /d "%SCRIPT_DIR%"
) else (
    echo [WARNING] Frontend package.json not found at %FRONTEND_DIR%\package.json
)

:: ============================================
:: Start Backend and Frontend
:: ============================================
echo.
echo [STEP 4/4] Starting Backend and Frontend Services...
echo.

:: Create a PID file to track processes
set "PID_FILE=%TEMP%\awacs_pids.txt"
echo. > "%PID_FILE%"

:: Start Backend in a new window
echo [INFO] Starting Backend Server (FastAPI on port 8000)...
start "AWACS Backend" cmd /k "cd /d "%SCRIPT_DIR%" && call venv\Scripts\activate.bat && cd backend && echo Backend Server Starting... && python main.py"
timeout /t 3 /nobreak >nul

:: Start Frontend in a new window
echo [INFO] Starting Frontend Server (React on port 3000)...
cd /d "%FRONTEND_DIR%"
start "AWACS Frontend" cmd /k "cd /d "%FRONTEND_DIR%" && echo Frontend Server Starting... && npm start"
cd /d "%SCRIPT_DIR%"

:: Wait a moment for services to start
timeout /t 5 /nobreak >nul

echo.
echo ========================================
echo    Application Started Successfully!
echo ========================================
echo.
echo Backend API: http://localhost:8000
echo Frontend App: http://localhost:3000
echo.
echo [INFO] Two windows have been opened:
echo        - AWACS Backend (minimized)
echo        - AWACS Frontend
echo.
echo [INFO] To stop the application, run Stop.bat
echo        or close the Backend and Frontend windows.
echo.
echo This window will remain open to monitor the services.
echo Press any key to close this window (services will continue running)...
pause >nul

endlocal

