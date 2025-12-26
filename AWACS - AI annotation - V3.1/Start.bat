@echo off
setlocal enabledelayedexpansion

:: Get the script directory FIRST
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

:: Set colors for better user experience
color 0A
title AWACS - Starting Application

:: Create log file for debugging
set "LOG_FILE=%SCRIPT_DIR%startup_log.txt"
echo AWACS Startup Log - %date% %time% > "%LOG_FILE%"
echo ========================================== >> "%LOG_FILE%"
echo Starting AWACS Application... >> "%LOG_FILE%"
echo ========================================== >> "%LOG_FILE%"
echo Script Directory: %SCRIPT_DIR% >> "%LOG_FILE%"
echo Current Directory: %CD% >> "%LOG_FILE%"
echo User: %USERNAME% >> "%LOG_FILE%"
echo Computer: %COMPUTERNAME% >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

:: Set error handling - continue on error but log it
if not defined SCRIPT_DIR (
    echo [CRITICAL ERROR] SCRIPT_DIR not defined! >> "%LOG_FILE%"
    echo [CRITICAL ERROR] Script directory could not be determined!
    pause
    exit /b 1
)

echo.
echo ========================================
echo    AWACS AI Annotation Tool - Startup
echo ========================================
echo.

:: Check if Python is installed
echo [CHECK] Verifying Python installation... >> "%LOG_FILE%"
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH!
    echo [ERROR] Python not found! >> "%LOG_FILE%"
    pause
    exit /b 1
)
python --version >> "%LOG_FILE%" 2>&1
echo [SUCCESS] Python found >> "%LOG_FILE%"

:: Check if Node.js is installed
echo [CHECK] Verifying Node.js installation... >> "%LOG_FILE%"
node --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Node.js is not installed or not in PATH!
    echo [ERROR] Node.js not found! >> "%LOG_FILE%"
    pause
    exit /b 1
)
node --version >> "%LOG_FILE%" 2>&1
echo [SUCCESS] Node.js found >> "%LOG_FILE%"

echo [INFO] Python and Node.js are installed.
echo [INFO] Prerequisites check passed >> "%LOG_FILE%"
echo.

:: ============================================
:: Python Virtual Environment Setup
:: ============================================
echo [STEP 1/4] Setting up Python Virtual Environment...
echo [STEP 1/4] Setting up Python Virtual Environment... >> "%LOG_FILE%"
if not exist "venv" (
    echo [INFO] Virtual environment not found. Creating new venv...
    echo [INFO] Creating virtual environment... >> "%LOG_FILE%"
    python -m venv venv
    if errorlevel 1 (
        echo [ERROR] Failed to create virtual environment!
        echo [ERROR] Failed to create virtual environment! >> "%LOG_FILE%"
        pause
        exit /b 1
    )
    echo [SUCCESS] Virtual environment created.
    echo [SUCCESS] Virtual environment created >> "%LOG_FILE%"
) else (
    echo [INFO] Virtual environment already exists. Skipping creation.
    echo [INFO] Virtual environment exists >> "%LOG_FILE%"
)

:: Activate virtual environment
echo [INFO] Activating virtual environment...
echo [INFO] Activating virtual environment... >> "%LOG_FILE%"
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo [ERROR] Failed to activate virtual environment!
    echo [ERROR] Failed to activate virtual environment! >> "%LOG_FILE%"
    pause
    exit /b 1
)
echo [SUCCESS] Virtual environment activated >> "%LOG_FILE%"

:: Verify virtual environment is active
where python >> "%LOG_FILE%" 2>&1
where python | findstr /C:"venv" >nul 2>&1
if errorlevel 1 (
    echo [WARNING] Virtual environment may not be properly activated.
    echo [WARNING] Venv may not be properly activated >> "%LOG_FILE%"
    echo [INFO] Attempting to activate again...
    call venv\Scripts\activate.bat
    echo [INFO] Retry activation completed >> "%LOG_FILE%"
) else (
    echo [SUCCESS] Virtual environment verified active >> "%LOG_FILE%"
)

:: ============================================
:: Install Python Requirements
:: ============================================
echo.
echo [STEP 2/4] Checking Python Dependencies...
echo. >> "%LOG_FILE%"
echo [STEP 2/4] Checking Python Dependencies... >> "%LOG_FILE%"

:: Upgrade pip first
echo [INFO] Upgrading pip...
echo [INFO] Upgrading pip... >> "%LOG_FILE%"
pip install --upgrade pip --quiet --timeout 60 2>>"%LOG_FILE%"
if errorlevel 1 (
    echo [WARNING] Pip upgrade failed, continuing with existing version...
    echo [WARNING] Pip upgrade failed >> "%LOG_FILE%"
) else (
    echo [SUCCESS] Pip upgraded >> "%LOG_FILE%"
)

:: Check and install root requirements.txt (or requirments.txt with typo)
echo [INFO] Checking for root requirements files... >> "%LOG_FILE%"
set "ROOT_REQUIREMENTS=requirments.txt"
if not exist "%ROOT_REQUIREMENTS%" (
    set "ROOT_REQUIREMENTS=requirements.txt"
)

set "NEED_ROOT_INSTALL=0"
if exist "%ROOT_REQUIREMENTS%" (
    echo [INFO] Checking root requirements from %ROOT_REQUIREMENTS%...
    echo [INFO] Found root requirements: %ROOT_REQUIREMENTS% >> "%LOG_FILE%"
    
    :: More robust check - use importlib.util to check for module availability
    echo [INFO] Running module check for root requirements... >> "%LOG_FILE%"
    set "CHECK_RESULT=0"
    python -c "import sys; import importlib.util; modules = ['pandas', 'openpyxl', 'selenium', 'requests', 'tqdm']; sys.exit(0 if all(importlib.util.find_spec(m) for m in modules) else 1)" 2>>"%LOG_FILE%" || set "CHECK_RESULT=1"
    if !CHECK_RESULT! equ 1 (
        set "NEED_ROOT_INSTALL=1"
        echo [INFO] Root modules not found, will install >> "%LOG_FILE%"
    ) else (
        echo [INFO] Root modules already present >> "%LOG_FILE%"
    )
    
    if !NEED_ROOT_INSTALL! equ 1 (
        echo [INFO] Installing root requirements from %ROOT_REQUIREMENTS%...
        echo [INFO] Installing root requirements... >> "%LOG_FILE%"
        pip install -r "%ROOT_REQUIREMENTS%" --timeout 60 --default-timeout=60 2>>"%LOG_FILE%"
        if errorlevel 1 (
            echo [WARNING] First installation attempt failed. Trying again with no-cache...
            echo [WARNING] First attempt failed, retrying... >> "%LOG_FILE%"
            pip install -r "%ROOT_REQUIREMENTS%" --no-cache-dir --timeout 60 2>>"%LOG_FILE%"
            if errorlevel 1 (
                echo [ERROR] Failed to install root requirements from %ROOT_REQUIREMENTS%!
                echo [ERROR] Please check your internet connection or try again later.
                echo [ERROR] Failed to install root requirements! >> "%LOG_FILE%"
                pause
                exit /b 1
            )
        )
        echo [SUCCESS] Root requirements installed from %ROOT_REQUIREMENTS%.
        echo [SUCCESS] Root requirements installed >> "%LOG_FILE%"
    ) else (
        echo [INFO] Root requirements already installed. Skipping installation.
        echo [INFO] Root requirements already installed >> "%LOG_FILE%"
    )
) else (
    echo [WARNING] Root requirements file not found (checked requirments.txt and requirements.txt)
    echo [WARNING] No root requirements file found >> "%LOG_FILE%"
)

:: Check and install backend requirements.txt
echo. >> "%LOG_FILE%"
echo [CHECK] Backend requirements section starting... >> "%LOG_FILE%"
set "BACKEND_REQUIREMENTS=backend\requirements.txt"
set "NEED_BACKEND_INSTALL=0"
if exist "%BACKEND_REQUIREMENTS%" (
    echo [INFO] Checking backend requirements from %BACKEND_REQUIREMENTS%...
    echo [INFO] Checking backend requirements from %BACKEND_REQUIREMENTS%... >> "%LOG_FILE%"
    
    :: More robust check - use a temporary file to capture the result
    echo [INFO] Running module check for fastapi and uvicorn... >> "%LOG_FILE%"
    set "CHECK_RESULT=0"
    python -c "import sys; import importlib.util; sys.exit(0 if all(importlib.util.find_spec(m) for m in ['fastapi', 'uvicorn']) else 1)" 2>>"%LOG_FILE%" || set "CHECK_RESULT=1"
    if !CHECK_RESULT! equ 1 (
        set "NEED_BACKEND_INSTALL=1"
        echo [INFO] Backend modules not found, will install >> "%LOG_FILE%"
    ) else (
        echo [INFO] Backend modules already present >> "%LOG_FILE%"
    )
    
    if !NEED_BACKEND_INSTALL! equ 1 (
        echo [INFO] Installing backend requirements from %BACKEND_REQUIREMENTS%...
        echo [INFO] Installing backend requirements... >> "%LOG_FILE%"
        echo [INFO] This may take several minutes. Please wait... >> "%LOG_FILE%"
        pip install -r "%BACKEND_REQUIREMENTS%" --timeout 60 --default-timeout=60 2>>"%LOG_FILE%"
        if errorlevel 1 (
            echo [WARNING] First installation attempt failed. Trying again with no-cache...
            echo [WARNING] First attempt failed, retrying with no-cache... >> "%LOG_FILE%"
            pip install -r "%BACKEND_REQUIREMENTS%" --no-cache-dir --timeout 60 2>>"%LOG_FILE%"
            if errorlevel 1 (
                echo [ERROR] Failed to install backend requirements from %BACKEND_REQUIREMENTS%!
                echo [ERROR] Please check your internet connection or try again later.
                echo [ERROR] Failed to install backend requirements! >> "%LOG_FILE%"
                pause
                exit /b 1
            )
        )
        echo [SUCCESS] Backend requirements installed from %BACKEND_REQUIREMENTS%.
        echo [SUCCESS] Backend requirements installed >> "%LOG_FILE%"
    ) else (
        echo [INFO] Backend requirements already installed. Skipping installation.
        echo [INFO] Backend requirements already installed >> "%LOG_FILE%"
    )
) else (
    echo [WARNING] Backend requirements file not found at %BACKEND_REQUIREMENTS%
    echo [WARNING] Backend requirements file not found >> "%LOG_FILE%"
)
echo [COMPLETE] Backend requirements section completed >> "%LOG_FILE%"

:: ============================================
:: Install Frontend Dependencies  
:: ============================================
echo.
echo [STEP 3/4] Checking Frontend Dependencies...
echo. >> "%LOG_FILE%"
echo [STEP 3/4] Checking Frontend Dependencies... >> "%LOG_FILE%"

set "FRONTEND_DIR=frontend\frontend"
echo [DEBUG] FRONTEND_DIR set to: %FRONTEND_DIR% >> "%LOG_FILE%"
echo [DEBUG] Current directory: %CD% >> "%LOG_FILE%"

:: Check for package.json existence
set "PKG_JSON_EXISTS=0"
if exist "%FRONTEND_DIR%\package.json" set "PKG_JSON_EXISTS=1"
echo [DEBUG] package.json existence check result: %PKG_JSON_EXISTS% >> "%LOG_FILE%"

:: Use goto to avoid complex if block nesting
if !PKG_JSON_EXISTS! equ 1 goto :FRONTEND_SETUP
goto :FRONTEND_SKIP

:FRONTEND_SETUP
echo [INFO] Frontend package.json found >> "%LOG_FILE%"
echo [INFO] Changing to frontend directory... >> "%LOG_FILE%"

cd /d "%FRONTEND_DIR%"
echo [DEBUG] Changed to: %CD% >> "%LOG_FILE%"

echo [INFO] Checking for node_modules... >> "%LOG_FILE%"
if exist "node_modules" goto :FRONTEND_MODULES_EXIST

echo [INFO] Installing node_modules... >> "%LOG_FILE%"
echo [INFO] Installing frontend dependencies (this may take a few minutes)...
echo [INFO] Please be patient, this step can take 5-10 minutes...
echo [INFO] Installing frontend dependencies... >> "%LOG_FILE%"

call npm install --loglevel=error --prefer-offline
if errorlevel 1 (
    echo [WARNING] First npm install failed, retrying with legacy-peer-deps... >> "%LOG_FILE%"
    call npm install --legacy-peer-deps --loglevel=error --prefer-offline
    if errorlevel 1 (
        echo [ERROR] Failed to install frontend dependencies! >> "%LOG_FILE%"
        echo [ERROR] Failed to install frontend dependencies!
        cd /d "%SCRIPT_DIR%"
        pause
        exit /b 1
    )
)
echo [SUCCESS] Frontend dependencies installed >> "%LOG_FILE%"
goto :FRONTEND_RETURN

:FRONTEND_MODULES_EXIST
echo [INFO] Frontend dependencies already installed >> "%LOG_FILE%"

:FRONTEND_RETURN
echo [INFO] Returning to script directory... >> "%LOG_FILE%"
cd /d "%SCRIPT_DIR%"
echo [DEBUG] Back to: %CD% >> "%LOG_FILE%"
echo [SUCCESS] Frontend dependencies check completed >> "%LOG_FILE%"
goto :FRONTEND_COMPLETE

:FRONTEND_SKIP
echo [WARNING] Frontend package.json not found >> "%LOG_FILE%"

:FRONTEND_COMPLETE
echo [COMPLETE] Frontend dependencies section completed >> "%LOG_FILE%"

:: ============================================
:: Start Backend and Frontend
:: ============================================
echo.
echo [STEP 4/4] Starting Backend and Frontend Services...
echo.
echo. >> "%LOG_FILE%"
echo [STEP 4/4] Starting Backend and Frontend Services... >> "%LOG_FILE%"

:: Create a PID file to track processes
set "PID_FILE=%TEMP%\awacs_pids.txt"
echo. > "%PID_FILE%"
echo [INFO] PID file created >> "%LOG_FILE%"

:: Start Backend in a new window
echo [INFO] Starting Backend Server (FastAPI on port 8000)...
echo [INFO] Starting Backend Server... >> "%LOG_FILE%"
start "AWACS Backend" cmd /k "cd /d "%SCRIPT_DIR%" && call venv\Scripts\activate.bat && cd backend && echo Backend Server Starting... && python main.py"
timeout /t 3 /nobreak >nul
echo [INFO] Backend window launched >> "%LOG_FILE%"

:: Start Frontend in a new window
echo [INFO] Starting Frontend Server (React on port 3000)...
echo [INFO] Starting Frontend Server... >> "%LOG_FILE%"
if exist "%FRONTEND_DIR%\package.json" (
    :: Build absolute path for frontend
    set "FRONTEND_ABS_PATH=%SCRIPT_DIR%%FRONTEND_DIR%"
    echo [DEBUG] Frontend absolute path: !FRONTEND_ABS_PATH! >> "%LOG_FILE%"
    start "AWACS Frontend" cmd /k "cd /d "!FRONTEND_ABS_PATH!" && echo Frontend Server Starting... && npm start"
    echo [INFO] Frontend window launched >> "%LOG_FILE%"
) else (
    echo [WARNING] Skipping frontend server - frontend directory not found >> "%LOG_FILE%"
    echo [WARNING] Skipping frontend server - frontend directory not found
)

:: Wait a moment for services to start
timeout /t 5 /nobreak >nul
echo [INFO] Waiting for services to initialize... >> "%LOG_FILE%"

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
echo. >> "%LOG_FILE%"
echo ========================================== >> "%LOG_FILE%"
echo [SUCCESS] Application Started Successfully! >> "%LOG_FILE%"
echo ========================================== >> "%LOG_FILE%"
echo Backend API: http://localhost:8000 >> "%LOG_FILE%"
echo Frontend App: http://localhost:3000 >> "%LOG_FILE%"
echo Startup completed at: %date% %time% >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

echo This window will remain open to monitor the services.
echo Press any key to close this window (services will continue running)...
pause >nul

endlocal
