@echo off
setlocal enabledelayedexpansion

:: Set colors for better user experience
color 0C
title AWACS - Stopping Application

echo.
echo ========================================
echo    AWACS AI Annotation Tool - Shutdown
echo ========================================
echo.

:: Stop Backend (Python/FastAPI processes)
echo [INFO] Stopping Backend Server...
taskkill /FI "WINDOWTITLE eq AWACS Backend*" /T /F >nul 2>&1
for /f "tokens=2" %%a in ('netstat -ano ^| findstr :8000 ^| findstr LISTENING') do (
    set "PID=%%a"
    if defined PID (
        taskkill /PID !PID! /F >nul 2>&1
        echo [INFO] Stopped process on port 8000 (PID: !PID!)
    )
)

:: Stop Frontend (Node.js/React processes)
echo [INFO] Stopping Frontend Server...
taskkill /FI "WINDOWTITLE eq AWACS Frontend*" /T /F >nul 2>&1
for /f "tokens=2" %%a in ('netstat -ano ^| findstr :3000 ^| findstr LISTENING') do (
    set "PID=%%a"
    if defined PID (
        taskkill /PID !PID! /F >nul 2>&1
        echo [INFO] Stopped process on port 3000 (PID: !PID!)
    )
)

:: Additional cleanup - kill any remaining Python processes related to the project
echo [INFO] Cleaning up any remaining processes...
for /f "tokens=2" %%a in ('tasklist /FI "IMAGENAME eq python.exe" /FO CSV ^| findstr /V "INFO:"') do (
    set "PID=%%a"
    set "PID=!PID:"=!"
    if defined PID (
        wmic process where "ProcessId=!PID!" get CommandLine 2>nul | findstr /i "main.py" >nul
        if !errorlevel! equ 0 (
            taskkill /PID !PID! /F >nul 2>&1
            echo [INFO] Stopped Python process (PID: !PID!)
        )
    )
)

:: Kill any remaining node processes related to react-scripts
for /f "tokens=2" %%a in ('tasklist /FI "IMAGENAME eq node.exe" /FO CSV ^| findstr /V "INFO:"') do (
    set "PID=%%a"
    set "PID=!PID:"=!"
    if defined PID (
        wmic process where "ProcessId=!PID!" get CommandLine 2>nul | findstr /i "react-scripts" >nul
        if !errorlevel! equ 0 (
            taskkill /PID !PID! /F >nul 2>&1
            echo [INFO] Stopped Node.js process (PID: !PID!)
        )
    )
)

echo.
echo ========================================
echo    Application Stopped Successfully!
echo ========================================
echo.
echo [INFO] All backend and frontend services have been stopped.
echo.
timeout /t 2 /nobreak >nul

endlocal

