@echo off
REM Setup script for Data Pack Tool
REM This will create a virtual environment and install dependencies

echo ========================================
echo Data Pack Tool - Environment Setup
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.9 or later from https://www.python.org/
    pause
    exit /b 1
)

echo Python found:
python --version
echo.

REM Check Python version (must be 3.9+)
python -c "import sys; exit(0 if sys.version_info >= (3, 9) else 1)" >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python 3.9 or later is required
    echo Please upgrade your Python installation
    pause
    exit /b 1
)

echo Creating virtual environment...
if exist venv (
    echo Virtual environment already exists. Removing old one...
    rmdir /s /q venv
)

python -m venv venv
if errorlevel 1 (
    echo ERROR: Failed to create virtual environment
    pause
    exit /b 1
)

echo.
echo Activating virtual environment...
call venv\Scripts\activate.bat

echo.
echo Upgrading pip...
python -m pip install --upgrade pip

echo.
echo Installing dependencies...
pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)

echo.
echo ========================================
echo Setup complete!
echo ========================================
echo.
echo To use the Data Pack Tool:
echo   1. Run: run_DataPackTool.bat
echo      OR
echo   2. Activate the environment manually:
echo      venv\Scripts\activate
echo      python df_readAndmap.py
echo.
pause
