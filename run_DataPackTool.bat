@echo off
REM Run the Data Pack Tool using the virtual environment

if not exist venv (
    echo ERROR: Virtual environment not found!
    echo Please run setup.bat first to create the environment
    pause
    exit /b 1
)

echo Activating virtual environment...
call venv\Scripts\activate.bat

echo.
echo Running Data Pack Tool...
echo ========================================
python df_readAndmap.py

echo.
echo ========================================
echo Processing complete!
echo.
pause
