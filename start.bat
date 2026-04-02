@echo off
setlocal enableextensions enabledelayedexpansion
title Inventory System

set "APP=%~dp0"
if "%APP:~-1%"=="\" set "APP=%APP:~0,-1%"

echo.
echo  ============================================================
echo   Inventory System  Starting...
echo  ============================================================

rem -- app.py check
if not exist "%APP%\app.py" (
    echo  [ERROR] app.py not found.
    pause & exit /b 1
)

rem -- .env check
if not exist "%APP%\.env" (
    echo  [ERROR] .env not found. Run setup.bat first.
    pause & exit /b 1
)

rem -- Find Python
set "PY="
for %%V in (314 313 312 311 310 39) do (
    if "!PY!"=="" (
        if exist "%LOCALAPPDATA%\Programs\Python\Python%%V\python.exe" (
            set "PY=%LOCALAPPDATA%\Programs\Python\Python%%V\python.exe"
        )
    )
)
if "!PY!"=="" (
    where python >NUL 2>&1
    if not errorlevel 1 (
        for /f "delims=" %%X in ('where python 2^>NUL') do (
            if "!PY!"=="" set "PY=%%X"
        )
    )
)
if "!PY!"=="" (
    echo  [ERROR] Python not found. Run setup.bat first.
    pause & exit /b 1
)

rem -- Install / update packages from requirements.txt
echo  Checking packages...
"!PY!" -m pip install -r "%APP%\requirements.txt" --quiet --disable-pip-version-check --no-warn-script-location
if errorlevel 1 (
    echo  [ERROR] Package install failed. Check requirements.txt.
    pause & exit /b 1
)

rem -- Get PORT
set "PORT=5000"
for /f "usebackq tokens=1,* delims==" %%A in ("%APP%\.env") do (
    if /i "%%~A"=="PORT" set "PORT=%%~B"
)

echo  Python : !PY!
echo  Port   : !PORT!
echo.
echo  Press Ctrl+C or close this window to stop.
echo.
cd /d "%APP%"
"!PY!" app.py

echo.
echo  [Inventory System] Server stopped.
pause
