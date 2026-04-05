@echo off
setlocal enableextensions enabledelayedexpansion
chcp 65001 > nul
title DB Migration

set "APP=%~dp0"
if "%APP:~-1%"=="\" set "APP=%APP:~0,-1%"

echo.
echo  ============================================================
echo   DB Migration - Column Add
echo  ============================================================
echo.

rem -- Python検索
set "PY="
for %%V in (314 313 312 311 310 39) do (
    if "!PY!"=="" if exist "%LOCALAPPDATA%\Programs\Python\Python%%V\python.exe" (
        set "PY=%LOCALAPPDATA%\Programs\Python\Python%%V\python.exe"
    )
)
if "!PY!"=="" (
    for /f "delims=" %%X in ('where python 2^>nul') do (
        if "!PY!"=="" set "PY=%%X"
    )
)
if "!PY!"=="" (
    echo  [ERROR] Python not found.
    pause & exit /b 1
)

echo  Python: !PY!
echo.

cd /d "%APP%"
"!PY!" "_run_migrate.py"

if errorlevel 1 (
    echo.
    echo  [FAILED] Check .env DB settings.
) else (
    echo.
    echo  ============================================================
    echo   Done! Please restart InventorySystem service.
    echo.
    echo   Admin CMD:
    echo     net stop InventorySystem
    echo     net start InventorySystem
    echo  ============================================================
)
echo.
pause
