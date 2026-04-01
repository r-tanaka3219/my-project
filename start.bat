@echo off
chcp 65001 > nul
setlocal enableextensions enabledelayedexpansion
title 在庫管理システム

set "APP=%~dp0"
if "%APP:~-1%"=="\" set "APP=%APP:~0,-1%"

echo.
echo  ============================================================
echo   在庫管理システム  起動中...
echo  ============================================================
echo.

rem ── app.py 確認 ──────────────────────────────────────────────
if not exist "%APP%\app.py" (
    echo  [ERROR] app.py が見つかりません: %APP%\app.py
    pause & exit /b 1
)

rem ── .env 確認 ─────────────────────────────────────────────────
if not exist "%APP%\.env" (
    echo  [ERROR] .env が見つかりません。先に setup.bat を実行してください。
    pause & exit /b 1
)

rem ── Python 検索 ──────────────────────────────────────────────
set "PY="
where python >nul 2>&1
if not errorlevel 1 (
    for /f "delims=" %%X in ('where python 2^>nul') do (
        if "!PY!"=="" set "PY=%%X"
    )
)
if "!PY!"=="" (
    for %%P in (
        "%LOCALAPPDATA%\Programs\Python\Python314\python.exe"
        "%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
        "%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
        "%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
        "%LOCALAPPDATA%\Programs\Python\Python310\python.exe"
        "%ProgramFiles%\Python314\python.exe"
        "%ProgramFiles%\Python313\python.exe"
        "%ProgramFiles%\Python312\python.exe"
        "%ProgramFiles%\Python311\python.exe"
        "%ProgramFiles%\Python310\python.exe"
    ) do (
        if exist %%~P (
            if "!PY!"=="" set "PY=%%~P"
        )
    )
)
if "!PY!"=="" (
    echo  [ERROR] Python が見つかりません。setup.bat を先に実行してください。
    pause & exit /b 1
)

rem ── 必須パッケージ確認（未インストールなら自動インストール）──
"!PY!" -c "import flask, dotenv, psycopg2, openpyxl, apscheduler, waitress" >nul 2>&1
if errorlevel 1 (
    echo  必要パッケージが不足しています。インストール中...
    "!PY!" -m pip install -r "%APP%\requirements.txt" --quiet
    if errorlevel 1 (
        echo  [ERROR] パッケージのインストールに失敗しました。
        pause & exit /b 1
    )
)

rem ── PORT 取得 ─────────────────────────────────────────────────
set "PORT=5000"
for /f "usebackq tokens=1,* delims==" %%A in ("%APP%\.env") do (
    if /i "%%~A"=="PORT" set "PORT=%%~B"
)

rem ── サーバー起動 ──────────────────────────────────────────────
echo  Python: !PY!
echo  ポート: !PORT!
echo  アプリ: %APP%\app.py
echo.
echo  停止するにはこのウィンドウを閉じてください。
echo.
cd /d "%APP%"
"!PY!" app.py

echo.
echo  サーバーが停止しました。
pause
