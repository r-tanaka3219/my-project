@echo off
chcp 932 > nul
setlocal enableextensions enabledelayedexpansion
title 在庫管理システム

set "APP=%~dp0"
if "%APP:~-1%"=="\" set "APP=%APP:~0,-1%"

echo.
echo  ============================================================
echo   在庫管理システム 起動中...
echo  ============================================================
echo.

if not exist "%APP%\app.py" (
    echo  [ERROR] app.py が見つかりません: %APP%\app.py
    pause & exit /b 1
)

rem ── Python 自動検出 ──────────────────────────────────────────
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
        "C:\Python314\python.exe"
        "C:\Python313\python.exe"
        "C:\Python312\python.exe"
        "C:\Python311\python.exe"
        "C:\Python310\python.exe"
    ) do (
        if exist %%~P (
            if "!PY!"=="" set "PY=%%~P"
        )
    )
)

if "!PY!"=="" (
    echo  [ERROR] Python が見つかりません。setup.bat を実行してください。
    pause & exit /b 1
)

echo  Python: !PY!
cd /d "%APP%"

rem ── .env チェック ────────────────────────────────────────────
if not exist "%APP%\.env" (
    echo  [ERROR] .env ファイルがありません。setup.bat を先に実行してください。
    pause & exit /b 1
)

rem ── パッケージ確認 ────────────────────────────────────────────
"!PY!" -c "import flask, dotenv, psycopg2, openpyxl, apscheduler, waitress" >nul 2>&1
if errorlevel 1 (
    echo  必要パッケージが不足しています。インストールします...
    "!PY!" -m pip install --upgrade pip --quiet
    "!PY!" -m pip install -r "%APP%\requirements.txt"
    if errorlevel 1 (
        echo  [ERROR] パッケージインストール失敗。ネットワーク接続を確認してください。
        pause & exit /b 1
    )
)

rem ── ポート番号を .env から取得 ─────────────────────────────
set "PORT=5000"
for /f "usebackq tokens=1,* delims==" %%A in ("%APP%\.env") do (
    if /I "%%~A"=="PORT" set "PORT=%%~B"
)

rem ── サーバー起動 ──────────────────────────────────────────────
echo  サーバーを起動しています (ポート: !PORT!)...
echo  この画面を閉じるとシステムが停止します。
echo.
start "InventoryServer" cmd /k "cd /d "%APP%" ^&^& "!PY!" app.py"

rem ── 起動確認（最大30秒待機）──────────────────────────────────
for /L %%I in (1,1,30) do (
    powershell -NoProfile -ExecutionPolicy Bypass -Command "try{Invoke-WebRequest -Uri 'http://127.0.0.1:!PORT!' -UseBasicParsing -TimeoutSec 2 ^| Out-Null; exit 0}catch{exit 1}" >nul 2>&1
    if not errorlevel 1 goto :ready
    timeout /t 1 /nobreak >nul
    echo  起動待機中... %%I/30秒
)
echo  [WARN] 30秒以内に応答なし。直接アクセスしてください。
echo  URL: http://localhost:!PORT!
pause & exit /b 0

:ready
echo.
echo  起動完了！ブラウザを開いています...
start "" "http://localhost:!PORT!"
pause
