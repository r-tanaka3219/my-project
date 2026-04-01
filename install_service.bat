@echo off
chcp 65001 > nul
setlocal enableextensions enabledelayedexpansion
title 在庫管理システム - Windows サービス登録

echo.
echo  ============================================================
echo   在庫管理システム  Windows サービス登録
echo   ※ PC 起動時に自動起動されるようになります
echo  ============================================================
echo.

rem ── 管理者権限確認 ──────────────────────────────────────────
net session >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] このスクリプトは管理者として実行する必要があります。
    echo  右クリック → 「管理者として実行」を選択してください。
    echo.
    pause & exit /b 1
)

set "APP=%~dp0"
if "%APP:~-1%"=="\" set "APP=%APP:~0,-1%"
set "SVC=InventorySystem"
set "NSSM=%APP%\nssm\nssm.exe"
set "LOGS=%APP%\logs"

rem ── nssm.exe 確認 ────────────────────────────────────────────
if not exist "%NSSM%" (
    echo  [ERROR] nssm\nssm.exe が見つかりません。
    echo.
    echo  【ダウンロード手順】
    echo    1. https://nssm.cc/download を開く
    echo    2. nssm-x.x.x.zip をダウンロード・解凍
    echo    3. win64\nssm.exe を以下へコピー:
    echo       %APP%\nssm\nssm.exe
    echo.
    start https://nssm.cc/download
    pause & exit /b 1
)

rem ── .env 確認 ─────────────────────────────────────────────────
if not exist "%APP%\.env" (
    echo  [ERROR] .env が見つかりません。先に setup.bat を実行してください。
    pause & exit /b 1
)

rem ── Python 検索 ──────────────────────────────────────────────
echo  Python を検索中...
set "PY="
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
    if exist %%~P ( set "PY=%%~P" & goto :found_py )
)
where python >nul 2>&1
if not errorlevel 1 (
    for /f "delims=" %%X in ('where python 2^>nul') do (
        if "!PY!"=="" set "PY=%%X" & goto :found_py
    )
)
echo  [ERROR] Python が見つかりません。
pause & exit /b 1

:found_py
for /f "tokens=2" %%V in ('"!PY!" --version 2^>^&1') do set "PY_VER=%%V"
echo   Python !PY_VER! : !PY!

rem ── PORT 取得 ─────────────────────────────────────────────────
set "PORT=5000"
for /f "usebackq tokens=1,* delims==" %%A in ("%APP%\.env") do (
    if /i "%%~A"=="PORT" set "PORT=%%~B"
)

rem ── ログフォルダ作成 ─────────────────────────────────────────
if not exist "%LOGS%" mkdir "%LOGS%"

rem ── 既存サービス削除 ─────────────────────────────────────────
"%NSSM%" status "%SVC%" >nul 2>&1
if not errorlevel 1 (
    echo  既存サービスを停止・削除中...
    "%NSSM%" stop   "%SVC%" >nul 2>&1
    "%NSSM%" remove "%SVC%" confirm >nul 2>&1
    timeout /t 3 /nobreak >nul
)

rem ── サービス登録 ─────────────────────────────────────────────
echo  サービスを登録中...
"%NSSM%" install       "%SVC%"  "!PY!"  "%APP%\app.py"
"%NSSM%" set "%SVC%"   DisplayName      "在庫管理システム (InventorySystem)"
"%NSSM%" set "%SVC%"   Description      "在庫管理システム Flask/Waitress サービス"
"%NSSM%" set "%SVC%"   AppDirectory     "%APP%"
"%NSSM%" set "%SVC%"   Start            SERVICE_DELAYED_AUTO_START
"%NSSM%" set "%SVC%"   AppExit          Default Restart
"%NSSM%" set "%SVC%"   AppRestartDelay  10000
"%NSSM%" set "%SVC%"   AppStdout        "%LOGS%\stdout.log"
"%NSSM%" set "%SVC%"   AppStderr        "%LOGS%\stderr.log"
"%NSSM%" set "%SVC%"   AppRotateFiles   1
"%NSSM%" set "%SVC%"   AppRotateSeconds 86400
"%NSSM%" set "%SVC%"   AppRotateBytes   10485760

rem ── サービス開始 ─────────────────────────────────────────────
echo  サービスを起動中...
"%NSSM%" start "%SVC%"
timeout /t 5 /nobreak >nul
"%NSSM%" status "%SVC%"

echo.
echo  ============================================================
echo   サービス登録完了
echo  ============================================================
echo   サービス名: !SVC!
echo   Python    : !PY_VER!
echo   アプリ    : %APP%
echo   ログ      : %LOGS%
echo   URL       : http://localhost:!PORT!
echo.
echo   管理: services.msc を開いて "InventorySystem" を確認
echo   ログ: service_log.bat で確認できます
echo.
echo   PC を再起動しても自動起動します。
echo  ============================================================
echo.
start "" "http://localhost:!PORT!"
pause
