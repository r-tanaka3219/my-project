@echo off
chcp 65001 > nul
setlocal enableextensions enabledelayedexpansion
title 在庫管理システム - Windowsサービス登録

echo.
echo  ============================================================
echo   在庫管理システム Windowsサービス登録（NSSM使用）
echo  ============================================================
echo.

rem ── 管理者権限チェック ──────────────────────────────────────
net session >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] 管理者として実行してください。
    echo  このファイルを右クリック → "管理者として実行"
    pause & exit /b 1
)

set "APP=%~dp0"
if "%APP:~-1%"=="\" set "APP=%APP:~0,-1%"
set "SVC=InventorySystem"
set "NSSM=%APP%\nssm\nssm.exe"
set "LOGS=%APP%\logs"

rem ── nssm.exe チェック ────────────────────────────────────────
if not exist "%NSSM%" (
    echo  [ERROR] nssm\nssm.exe が見つかりません。
    echo.
    echo  【配置手順】
    echo   1. https://nssm.cc/download を開く
    echo   2. nssm-x.x.x.zip をダウンロード・解凍
    echo   3. win64\nssm.exe を %APP%\nssm\nssm.exe にコピー
    echo.
    start https://nssm.cc/download
    pause & exit /b 1
)

rem ── Python 自動検出（全ユーザー・全バージョン）──────────────
echo  Python を検索中...
set "PY="
for %%P in (
    "%LOCALAPPDATA%\Python\pythoncore-3.14-64\python.exe"
    "%LOCALAPPDATA%\Python\pythoncore-3.13-64\python.exe"
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
    for /f "delims=" %%X in ('where python 2^>nul') do ( set "PY=%%X" & goto :found_py )
)
echo  [ERROR] Python が見つかりません。setup.bat を先に実行してください。
pause & exit /b 1

:found_py
for /f "tokens=2" %%V in ('"%PY%" --version 2^>^&1') do set "PY_VER=%%V"
echo  Python %PY_VER%: %PY%

rem ── .env チェック ────────────────────────────────────────────
if not exist "%APP%\.env" (
    echo  [ERROR] .env が見つかりません。setup.bat を先に実行してください。
    pause & exit /b 1
)

rem ── ポート番号取得 ───────────────────────────────────────────
set "PORT=5000"
for /f "usebackq tokens=1,* delims==" %%A in ("%APP%\.env") do (
    if /I "%%~A"=="PORT" set "PORT=%%~B"
)

rem ── ログフォルダ作成 ─────────────────────────────────────────
if not exist "%LOGS%" mkdir "%LOGS%"

rem ── 既存サービス削除 ─────────────────────────────────────────
"%NSSM%" status "%SVC%" >nul 2>&1
if not errorlevel 1 (
    echo  既存サービスを停止・削除しています...
    "%NSSM%" stop   "%SVC%" >nul 2>&1
    "%NSSM%" remove "%SVC%" confirm >nul 2>&1
    timeout /t 2 /nobreak >nul
)

rem ── サービス登録 ─────────────────────────────────────────────
echo  サービスを登録しています...
"%NSSM%" install       "%SVC%"  "%PY%"  "%APP%\app.py"
"%NSSM%" set "%SVC%"   DisplayName     "在庫管理システム (InventorySystem)"
"%NSSM%" set "%SVC%"   Description     "在庫管理システム Flask/waitress サービス"
"%NSSM%" set "%SVC%"   AppDirectory    "%APP%"
"%NSSM%" set "%SVC%"   Start           SERVICE_DELAYED_AUTO_START
"%NSSM%" set "%SVC%"   AppExit         Default Restart
"%NSSM%" set "%SVC%"   AppRestartDelay 10000
"%NSSM%" set "%SVC%"   AppStdout       "%LOGS%\stdout.log"
"%NSSM%" set "%SVC%"   AppStderr       "%LOGS%\stderr.log"
"%NSSM%" set "%SVC%"   AppRotateFiles  1
"%NSSM%" set "%SVC%"   AppRotateSeconds 86400
"%NSSM%" set "%SVC%"   AppRotateBytes  10485760

rem ── サービス開始 ─────────────────────────────────────────────
echo  サービスを起動しています...
"%NSSM%" start "%SVC%"
timeout /t 5 /nobreak >nul
"%NSSM%" status "%SVC%"

echo.
echo  ============================================================
echo   サービス登録完了！
echo   サービス名 : %SVC%
echo   Python     : %PY_VER%
echo   アプリ場所 : %APP%
echo   ログ場所   : %LOGS%
echo   アクセスURL: http://localhost:%PORT%
echo   管理       : services.msc
echo  ============================================================
echo.
echo  ※ PC再起動後も自動で起動します（遅延自動起動）。
echo.
start "" "http://localhost:%PORT%"
pause
