@echo off
chcp 932 > nul
setlocal enableextensions enabledelayedexpansion
title Inventory System Setup

echo.
echo  ============================================================
echo   在庫管理システム セットアップ
echo  ============================================================
echo.

set "APP=%~dp0"
if "%APP:~-1%"=="\" set "APP=%APP:~0,-1%"

rem [1/5] Python検索
echo  [1/5] Python を検索中...
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
    echo  [ERROR] Python 3.10以上が見つかりません。
    echo  https://www.python.org/downloads/
    pause
    exit /b 1
)

rem バージョン確認（一時ファイル経由）
"!PY!" --version > "%TEMP%\pyver.tmp" 2>&1
set /p PY_VER_LINE=<"%TEMP%\pyver.tmp"
del "%TEMP%\pyver.tmp" >nul 2>&1
for /f "tokens=2" %%V in ("!PY_VER_LINE!") do set "PY_VER=%%V"
echo  Python !PY_VER! : !PY!

for /f "tokens=1,2 delims=." %%A in ("!PY_VER!") do (
    if %%A LSS 3 ( echo [ERROR] Python 3.10以上必要 & pause & exit /b 1 )
    if %%A EQU 3 if %%B LSS 10 ( echo [ERROR] Python 3.10以上必要 & pause & exit /b 1 )
)

rem [2/5] パッケージインストール
echo.
echo  [2/5] パッケージをインストール中...
"!PY!" -m pip install --upgrade pip --quiet
"!PY!" -m pip install -r "%APP%\requirements.txt"
if errorlevel 1 (
    echo  [ERROR] パッケージインストール失敗。ネットワークを確認してください。
    pause
    exit /b 1
)
echo  パッケージインストール完了

rem [3/5] .env作成
echo.
echo  [3/5] 設定ファイル (.env) を作成します。
if exist "%APP%\.env" (
    echo  既存の .env が見つかりました。
    set /p "OW=  上書きしますか？ [y/N]: "
    if /i not "!OW!"=="y" goto :skip_env
)

set /p "PG_HOST=  PostgreSQLホスト [localhost]: "
if "!PG_HOST!"=="" set "PG_HOST=localhost"

set /p "PG_PORT=  PostgreSQLポート [5432]: "
if "!PG_PORT!"=="" set "PG_PORT=5432"

set /p "PG_DBNAME=  DB名 [inventory]: "
if "!PG_DBNAME!"=="" set "PG_DBNAME=inventory"

set /p "PG_USER=  DBユーザー [inventory_user]: "
if "!PG_USER!"=="" set "PG_USER=inventory_user"

set /p "PG_PASSWORD=  DBパスワード: "
if "!PG_PASSWORD!"=="" set "PG_PASSWORD=inventory_pass"

set /p "APP_PORT=  ポート番号 [5000]: "
if "!APP_PORT!"=="" set "APP_PORT=5000"

echo import secrets > "%TEMP%\gen_sk.py"
echo print(secrets.token_hex(32)) >> "%TEMP%\gen_sk.py"
for /f "delims=" %%K in ('"!PY!" "%TEMP%\gen_sk.py"') do set "SK=%%K"
del "%TEMP%\gen_sk.py" >nul 2>&1

(
echo SECRET_KEY=!SK!
echo PG_HOST=!PG_HOST!
echo PG_PORT=!PG_PORT!
echo PG_DBNAME=!PG_DBNAME!
echo PG_USER=!PG_USER!
echo PG_PASSWORD=!PG_PASSWORD!
echo PORT=!APP_PORT!
echo MAIL_SERVER=
echo MAIL_PORT=587
echo MAIL_USE_TLS=True
echo MAIL_USE_SSL=False
echo MAIL_AUTH=True
echo MAIL_USERNAME=
echo MAIL_PASSWORD=
echo MAIL_FROM=
echo MAIL_FROM_NAME=在庫管理システム
echo DAILY_MAIL_HOUR=8
echo DAILY_MAIL_MINUTE=0
echo MONTH_END_IMPORT_HOUR=5
echo MONTH_END_IMPORT_MINUTE=0
echo USE_WAITRESS=1
) > "%APP%\.env"
echo  .env を作成しました。

:skip_env

rem [4/5] DB初期化
echo.
echo  [4/5] データベース初期化中...
"!PY!" "%APP%\create_db.py"
if errorlevel 1 (
    echo  [ERROR] DB初期化失敗。PostgreSQLが起動しているか確認してください。
    pause
    exit /b 1
)
echo  DB初期化完了

rem [5/5] 完了
echo.
echo  ============================================================
echo   セットアップ完了！
echo   起動: start.bat をダブルクリック
echo   URL : http://localhost:!APP_PORT!
echo   初期アカウント: admin / admin
echo  ============================================================
echo.
set /p "SN=  今すぐ起動しますか？ [Y/n]: "
if /i not "!SN!"=="n" start "" "%APP%\start.bat"
pause
