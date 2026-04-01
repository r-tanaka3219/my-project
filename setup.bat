@echo off
chcp 65001 > nul
setlocal enableextensions enabledelayedexpansion
title 在庫管理システム - セットアップ

set "APP=%~dp0"
if "%APP:~-1%"=="\" set "APP=%APP:~0,-1%"

echo.
echo  ============================================================
echo   在庫管理システム  初回セットアップ
echo  ============================================================
echo.
echo  このスクリプトは以下を自動実行します:
echo    1. Python バージョン確認
echo    2. 必要パッケージのインストール
echo    3. 設定ファイル (.env) の作成
echo    4. データベース初期化
echo.
pause

rem ── [1/4] Python 検索 ─────────────────────────────────────────
echo.
echo  [1/4] Python を確認中...
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
    echo.
    echo  [ERROR] Python が見つかりません。
    echo  以下よりインストール後、再実行してください:
    echo    https://www.python.org/downloads/
    echo  ※ インストール時 "Add Python to PATH" にチェックを忘れずに！
    echo.
    pause & exit /b 1
)

for /f "tokens=2" %%V in ('"!PY!" --version 2^>^&1') do set "PY_VER=%%V"
echo   Python !PY_VER! : !PY!

for /f "tokens=1,2 delims=." %%A in ("!PY_VER!") do (
    set "PY_MAJOR=%%A"
    set "PY_MINOR=%%B"
)
if !PY_MAJOR! LSS 3 ( echo  [ERROR] Python 3.10 以上が必要です。 & pause & exit /b 1 )
if !PY_MAJOR! EQU 3 if !PY_MINOR! LSS 10 (
    echo  [ERROR] Python 3.10 以上が必要です。現在: !PY_VER!
    pause & exit /b 1
)

rem ── [2/4] パッケージインストール ──────────────────────────────
echo.
echo  [2/4] 必要パッケージをインストール中...
echo  （初回は数分かかる場合があります）
echo.
"!PY!" -m pip install --upgrade pip --quiet
"!PY!" -m pip install -r "%APP%\requirements.txt"
if errorlevel 1 (
    echo.
    echo  [ERROR] パッケージのインストールに失敗しました。
    echo  ネットワーク接続を確認し、再度実行してください。
    echo  手動実行: python -m pip install -r requirements.txt
    echo.
    pause & exit /b 1
)
echo   パッケージのインストール完了

rem ── [3/4] .env 作成 ──────────────────────────────────────────
echo.
echo  [3/4] 設定ファイル (.env) を作成します。
echo.

if exist "%APP%\.env" (
    echo  既存の .env ファイルが見つかりました。
    set /p "OW=  上書きしますか？ [y/N]: "
    if /i not "!OW!"=="y" goto :skip_env
    echo.
)

echo  ┌─ PostgreSQL 接続設定 ─────────────────────────────────────
echo  │ （後から .env を直接編集して変更できます）
echo  └────────────────────────────────────────────────────────────
echo.
set /p "PG_HOST=  DB サーバーアドレス  [localhost]: "
if "!PG_HOST!"=="" set "PG_HOST=localhost"

set /p "PG_PORT=  DB ポート番号        [5432]: "
if "!PG_PORT!"=="" set "PG_PORT=5432"

set /p "PG_DBNAME=  データベース名     [inventory]: "
if "!PG_DBNAME!"=="" set "PG_DBNAME=inventory"

set /p "PG_USER=  DB ユーザー名        [inventory_user]: "
if "!PG_USER!"=="" set "PG_USER=inventory_user"

set /p "PG_PASSWORD=  DB パスワード: "
if "!PG_PASSWORD!"=="" set "PG_PASSWORD=inventory_pass"

echo.
set /p "APP_PORT=  Web ポート番号      [5000]: "
if "!APP_PORT!"=="" set "APP_PORT=5000"

echo.
echo  ┌─ メール送信設定（省略可・後から変更可）────────────────────
echo  └────────────────────────────────────────────────────────────
set "MAIL_SERVER="
set "MAIL_PORT=25"
set "MAIL_USE_TLS=False"
set "MAIL_AUTH=False"
set "MAIL_USERNAME="
set "MAIL_PASSWORD="
set "MAIL_FROM="
set /p "MAIL_SERVER=  SMTP サーバー   [空白でスキップ]: "
if not "!MAIL_SERVER!"=="" (
    set /p "MAIL_PORT=  SMTP ポート         [587]: "
    if "!MAIL_PORT!"=="" set "MAIL_PORT=587"
    set /p "MAIL_USE_TLS=  TLS 使用 (True/False) [True]: "
    if "!MAIL_USE_TLS!"=="" set "MAIL_USE_TLS=True"
    set /p "MAIL_AUTH=  認証あり (True/False) [True]: "
    if "!MAIL_AUTH!"=="" set "MAIL_AUTH=True"
    set /p "MAIL_USERNAME=  メールアカウント: "
    set /p "MAIL_PASSWORD=  メールパスワード: "
    set /p "MAIL_FROM=  送信元アドレス: "
)

rem SECRET_KEY を Python で生成
for /f "delims=" %%K in ('"!PY!" -c "import secrets; print(secrets.token_hex(32))"') do set "SK=%%K"

(
echo SECRET_KEY=!SK!
echo PG_HOST=!PG_HOST!
echo PG_PORT=!PG_PORT!
echo PG_DBNAME=!PG_DBNAME!
echo PG_USER=!PG_USER!
echo PG_PASSWORD=!PG_PASSWORD!
echo PORT=!APP_PORT!
echo USE_WAITRESS=1
echo MAIL_SERVER=!MAIL_SERVER!
echo MAIL_PORT=!MAIL_PORT!
echo MAIL_USE_TLS=!MAIL_USE_TLS!
echo MAIL_USE_SSL=False
echo MAIL_AUTH=!MAIL_AUTH!
echo MAIL_USERNAME=!MAIL_USERNAME!
echo MAIL_PASSWORD=!MAIL_PASSWORD!
echo MAIL_FROM=!MAIL_FROM!
echo MAIL_FROM_NAME=在庫管理システム
echo DAILY_MAIL_HOUR=8
echo DAILY_MAIL_MINUTE=0
echo MONTH_END_IMPORT_HOUR=5
echo MONTH_END_IMPORT_MINUTE=0
) > "%APP%\.env"
echo.
echo   .env を作成しました

:skip_env

rem ── [4/4] DB 初期化 ──────────────────────────────────────────
echo.
echo  [4/4] データベースを初期化中...
echo  ※ PostgreSQL サービスが起動していることを確認してください
echo.
"!PY!" "%APP%\create_db.py"
if errorlevel 1 (
    echo.
    echo  [ERROR] DB 初期化に失敗しました。
    echo  確認事項:
    echo    ・PostgreSQL が起動しているか
    echo    ・.env の PG_HOST / PG_USER / PG_PASSWORD が正しいか
    echo  再試行: python create_db.py
    echo.
    pause & exit /b 1
)

rem ── 完了 ────────────────────────────────────────────────────
echo.
echo  ============================================================
echo   セットアップ完了！
echo  ============================================================
echo.
echo   【手動起動】
echo     start.bat をダブルクリック
echo.
echo   【本番運用（Windows サービス化）】
echo     install_service.bat を右クリック→管理者として実行
echo.
echo   アクセス URL:
echo     http://localhost:!APP_PORT!
echo.
echo   初期ログイン:
echo     ユーザー名: admin
echo     パスワード: admin
echo     ※ ログイン後すぐにパスワードを変更してください
echo.
echo  ============================================================
echo.
set /p "ST=  今すぐ起動しますか？ [Y/n]: "
if /i not "!ST!"=="n" start "" "%APP%\start.bat"
pause
