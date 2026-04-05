@echo off
chcp 65001 > nul
title DB マイグレーション実行

set "APP=%~dp0"
if "%APP:~-1%"=="\" set "APP=%APP:~0,-1%"

echo.
echo  ============================================================
echo   在庫管理システム  DBマイグレーション実行
echo   (カラム追加・テーブル追加を適用します)
echo  ============================================================
echo.

rem -- Python検索
set "PY="
for %%V in (314 313 312 311 310 39) do (
    if "!PY!"=="" (
        if exist "%LOCALAPPDATA%\Programs\Python\Python%%V\python.exe" (
            set "PY=%LOCALAPPDATA%\Programs\Python\Python%%V\python.exe"
        )
    )
)
if "!PY!"=="" (
    for /f "delims=" %%X in ('where python 2^>nul') do (
        if "!PY!"=="" set "PY=%%X"
    )
)
if "!PY!"=="" (
    echo  [ERROR] Pythonが見つかりません。
    pause & exit /b 1
)

echo  Python: %PY%
echo.
echo  マイグレーション実行中...
echo.

cd /d "%APP%"
"%PY%" -c "
import sys
sys.path.insert(0, '.')
try:
    from database import migrate_db
    migrate_db()
    print('  [OK] マイグレーション完了！')
    print('  サービスを再起動してください。')
except Exception as e:
    print('  [ERROR]', e)
    sys.exit(1)
"

if errorlevel 1 (
    echo.
    echo  [FAILED] エラーが発生しました。
    echo  .envのDB接続情報を確認してください。
) else (
    echo.
    echo  ============================================================
    echo   完了！ 次にサービスを再起動してください。
    echo.
    echo   管理者コマンドプロンプトで:
    echo     net stop InventorySystem
    echo     net start InventorySystem
    echo.
    echo   または services.msc から InventorySystem を再起動
    echo  ============================================================
)
echo.
pause
