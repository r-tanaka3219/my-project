@echo off
chcp 65001 > nul
setlocal
title 在庫管理システム - サービス削除

echo.
echo  ============================================================
echo   在庫管理システム  Windows サービス削除
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

if not exist "%NSSM%" (
    echo  [ERROR] nssm\nssm.exe が見つかりません。
    pause & exit /b 1
)

echo  サービス「%SVC%」を停止・削除します。
set /p "CONFIRM=  続けますか？ [y/N]: "
if /i not "%CONFIRM%"=="y" (
    echo  キャンセルしました。
    pause & exit /b 0
)

echo.
echo  サービスを停止中...
"%NSSM%" stop   "%SVC%" >nul 2>&1
timeout /t 3 /nobreak >nul
echo  サービスを削除中...
"%NSSM%" remove "%SVC%" confirm

echo.
echo  削除完了。サービスの自動起動は無効になりました。
echo  手動起動する場合は start.bat を実行してください。
echo.
pause
