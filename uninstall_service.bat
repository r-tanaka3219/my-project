@echo off
chcp 65001 > nul
setlocal
title 在庫管理システム - サービス削除

net session >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] 管理者として実行してください。
    pause & exit /b 1
)

set "APP=%~dp0"
if "%APP:~-1%"=="\" set "APP=%APP:~0,-1%"
set "SVC=InventorySystem"
set "NSSM=%APP%\nssm\nssm.exe"

if not exist "%NSSM%" ( echo  [ERROR] nssm.exe が見つかりません。 & pause & exit /b 1 )

echo  サービスを停止・削除しています...
"%NSSM%" stop   "%SVC%" >nul 2>&1
"%NSSM%" remove "%SVC%" confirm
echo  完了。
pause
