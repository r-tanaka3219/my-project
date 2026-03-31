@echo off
chcp 65001 > nul
echo 在庫管理システムを停止しています...
taskkill /f /im python.exe >nul 2>&1
taskkill /f /im python3.exe >nul 2>&1
echo 停止しました。
pause
