@echo off
chcp 65001 > nul
set "APP=%~dp0"
if "%APP:~-1%"=="\" set "APP=%APP:~0,-1%"
set "LOGS=%APP%\logs"

echo.
echo  ============================================================
echo   在庫管理システム  ログ表示
echo  ============================================================
echo.

echo  ── stdout.log (最新 50 行) ──────────────────────────────────
if exist "%LOGS%\stdout.log" (
    powershell -NoProfile -Command "Get-Content '%LOGS%\stdout.log' -Tail 50"
) else (
    echo  ログファイルなし: %LOGS%\stdout.log
)

echo.
echo  ── stderr.log (最新 30 行) ──────────────────────────────────
if exist "%LOGS%\stderr.log" (
    powershell -NoProfile -Command "Get-Content '%LOGS%\stderr.log' -Tail 30"
) else (
    echo  エラーログなし
)

echo.
echo  ログフォルダ: %LOGS%
echo.
pause
