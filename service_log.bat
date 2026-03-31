@echo off
chcp 65001 > nul
set "APP=%~dp0"
if "%APP:~-1%"=="\" set "APP=%APP:~0,-1%"
set "LOGS=%APP%\logs"

echo ── stdout.log（最新50行）──────────────────────────────────
if exist "%LOGS%\stdout.log" (
    powershell -NoProfile -Command "Get-Content '%LOGS%\stdout.log' -Tail 50"
) else ( echo  ログファイルがありません: %LOGS%\stdout.log )

echo.
echo ── stderr.log（最新20行）──────────────────────────────────
if exist "%LOGS%\stderr.log" (
    powershell -NoProfile -Command "Get-Content '%LOGS%\stderr.log' -Tail 20"
) else ( echo  エラーログなし )
echo.
pause
