@echo off
echo.
echo  [Inventory System] Stopping...
taskkill /f /im python.exe    2>NUL
taskkill /f /im python3.exe   2>NUL
taskkill /f /im python3.9.exe  2>NUL
taskkill /f /im python3.10.exe 2>NUL
taskkill /f /im python3.11.exe 2>NUL
taskkill /f /im python3.12.exe 2>NUL
taskkill /f /im python3.13.exe 2>NUL
echo  [Inventory System] Stopped.
echo.
pause
