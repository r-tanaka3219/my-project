#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
在庫管理システム - セットアップ
ダブルクリックで実行してください
"""
import os, sys, shutil, subprocess, pathlib, winreg, ctypes

APP_NAME  = "在庫管理システム"
DEST_NAME = "inventory_system"

def pause(msg="続けるにはEnterを押してください..."):
    input(msg)

def ask_yn(msg, default="Y"):
    while True:
        ans = input(f"{msg} [{default}/N]: ").strip().upper() or default
        if ans in ("Y","N"): return ans == "Y"

def title(text):
    print("\n" + "=" * 60)
    print(f"  {text}")
    print("=" * 60)

def step(n, text):
    print(f"\n[{n}/5] {text}")

# ─── インストール先の決定 ───────────────────────────────────────
def choose_dest():
    default = pathlib.Path(os.environ["USERPROFILE"]) / DEST_NAME
    print(f"\n  インストール先: {default}")
    print("  ※ 管理者権限は不要です")
    if ask_yn("このフォルダにインストールしますか？", "Y"):
        return default
    print("\n  インストール先を入力してください")
    print(f"  例1: {default}  （推奨）")
    print("  例2: D:\\inventory_system")
    while True:
        p = input("  インストール先: ").strip().strip('"')
        if p:
            dest = pathlib.Path(p)
            if "Program Files" in str(dest):
                print("  ⚠ Program Files は管理者権限が必要なため推奨しません")
                if not ask_yn("  このまま続けますか？", "N"):
                    continue
            return dest

# ─── Python確認 ────────────────────────────────────────────────
def check_python():
    v = sys.version_info
    print(f"  Python {v.major}.{v.minor}.{v.micro} OK")
    if v.major < 3 or (v.major == 3 and v.minor < 9):
        print("  ⚠ Python 3.9以上を推奨します")
        print("  https://www.python.org/downloads/")

# ─── ファイルコピー ─────────────────────────────────────────────
def copy_files(src, dest):
    dest.mkdir(parents=True, exist_ok=True)
    here = pathlib.Path(src)
    skip = {".env", "inventory.db", "__pycache__", "setup.py"}
    count = 0
    for item in here.rglob("*"):
        if any(s in str(item) for s in skip): continue
        if item.suffix in (".pyc",): continue
        rel = item.relative_to(here)
        target = dest / rel
        if item.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, target)
            count += 1
    print(f"  OK: {count} ファイルをコピーしました → {dest}")

# ─── pip インストール ───────────────────────────────────────────
def install_libs():
    pkgs = ["flask", "python-dotenv", "openpyxl", "psycopg2-binary"]
    r = subprocess.run(
        [sys.executable, "-m", "pip", "install"] + pkgs + ["--quiet"],
        capture_output=True, text=True
    )
    if r.returncode == 0:
        print("  OK: ライブラリをインストールしました")
    else:
        print("  WARNING: 一部インストールに失敗しました")
        print(r.stderr[:300])

# ─── .env 作成 ─────────────────────────────────────────────────
def create_env(dest):
    print("\n  PostgreSQL接続情報を入力してください")
    print("  （後から .env ファイルを直接編集できます）\n")
    host   = input("  サーバーアドレス (例: localhost): ").strip() or "localhost"
    port   = input("  ポート番号      (例: 5432)      : ").strip() or "5432"
    dbname = input("  データベース名  (例: inventory)  : ").strip() or "inventory"
    user   = input("  ユーザー名      (例: inv_user)   : ").strip() or "inventory_user"
    passwd = input("  パスワード                       : ").strip()
    import random
    secret = f"inv-{random.randint(10000,99999)}{random.randint(10000,99999)}"
    env_path = dest / ".env"
    env_path.write_text(
        f"PG_HOST={host}\nPG_PORT={port}\nPG_DBNAME={dbname}\n"
        f"PG_USER={user}\nPG_PASSWORD={passwd}\n"
        f"SECRET_KEY={secret}\n"
        "MAIL_SERVER=\nMAIL_PORT=25\nMAIL_USE_TLS=False\n"
        "MAIL_USE_SSL=False\nMAIL_AUTH=False\n"
        "MAIL_USERNAME=\nMAIL_PASSWORD=\n"
        "MAIL_FROM=\nMAIL_FROM_NAME=在庫管理システム\n",
        encoding="utf-8"
    )
    print(f"  OK: .env を作成しました → {env_path}")

# ─── デスクトップショートカット作成 ────────────────────────────
def create_shortcut(dest):
    try:
        import winreg
        desktop_key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders"
        )
        desktop = pathlib.Path(winreg.QueryValueEx(desktop_key, "Desktop")[0])
        winreg.CloseKey(desktop_key)
    except Exception:
        desktop = pathlib.Path(os.environ["USERPROFILE"]) / "Desktop"

    # .ps1 ランチャーを経由する（Smart App Control 対策）
    launcher = dest / "start_server.ps1"
    lnk_path = desktop / "Inventory_Start.lnk"

    # WScriptのCOMを使ってショートカット作成
    try:
        import comtypes.client
        shell = comtypes.client.CreateObject("WScript.Shell")
        sc = shell.CreateShortcut(str(lnk_path))
        sc.TargetPath = str(launcher)
        sc.WorkingDirectory = str(dest)
        sc.Description = "Inventory System"
        sc.Save()
        print(f"  OK: ショートカット作成 → {lnk_path}")
    except Exception:
        # comtypes がない場合は場所だけ案内
        print(f"  INFO: 手動でショートカットを作成してください")
        print(f"        対象: {launcher}")

# ─── PowerShell ランチャー作成 ──────────────────────────────────
def create_ps1_launcher(dest):
    """start_server.ps1 - PowerShellで起動（BAT/VBS不要）"""
    ps1 = dest / "start_server.ps1"
    ps1.write_text(
        '# Inventory System Launcher\n'
        'Set-Location $PSScriptRoot\n'
        '$ip = (Get-NetIPAddress -AddressFamily IPv4 '
        '| Where-Object {$_.IPAddress -notmatch "^127"} '
        '| Select-Object -First 1).IPAddress\n'
        'Write-Host ""\n'
        'Write-Host "  Local : http://localhost:5000"\n'
        'Write-Host "  LAN   : http://${ip}:5000"\n'
        'Write-Host ""\n'
        'Start-Sleep 2\n'
        'Start-Process "http://localhost:5000"\n'
        'python app.py\n'
        'Write-Host ""\n'
        'Write-Host "Server stopped. Press Enter to exit."\n'
        'Read-Host\n',
        encoding="utf-8-sig"
    )
    print(f"  OK: start_server.ps1 を作成しました")

    # autostart 用 ps1
    auto = dest / "autostart.ps1"
    auto.write_text(
        '# Register auto-start task\n'
        '$taskName = "InventorySystem"\n'
        '$scriptPath = Join-Path $PSScriptRoot "start_server.ps1"\n'
        '$action = New-ScheduledTaskAction -Execute "powershell.exe" '
        '-Argument "-WindowStyle Hidden -File `"$scriptPath`""\n'
        '$trigger = New-ScheduledTaskTrigger -AtLogOn\n'
        '$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Hours 0)\n'
        'Register-ScheduledTask -TaskName $taskName -Action $action '
        '-Trigger $trigger -Settings $settings -RunLevel Highest -Force\n'
        'Write-Host "Auto-start registered: $taskName"\n'
        'Read-Host "Press Enter to close"\n',
        encoding="utf-8-sig"
    )
    # remove task ps1
    rm = dest / "remove_autostart.ps1"
    rm.write_text(
        '# Remove auto-start task\n'
        'Unregister-ScheduledTask -TaskName "InventorySystem" -Confirm:$false\n'
        'Write-Host "Auto-start removed."\n'
        'Read-Host "Press Enter to close"\n',
        encoding="utf-8-sig"
    )
    print(f"  OK: autostart.ps1 / remove_autostart.ps1 を作成しました")

# ─── メイン ────────────────────────────────────────────────────
def main():
    title("在庫管理システム - セットアップ")
    here = pathlib.Path(__file__).parent

    # 1. Python確認
    step(1, "Python確認")
    check_python()

    # 2. インストール先
    dest = choose_dest()

    # 3. ファイルコピー
    step(2, "ファイルのコピー")
    copy_files(here, dest)
    create_ps1_launcher(dest)

    # 4. ライブラリ
    step(3, "Pythonライブラリのインストール")
    install_libs()

    # 5. .env
    step(4, "PostgreSQL接続設定")
    create_env(dest)

    # 6. ショートカット
    step(5, "デスクトップショートカット")
    create_shortcut(dest)

    # 完了
    title("セットアップ完了！")
    print(f"""
  インストール先: {dest}

  【起動方法】
    デスクトップの Inventory_Start をダブルクリック
    または PowerShell で:
      cd "{dest}"
      python app.py

  【サーバーをブラウザで開く】
    http://localhost:5000

  【PC起動時の自動起動を設定する】
    autostart.ps1 を右クリック → PowerShellで実行

  【接続設定の変更】
    {dest}\\.env をメモ帳で開いて編集
""")
    pause("Enterを押して終了します...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nキャンセルしました")
    except Exception as e:
        print(f"\nエラーが発生しました: {e}")
        pause()
