#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
在庫管理システム - データベース初期化スクリプト
PostgreSQL への接続・DBユーザー作成・テーブル生成を行います。

実行方法:
    python create_db.py
"""
from __future__ import annotations

import getpass
import os
import subprocess
import sys
from pathlib import Path

APP = Path(__file__).resolve().parent
ENV_PATH = APP / '.env'


# ─────────────────────────────────────────────────────────────
# .env 読み書き
# ─────────────────────────────────────────────────────────────

def read_env(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    if not path.exists():
        return data
    raw = path.read_bytes()
    text = None
    for enc in ('utf-8-sig', 'utf-8', 'cp932', 'utf-16'):
        try:
            text = raw.decode(enc)
            break
        except Exception:
            pass
    if text is None:
        text = raw.decode('utf-8', errors='ignore')
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith('#') or '=' not in s:
            continue
        k, v = s.split('=', 1)
        data[k.strip()] = v.strip()
    return data


def write_env(path: Path, values: dict[str, str]) -> None:
    """既存 .env を読み込み、指定キーのみ更新して書き直す"""
    existing = read_env(path)
    existing.update(values)
    lines = [f'{k}={v}' for k, v in existing.items()]
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


# ─────────────────────────────────────────────────────────────
# psql 検索
# ─────────────────────────────────────────────────────────────

def find_psql() -> str:
    import shutil
    found = shutil.which('psql')
    if found:
        return found

    candidates = []
    pf64 = os.environ.get('ProgramFiles', r'C:\Program Files')
    pf86 = os.environ.get('ProgramFiles(x86)', r'C:\Program Files (x86)')
    for pf in (pf64, pf86):
        for ver in ('17', '16', '15', '14', '13', '12'):
            candidates.append(Path(pf) / 'PostgreSQL' / ver / 'bin' / 'psql.exe')
    for drv in ('C:', 'D:'):
        for ver in ('17', '16', '15', '14', '13', '12'):
            candidates.append(Path(drv + '\\') / 'PostgreSQL' / ver / 'bin' / 'psql.exe')
    for c in candidates:
        if c.exists():
            return str(c)

    try:
        import winreg
        for hive in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
            for sub in (r'SOFTWARE\PostgreSQL\Installations',
                        r'SOFTWARE\Wow6432Node\PostgreSQL\Installations'):
                try:
                    key = winreg.OpenKey(hive, sub)
                    for i in range(winreg.QueryInfoKey(key)[0]):
                        try:
                            sub2 = winreg.EnumKey(key, i)
                            k2 = winreg.OpenKey(key, sub2)
                            base, _ = winreg.QueryValueEx(k2, 'Base Directory')
                            psql = Path(base) / 'bin' / 'psql.exe'
                            if psql.exists():
                                return str(psql)
                        except Exception:
                            pass
                except Exception:
                    pass
    except Exception:
        pass

    return 'psql'


def decode_output(data: bytes) -> str:
    for enc in ('cp932', 'utf-8', 'utf-16'):
        try:
            return data.decode(enc)
        except Exception:
            pass
    return data.decode('utf-8', errors='ignore')


def run_psql(
    psql: str, host: str, port: str, user: str,
    password: str, dbname: str, sql: str
) -> tuple[int, str]:
    env = os.environ.copy()
    env['PGPASSWORD'] = password
    proc = subprocess.run(
        [psql, '-h', host, '-p', str(port), '-U', user,
         '-d', dbname, '-v', 'ON_ERROR_STOP=1', '-c', sql],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    )
    return proc.returncode, decode_output(proc.stdout)


def sql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def ask(prompt: str, default: str) -> str:
    v = input(f'  {prompt} [{default}]: ').strip()
    return v or default


# ─────────────────────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────────────────────

def main() -> int:
    print()
    print('=' * 60)
    print('  在庫管理システム  データベース初期化')
    print('=' * 60)
    print()

    env = read_env(ENV_PATH)

    print('  ─ アプリ用 DB 接続情報 (.env から読み込み) ─')
    host    = ask('DB サーバー', env.get('PG_HOST', 'localhost'))
    port    = ask('ポート番号',  env.get('PG_PORT', '5432'))
    app_db  = ask('データベース名', env.get('PG_DBNAME', 'inventory'))
    app_usr = ask('DB ユーザー名', env.get('PG_USER', 'inventory_user'))

    current_pw = env.get('PG_PASSWORD', '')
    hint = '（入力なしで現在の値を維持）' if current_pw else ''
    entered = getpass.getpass(f'  DB パスワード {hint}: ')
    app_pw = entered if entered else (current_pw or 'inventory_pass')

    print()
    print('  ─ PostgreSQL 管理者接続情報 ─')
    print('  ※ DB ユーザー・データベースを作成するために必要です')
    super_user = ask('管理者ユーザー名', 'postgres')
    super_pw = getpass.getpass(f'  {super_user} のパスワード: ')

    psql = find_psql()
    print(f'\n  psql: {psql}')

    # 接続テスト
    print('\n  接続テスト中...')
    code, out = run_psql(psql, host, port, super_user, super_pw, 'postgres', 'SELECT 1;')
    if code != 0:
        print('\n  [ERROR] PostgreSQL への接続に失敗しました。')
        print(f'  詳細: {out.strip()}')
        print('  確認事項: ホスト / ポート / パスワード / PostgreSQL サービスの起動')
        return 1
    print('  接続 OK')

    # エスケープ
    usr_esc = app_usr.replace("'", "''")
    pw_esc  = app_pw.replace("'", "''")
    db_esc  = app_db.replace("'", "''")

    # ユーザー作成 / パスワード更新
    print(f'\n  DB ユーザー「{app_usr}」を確認中...')
    user_sql = (
        "DO $$ BEGIN "
        f"IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{usr_esc}') THEN "
        f"CREATE ROLE {sql_ident(app_usr)} LOGIN PASSWORD '{pw_esc}'; "
        "ELSE "
        f"ALTER ROLE {sql_ident(app_usr)} WITH LOGIN PASSWORD '{pw_esc}'; "
        "END IF; "
        "END $$;"
    )
    code, out = run_psql(psql, host, port, super_user, super_pw, 'postgres', user_sql)
    if code != 0:
        print(f'  [ERROR] ユーザー作成に失敗しました: {out.strip()}')
        return 1
    print(f'  ユーザー「{app_usr}」OK')

    # データベース作成
    print(f'\n  データベース「{app_db}」を確認中...')
    code, out = run_psql(psql, host, port, super_user, super_pw, 'postgres',
                         f"SELECT 'exists' FROM pg_database WHERE datname = '{db_esc}';")
    if code != 0:
        print(f'  [ERROR] DB 確認に失敗しました: {out.strip()}')
        return 1

    if 'exists' in out.lower():
        print(f'  データベース「{app_db}」は既に存在します')
        run_psql(psql, host, port, super_user, super_pw, 'postgres',
                 f'ALTER DATABASE {sql_ident(app_db)} OWNER TO {sql_ident(app_usr)};')
    else:
        create_sql = (
            f"CREATE DATABASE {sql_ident(app_db)} "
            f"OWNER {sql_ident(app_usr)} ENCODING 'UTF8';"
        )
        code, out = run_psql(psql, host, port, super_user, super_pw, 'postgres', create_sql)
        if code != 0:
            print(f'  [ERROR] DB 作成に失敗しました: {out.strip()}')
            return 1
        print(f'  データベース「{app_db}」を作成しました')

    # .env 更新
    write_env(ENV_PATH, {
        'PG_HOST':     host,
        'PG_PORT':     port,
        'PG_DBNAME':   app_db,
        'PG_USER':     app_usr,
        'PG_PASSWORD': app_pw,
    })
    print('\n  .env を更新しました')

    # テーブル初期化
    print('\n  テーブルを初期化中...')
    sys.path.insert(0, str(APP))
    try:
        from database import init_db
        init_db()
    except Exception as e:
        print(f'  [ERROR] テーブル初期化に失敗しました: {e}')
        return 1

    print()
    print('=' * 60)
    print('  データベース初期化  完了！')
    print(f'  DB  : {app_db}@{host}:{port}')
    print(f'  USER: {app_usr}')
    print('=' * 60)
    print()
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print('\nキャンセルしました')
        sys.exit(1)
