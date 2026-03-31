#!/usr/bin/env python3
from __future__ import annotations
import getpass
import os
import subprocess
import sys
from pathlib import Path

APP = Path(__file__).resolve().parent
ENV_PATH = APP / '.env'


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
    lines = [
        f"PG_HOST={values.get('PG_HOST', 'localhost')}",
        f"PG_PORT={values.get('PG_PORT', '5432')}",
        f"PG_DBNAME={values.get('PG_DBNAME', 'inventory')}",
        f"PG_USER={values.get('PG_USER', 'inventory_user')}",
        f"PG_PASSWORD={values.get('PG_PASSWORD', 'inventory_pass')}",
        f"SECRET_KEY={values.get('SECRET_KEY', 'inv-secret-key')}",
        f"MAIL_SERVER={values.get('MAIL_SERVER', '')}",
        f"MAIL_PORT={values.get('MAIL_PORT', '25')}",
        f"MAIL_USE_TLS={values.get('MAIL_USE_TLS', 'False')}",
        f"MAIL_USE_SSL={values.get('MAIL_USE_SSL', 'False')}",
        f"MAIL_AUTH={values.get('MAIL_AUTH', 'False')}",
        f"MAIL_USERNAME={values.get('MAIL_USERNAME', '')}",
        f"MAIL_PASSWORD={values.get('MAIL_PASSWORD', '')}",
        f"MAIL_FROM={values.get('MAIL_FROM', '')}",
        f"MAIL_FROM_NAME={values.get('MAIL_FROM_NAME', '在庫管理システム')}",
    ]
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def decode_output(data: bytes) -> str:
    for enc in ('cp932', 'utf-8', 'utf-16'):
        try:
            return data.decode(enc)
        except Exception:
            pass
    return data.decode('utf-8', errors='ignore')


def find_psql() -> str:
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
    import shutil
    found = shutil.which('psql')
    if found:
        return found
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



def run_psql(psql: str, host: str, port: str, user: str, password: str, dbname: str, sql: str) -> tuple[int, str]:
    env = os.environ.copy()
    env['PGPASSWORD'] = password
    proc = subprocess.run(
        [psql, '-h', host, '-p', str(port), '-U', user, '-d', dbname, '-v', 'ON_ERROR_STOP=1', '-c', sql],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    )
    return proc.returncode, decode_output(proc.stdout)


def sql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def ask(prompt: str, default: str) -> str:
    v = input(f'{prompt} [{default}]: ').strip()
    return v or default


def main() -> int:
    print('=== Inventory DB bootstrap ===')
    env = read_env(ENV_PATH)
    host = ask('PG host', env.get('PG_HOST', 'localhost'))
    port = ask('PG port', env.get('PG_PORT', '5432'))
    app_db = ask('App DB name', env.get('PG_DBNAME', 'inventory'))
    app_user = ask('App DB user', env.get('PG_USER', 'inventory_user'))
    current_pw = env.get('PG_PASSWORD', 'inventory_pass')
    entered = getpass.getpass('App DB password [hidden, Enter to keep current]: ')
    app_pw = entered or current_pw or 'inventory_pass'
    super_user = ask('Postgres superuser', 'postgres')
    super_pw = getpass.getpass(f'Password for {super_user}: ')

    psql = find_psql()
    print(f'Using psql: {psql}')

    code, out = run_psql(psql, host, port, super_user, super_pw, 'postgres', 'SELECT 1;')
    if code != 0:
        print('[ERROR] DB bootstrap failed: Connection test failed: ' + out.strip())
        return 1

    app_user_esc = app_user.replace("'", "''")
    app_pw_esc = app_pw.replace("'", "''")
    app_db_esc = app_db.replace("'", "''")

    user_sql = (
        "DO $$ BEGIN "
        f"IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{app_user_esc}') THEN "
        f"CREATE ROLE {sql_ident(app_user)} LOGIN PASSWORD '{app_pw_esc}'; "
        "ELSE "
        f"ALTER ROLE {sql_ident(app_user)} WITH LOGIN PASSWORD '{app_pw_esc}'; "
        "END IF; "
        "END $$;"
    )
    code, out = run_psql(psql, host, port, super_user, super_pw, 'postgres', user_sql)
    if code != 0:
        print('[ERROR] DB bootstrap failed: ' + out.strip())
        return 1

    db_sql = (
        "SELECT 'exists' FROM pg_database WHERE datname = "
        f"'{app_db_esc}';"
    )
    code, out = run_psql(psql, host, port, super_user, super_pw, 'postgres', db_sql)
    if code != 0:
        print('[ERROR] DB bootstrap failed: ' + out.strip())
        return 1
    exists = 'exists' in out.lower()
    if not exists:
        create_sql = f'CREATE DATABASE {sql_ident(app_db)} OWNER {sql_ident(app_user)} ENCODING \'UTF8\';'
        code, out = run_psql(psql, host, port, super_user, super_pw, 'postgres', create_sql)
        if code != 0:
            print('[ERROR] DB bootstrap failed: ' + out.strip())
            return 1
    else:
        grant_sql = f'ALTER DATABASE {sql_ident(app_db)} OWNER TO {sql_ident(app_user)};'
        run_psql(psql, host, port, super_user, super_pw, 'postgres', grant_sql)

    env.update({
        'PG_HOST': host,
        'PG_PORT': str(port),
        'PG_DBNAME': app_db,
        'PG_USER': app_user,
        'PG_PASSWORD': app_pw,
    })
    write_env(ENV_PATH, env)
    print('Database and app user ready.')

    # initialize tables immediately
    sys.path.insert(0, str(APP))
    try:
        from database import init_db
        init_db()
    except Exception as e:
        print(f'[ERROR] Table initialization failed: {e}')
        return 1
    print('DB bootstrap completed. Tables are ready.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
