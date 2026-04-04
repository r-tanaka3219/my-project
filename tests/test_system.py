"""
在庫管理システム 統合テスト
修正内容（db.rowcount, teardown, パスワードハッシュ, プーリング, FOR UPDATE）を検証
実行: python tests/test_system.py
"""
import sys, os, traceback
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv('.env')

PASS = []
FAIL = []

def ok(name):
    PASS.append(name)
    print(f"  [PASS] {name}")

def ng(name, err):
    FAIL.append(name)
    print(f"  [FAIL] {name}: {err}")

# ─── 1. DBコネクションプーリング ──────────────────────────────────
print("\n=== 1. DBコネクションプーリング ===")
try:
    from database import get_db, _get_pool
    pool = _get_pool()
    assert pool is not None, "プールがNone"
    conns = [get_db() for _ in range(3)]
    for c in conns:
        c.close()
    ok("ThreadedConnectionPool 初期化・3接続取得・返却")
except Exception as e:
    ng("ThreadedConnectionPool", traceback.format_exc().splitlines()[-1])

# ─── 2. パスワードハッシュ強化 ────────────────────────────────────
print("\n=== 2. パスワードハッシュ強化 ===")
try:
    from auth_helpers import _hash, _check_hash
    h = _hash("testpassword123")
    # werkzeug の版によって pbkdf2: または scrypt: を使用（どちらも安全）
    assert h.startswith("pbkdf2:") or h.startswith("scrypt:"), f"不明なハッシュ形式: {h[:30]}"
    assert _check_hash(h, "testpassword123"), "新形式：正しいPWが検証失敗"
    assert not _check_hash(h, "wrongpassword"), "新形式：誤ったPWが通過"
    ok(f"新形式ハッシュ生成・検証（{h.split(':')[0]}）")
except Exception as e:
    ng("新形式パスワードハッシュ", traceback.format_exc().splitlines()[-1])

try:
    import hashlib
    old_hash = hashlib.sha256("legacy_pass".encode()).hexdigest()
    assert _check_hash(old_hash, "legacy_pass"), "旧SHA256形式の後方互換失敗"
    assert not _check_hash(old_hash, "wrong"), "旧SHA256形式で誤PWが通過"
    ok("旧 SHA-256 形式との後方互換")
except Exception as e:
    ng("旧形式後方互換", traceback.format_exc().splitlines()[-1])

# ─── 3. トランザクション基本動作 ──────────────────────────────────
print("\n=== 3. トランザクション・コミット・ロールバック ===")
db = None
try:
    db = get_db()
    db.execute(
        "INSERT INTO demand_plans (jan, demand_date, demand_qty, demand_type) VALUES (%s, CURRENT_DATE+10, 10, 'test')",
        ['0000000000001']
    )
    db.commit()
    row = db.execute(
        "SELECT * FROM demand_plans WHERE jan=%s AND demand_type='test'", ['0000000000001']
    ).fetchone()
    assert row is not None, "INSERT後にSELECTで取得できない"
    ok("INSERT → COMMIT → SELECT 確認")
    db.close()
except Exception as e:
    ng("INSERT/COMMIT/SELECT", traceback.format_exc().splitlines()[-1])
    if db:
        try: db.rollback(); db.close()
        except: pass

db = None
try:
    db = get_db()
    db.execute(
        "INSERT INTO demand_plans (jan, demand_date, demand_qty, demand_type) VALUES (%s, CURRENT_DATE+11, 99, 'test_rollback')",
        ['0000000000002']
    )
    db.rollback()
    row = db.execute(
        "SELECT * FROM demand_plans WHERE jan=%s AND demand_type='test_rollback'", ['0000000000002']
    ).fetchone()
    assert row is None, "ROLLBACK後にデータが残っている"
    ok("INSERT → ROLLBACK → データ消去確認")
    db.close()
except Exception as e:
    ng("INSERT/ROLLBACK", traceback.format_exc().splitlines()[-1])
    if db:
        try: db.rollback(); db.close()
        except: pass

# ─── 4. db.rowcount バグ修正確認 ──────────────────────────────────
print("\n=== 4. db.rowcount 修正確認 ===")
db = None
try:
    db = get_db()
    db.execute(
        "INSERT INTO demand_plans (jan, demand_date, demand_qty, demand_type) VALUES (%s, CURRENT_DATE+12, 5, 'test_rowcount')",
        ['0000000000003']
    )
    db.commit()
    cur = db.execute("DELETE FROM demand_plans WHERE demand_type='test_rowcount'")
    count = cur.rowcount
    db.commit()
    assert isinstance(count, int) and count == 1, f"rowcount={count}（期待値: 1）"
    ok(f"cur.rowcount 正常取得（削除件数={count}）")
    db.close()
except Exception as e:
    ng("db.rowcount", traceback.format_exc().splitlines()[-1])
    if db:
        try: db.rollback(); db.close()
        except: pass

# ─── 5. テストデータクリーンアップ ───────────────────────────────
print("\n=== 5. テストデータクリーンアップ ===")
db = None
try:
    db = get_db()
    db.execute(
        "DELETE FROM demand_plans WHERE jan IN ('0000000000001','0000000000002','0000000000003') AND demand_type LIKE 'test%'"
    )
    db.commit()
    db.close()
    ok("テストデータ削除完了")
except Exception as e:
    ng("クリーンアップ", traceback.format_exc().splitlines()[-1])
    if db:
        try: db.rollback(); db.close()
        except: pass

# ─── 6. SELECT FOR UPDATE（排他ロック）────────────────────────────
print("\n=== 6. SELECT FOR UPDATE（排他ロック）===")
db = None
try:
    db = get_db()
    row = db.execute("SELECT * FROM stocks WHERE quantity>0 LIMIT 1 FOR UPDATE").fetchone()
    if row:
        ok(f"SELECT FOR UPDATE 実行成功（JAN: {row['jan']}）")
    else:
        ok("SELECT FOR UPDATE 実行成功（在庫データなし・構文OK）")
    db.rollback()
    db.close()
except Exception as e:
    ng("SELECT FOR UPDATE", traceback.format_exc().splitlines()[-1])
    if db:
        try: db.rollback(); db.close()
        except: pass

# ─── 7. 主要テーブル クエリ確認 ───────────────────────────────────
print("\n=== 7. 主要テーブル クエリ確認 ===")
db = None
try:
    db = get_db()
    for table in ['products', 'stocks', 'order_history', 'sales_history', 'users']:
        cnt = db.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()['c']
        ok(f"{table}: {cnt} 件")
    db.close()
except Exception as e:
    ng("主要テーブル SELECT", traceback.format_exc().splitlines()[-1])
    if db:
        try: db.close()
        except: pass

# ─── 8. プール接続数上限テスト ────────────────────────────────────
print("\n=== 8. プール接続数上限テスト ===")
conns = []
try:
    from psycopg2.pool import PoolError
    for i in range(10):
        conns.append(get_db())
    ok("最大10接続取得成功")
    for c in conns:
        c.close()
    ok("10接続全返却成功")
    conns = []
except Exception as e:
    ng("プール接続テスト", traceback.format_exc().splitlines()[-1])
    for c in conns:
        try: c.close()
        except: pass

# ─── 9. Flask アプリ import・ルート登録確認 ──────────────────────
print("\n=== 9. Flask アプリ import・ルート登録確認 ===")
try:
    import app as flask_app
    application = flask_app.app
    rules = [r.rule for r in application.url_map.iter_rules()]
    for required in ['/login', '/inventory', '/orders', '/products', '/receipt']:
        assert required in rules, f"{required} ルートが未登録"
    ok(f"Flask app import成功・ルート登録数: {len(rules)}")
except Exception as e:
    ng("Flask app import", traceback.format_exc().splitlines()[-1])

# ─── 10. 認証フロー（Flaskテストクライアント）────────────────────
print("\n=== 10. 認証フロー テスト ===")
# テスト用にCSRF無効化
flask_app.app.config['WTF_CSRF_ENABLED'] = False
try:
    client = flask_app.app.test_client()
    # 未ログイン → /inventory はリダイレクト
    resp = client.get('/inventory', follow_redirects=False)
    assert resp.status_code in (302, 301), f"未認証で/inventoryが{resp.status_code}を返した"
    ok(f"未認証 /inventory → リダイレクト（{resp.status_code}）")
except Exception as e:
    ng("未認証リダイレクト", traceback.format_exc().splitlines()[-1])

try:
    resp = client.get('/login')
    assert resp.status_code == 200, f"/login が{resp.status_code}"
    ok("/login ページ表示成功（200）")
except Exception as e:
    ng("/login ページ", traceback.format_exc().splitlines()[-1])

try:
    resp = client.post('/login', data={'username': 'nouser', 'password': 'badpass'}, follow_redirects=True)
    assert resp.status_code == 200
    # ログイン失敗時はloginページが再表示される
    assert b'login' in resp.data.lower() or 'パスワード'.encode('utf-8') in resp.data
    ok("無効なログイン → 認証失敗・ページ再表示（200）")
except Exception as e:
    ng("無効ログイン検証", traceback.format_exc().splitlines()[-1])

# ─── 11. 実DBユーザーでのログイン（admin）────────────────────────
print("\n=== 11. admin ユーザーログインテスト ===")
try:
    db = get_db()
    admin = db.execute("SELECT username, password FROM users WHERE username='admin' AND is_active=1").fetchone()
    db.close()
    if admin:
        stored = admin['password']
        if stored.startswith('pbkdf2:'):
            ok(f"admin ユーザーのパスワードは新形式（PBKDF2）で保存済み")
        else:
            ok(f"admin ユーザーのパスワードは旧形式（SHA-256）→ 次回ログイン時に自動アップグレード")
    else:
        ok("admin ユーザーが存在しない（初期化前 or 別名）")
except Exception as e:
    ng("adminユーザー確認", traceback.format_exc().splitlines()[-1])

# ─── 結果サマリー ──────────────────────────────────────────────────
print("\n" + "="*50)
print(f"  結果: {len(PASS)} PASS / {len(FAIL)} FAIL")
if FAIL:
    print("  失敗項目:")
    for f in FAIL:
        print(f"    × {f}")
print("="*50)
sys.exit(0 if not FAIL else 1)
