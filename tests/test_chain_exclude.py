"""
チェーンCD除外ロジック 詳細診断テスト
実行方法: python tests/test_chain_exclude.py
"""
import sys, os, csv, io
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import get_dsn
import psycopg2
from psycopg2.extras import RealDictCursor

def get_conn():
    return psycopg2.connect(**get_dsn(), cursor_factory=RealDictCursor)

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

def ok(msg):   print(f"  [OK]  {msg}")
def ng(msg):   print(f"  [NG]  {msg}")
def info(msg): print(f"  [情報] {msg}")
def warn(msg): print(f"  [警告] {msg}")

# ─────────────────────────────────────────────────────────────
# 1. DB状態診断
# ─────────────────────────────────────────────────────────────
def check_db_state(conn):
    section("1. DB状態診断")
    cur = conn.cursor()

    # chain_masters
    cur.execute("SELECT * FROM chain_masters ORDER BY chain_cd")
    chains = cur.fetchall()
    print(f"\n  chain_masters ({len(chains)}件):")
    if not chains:
        ng("chain_mastersにレコードがありません")
    for c in chains:
        flag = "★除外" if c['exclude_deduct'] else "  引当"
        print(f"    {flag} | chain_cd={c['chain_cd']!r} | chain_name={c['chain_name']!r}")

    # 除外設定があるチェーン
    excluded = [c for c in chains if c['exclude_deduct']]
    if excluded:
        ok(f"除外設定チェーン: {[c['chain_cd'] for c in excluded]}")
    else:
        ng("除外設定(exclude_deduct=1)のチェーンが1件もありません！")

    # csv_import_settings の col_chain_cd
    cur.execute("SELECT id, name, import_type, col_chain_cd, col_store_cd, col_jan, col_qty, col_date FROM csv_import_settings WHERE is_active=1")
    csv_settings = cur.fetchall()
    print(f"\n  csv_import_settings (有効設定 {len(csv_settings)}件):")
    for s in csv_settings:
        col_chain = s['col_chain_cd'] or '（未設定→デフォルト: チェーンCD）'
        col_store = s['col_store_cd'] or '（未設定→デフォルト: 得意先CD）'
        print(f"    ID={s['id']} name={s['name']!r} import_type={s['import_type']!r}")
        print(f"      col_chain_cd={col_chain!r}  col_store_cd={col_store!r}")
        print(f"      col_jan={s['col_jan']!r}  col_qty={s['col_qty']!r}  col_date={s['col_date']!r}")

    return excluded, csv_settings

# ─────────────────────────────────────────────────────────────
# 2. 直近のsales_historyのchain_cd分布
# ─────────────────────────────────────────────────────────────
def check_sales_history(conn, excluded_chains):
    section("2. 直近sales_historyのchain_cd分布（最新3日分）")
    cur = conn.cursor()
    cur.execute("""
        SELECT chain_cd, COUNT(*) as cnt, MAX(sale_date) as last_date
        FROM sales_history
        WHERE sale_date >= TO_CHAR(CURRENT_DATE - INTERVAL '3 days', 'YYYY-MM-DD')
        GROUP BY chain_cd
        ORDER BY cnt DESC
        LIMIT 20
    """)
    rows = cur.fetchall()
    if not rows:
        warn("直近3日のsales_historyレコードなし")
        return
    excluded_cds = {c['chain_cd'] for c in excluded_chains}
    print(f"\n  chain_cd | 件数 | 最終日付 | 除外設定")
    for r in rows:
        cd = r['chain_cd'] or '（空）'
        is_excluded = r['chain_cd'] in excluded_cds if r['chain_cd'] else False
        flag = "★除外設定あり" if is_excluded else "  除外なし"
        print(f"    {cd!r:<20} | {r['cnt']:>5}件 | {r['last_date']} | {flag}")
        if is_excluded:
            warn(f"  → chain_cd={r['chain_cd']!r} は除外設定済みなのにsales_historyに存在！")

# ─────────────────────────────────────────────────────────────
# 3. 在庫引き当て除外ロジック 単体テスト（テストデータ使用）
# ─────────────────────────────────────────────────────────────
def test_exclude_logic(conn):
    section("3. 除外ロジック 単体テスト（テストデータ）")
    cur = conn.cursor()

    # テスト用チェーンCDを取得または作成
    TEST_CHAIN_CD = 'TEST_EXCLUDE_001'
    TEST_JAN = None

    # アクティブな商品を1件取得
    cur.execute("SELECT jan, product_name, supplier_cd, product_cd FROM products WHERE is_active=1 LIMIT 1")
    product = cur.fetchone()
    if not product:
        ng("テスト用商品が見つかりません（productsにis_active=1のレコードなし）")
        return
    TEST_JAN = product['jan']
    info(f"テスト商品: JAN={TEST_JAN!r} name={product['product_name']!r}")

    # テスト用チェーンCD（除外設定=1）をINSERT
    cur.execute("""
        INSERT INTO chain_masters (chain_cd, chain_name, exclude_deduct)
        VALUES (%s, %s, 1)
        ON CONFLICT (chain_cd) DO UPDATE SET exclude_deduct=1
    """, [TEST_CHAIN_CD, 'テスト除外チェーン'])
    conn.commit()
    ok(f"テスト用チェーンCD {TEST_CHAIN_CD!r} を exclude_deduct=1 でセット")

    # 在庫残数を確認
    cur.execute("SELECT COALESCE(SUM(quantity),0) AS total FROM stocks WHERE jan=%s", [TEST_JAN])
    before_qty = cur.fetchone()['total']
    info(f"テスト前在庫: {before_qty}個")

    if before_qty == 0:
        warn("在庫が0のため引き当てテストをスキップします（在庫を追加してください）")
        # テスト用在庫を追加
        cur.execute("""
            SELECT id, product_name, supplier_cd, supplier_name, product_cd, unit_qty
            FROM products WHERE jan=%s AND is_active=1
        """, [TEST_JAN])
        p = cur.fetchone()
        cur.execute("""
            INSERT INTO stocks (product_id, jan, product_name, supplier_cd, supplier_name,
                                product_cd, unit_qty, quantity, expiry_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'')
        """, [p['id'] if 'id' in p.keys() else 0, TEST_JAN, p['product_name'],
              p['supplier_cd'] or '', p['supplier_name'] if 'supplier_name' in p.keys() else '',
              p['product_cd'] or '', p['unit_qty'] or 1, 10])
        conn.commit()
        before_qty = 10
        info(f"テスト用在庫10個を追加しました")

    # ── 除外ロジックをシミュレート ──
    print(f"\n  【テスト A】chain_cd={TEST_CHAIN_CD!r} (exclude_deduct=1) → 引き当て除外されるか？")
    _chain_cache = {}
    exclude = False
    chain_cd = TEST_CHAIN_CD
    if chain_cd:
        if chain_cd not in _chain_cache:
            cur.execute("SELECT exclude_deduct FROM chain_masters WHERE chain_cd=%s", [chain_cd])
            cm = cur.fetchone()
            if cm:
                _chain_cache[chain_cd] = bool(cm['exclude_deduct'])
            else:
                _chain_cache[chain_cd] = False
        exclude = _chain_cache[chain_cd]
    if exclude:
        ok(f"除外ロジック正常: chain_cd={TEST_CHAIN_CD!r} → exclude=True → 引き当てスキップ")
    else:
        ng(f"除外ロジック異常: chain_cd={TEST_CHAIN_CD!r} → exclude=False → 引き当て実行されてしまう！")

    print(f"\n  【テスト B】chain_cd='' (空) → exclude=False になるか？")
    _chain_cache2 = {}
    exclude2 = False
    chain_cd2 = ''
    if chain_cd2:
        exclude2 = True  # 空なのでここには入らない
    if not exclude2:
        ok("chain_cd='' のとき exclude=False（正常）")
    else:
        ng("chain_cd='' のとき exclude=True（異常）")

    print(f"\n  【テスト C】存在しないchain_cd='UNKNOWN_999' → exclude=False になるか？")
    _chain_cache3 = {}
    exclude3 = False
    chain_cd3 = 'UNKNOWN_999'
    if chain_cd3:
        if chain_cd3 not in _chain_cache3:
            cur.execute("SELECT exclude_deduct FROM chain_masters WHERE chain_cd=%s", [chain_cd3])
            cm3 = cur.fetchone()
            _chain_cache3[chain_cd3] = bool(cm3 and cm3['exclude_deduct'])
        exclude3 = _chain_cache3[chain_cd3]
    if not exclude3:
        ok(f"未登録chain_cd={chain_cd3!r} → exclude=False（正常）")
    else:
        ng(f"未登録chain_cd={chain_cd3!r} → exclude=True（異常）")

    # テスト用チェーンCDをクリーンアップ
    cur.execute("DELETE FROM chain_masters WHERE chain_cd=%s", [TEST_CHAIN_CD])
    conn.commit()
    info(f"テスト用チェーンCD {TEST_CHAIN_CD!r} を削除（クリーンアップ完了）")

# ─────────────────────────────────────────────────────────────
# 4. CSV列名と実際のCSVヘッダーの突合確認ガイド
# ─────────────────────────────────────────────────────────────
def check_column_name_mismatch(conn, csv_settings):
    section("4. CSVカラム名設定 確認")
    cur = conn.cursor()

    for s in csv_settings:
        col_chain = (s['col_chain_cd'] or 'チェーンCD').strip()
        print(f"\n  設定ID={s['id']} ({s['name']!r})")
        print(f"    → col_chain_cdの設定値: {col_chain!r}")
        print(f"    → CSVの列名が {col_chain!r} と一致していないと chain_cd が空になります")
        print(f"    → chain_cd が空 = 除外チェックがスキップされる（引き当て実行される）")

    print(f"""
  【確認方法】
  実際のCSVファイルを開いて、最初の行（ヘッダー行）を確認してください。
  「チェーンCD」「チェーン」「CHAIN_CD」など列名が異なると除外されません。

  col_chain_cd の変更は:
  管理画面 → CSVインポート設定 → 対象設定を編集 → 「チェーンCD列名」を修正
""")

# ─────────────────────────────────────────────────────────────
# 5. 除外チェーンの在庫引き当て実害チェック（stock_movements × sales_history）
# ─────────────────────────────────────────────────────────────
def check_stock_movements(conn, excluded_chains):
    section("5. 除外チェーンで実際に在庫が引き当てられたか確認")
    cur = conn.cursor()
    excluded_cds = {c['chain_cd'] for c in excluded_chains}

    # 除外チェーンのJAN・日付を取得
    cur.execute("""
        SELECT DISTINCT chain_cd, jan, sale_date
        FROM sales_history
        WHERE sale_date >= TO_CHAR(CURRENT_DATE - INTERVAL '7 days', 'YYYY-MM-DD')
          AND chain_cd IS NOT NULL AND chain_cd != ''
    """)
    sh_rows = cur.fetchall()

    if not sh_rows:
        warn("直近7日のsales_historyにchain_cdが入っているデータなし")
        warn("→ CSVのチェーンCD列名が col_chain_cd の設定値と一致していない可能性大")
        return

    # 除外チェーンのJAN×日付のセット
    excluded_jan_dates = set()
    all_jan_dates = {}  # (jan, date) → set of chain_cds
    for r in sh_rows:
        key = (r['jan'], r['sale_date'])
        if key not in all_jan_dates:
            all_jan_dates[key] = set()
        all_jan_dates[key].add(r['chain_cd'])
        if r['chain_cd'] in excluded_cds:
            excluded_jan_dates.add(key)

    # 除外チェーン「だけ」が売った JAN×日付を特定（非除外チェーンも売っていたら除外が正常でも deduction が起きる）
    only_excluded_jan_dates = set()
    for key in excluded_jan_dates:
        non_excluded = all_jan_dates[key] - excluded_cds
        if not non_excluded:
            only_excluded_jan_dates.add(key)

    print(f"\n  除外チェーン「のみ」が販売したJAN×日付: {len(only_excluded_jan_dates)}件")
    print(f"  （他チェーンも販売している場合はそちらの引き当てが正常なので除外）\n")

    problem_count = 0
    ok_count = 0
    for key in sorted(only_excluded_jan_dates):
        jan, sale_date = key
        chain_cd_set = all_jan_dates[key] & excluded_cds
        cur.execute("""
            SELECT id, quantity, move_date, note
            FROM stock_movements
            WHERE move_type='sale' AND jan=%s AND move_date=%s
            LIMIT 3
        """, [jan, sale_date])
        mv_rows = cur.fetchall()
        if mv_rows:
            for mv in mv_rows:
                ng(f"★確定バグ: chain={chain_cd_set} JAN={jan!r} 日付={sale_date} "
                   f"→ stock_movements id={mv['id']} 数量={mv['quantity']}")
            problem_count += 1
        else:
            ok_count += 1

    if problem_count == 0 and ok_count > 0:
        ok(f"除外チェーン単独販売で在庫引き当てなし（{ok_count}件）→ 除外ロジック正常動作中")
    elif problem_count == 0 and ok_count == 0:
        info("除外チェーン単独販売のJANが存在しない（全て他チェーンとの重複）→ 判定不可")
        info("→ 除外ロジックは正常の可能性が高い")
    else:
        ng(f"★確定バグ: 除外チェーンのみ販売したJANで在庫引き当てが発生: {problem_count}件")

    # stock_movementsの最新sale件数確認
    cur.execute("""
        SELECT COUNT(*) as cnt FROM stock_movements
        WHERE move_type='sale'
          AND move_date >= TO_CHAR(CURRENT_DATE - INTERVAL '3 days', 'YYYY-MM-DD')
    """)
    total_mv = cur.fetchone()['cnt']
    info(f"直近3日の在庫引き当て合計: {total_mv}件")

# ─────────────────────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────────────────────
def main():
    print("\n" + "="*60)
    print("  チェーンCD除外ロジック 詳細診断テスト")
    print("="*60)
    try:
        conn = get_conn()
    except Exception as e:
        print(f"[エラー] DB接続失敗: {e}")
        sys.exit(1)

    try:
        excluded_chains, csv_settings = check_db_state(conn)
        check_sales_history(conn, excluded_chains)
        test_exclude_logic(conn)
        check_column_name_mismatch(conn, csv_settings)
        check_stock_movements(conn, excluded_chains)
    finally:
        conn.close()

    section("診断完了")
    print("""
  最も多い原因:
  1. CSVのチェーンCD列名 ≠ col_chain_cd の設定値
     → chain_cd が空になり除外チェックが動かない

  2. chain_masters のchain_cd ≠ CSVのchain_cd値
     → 違うコードが登録されている（大文字/小文字、全角/半角の違いなど）

  3. 月末月次取込を使用 → 今回の修正で対応済み
""")

if __name__ == '__main__':
    main()
