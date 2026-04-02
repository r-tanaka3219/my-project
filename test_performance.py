"""
パフォーマンステスト
実行方法: python test_performance.py [--verbose]

テスト内容:
  1. インデックス使用状況 (EXPLAIN ANALYZE)
  2. 予測クエリ実行時間（sales_daily_agg あり/なし）
  3. N+1 修正確認（order_receipts 相関サブクエリ vs JOIN）
  4. sales_daily_agg 集計テーブル確認
  5. キャッシュ動作確認（TTL・無効化・背景再構築）
  6. テーブルサイズ・統計情報
"""
import sys
import os
import time
import threading

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

VERBOSE = '--verbose' in sys.argv

# ---- カラー出力ヘルパー ------------------------------------------------
def _ok(msg):    print("  [PASS] " + msg)
def _fail(msg):  print("  [FAIL] " + msg)
def _info(msg):  print("  [INFO] " + msg)
def _warn(msg):  print("  [WARN] " + msg)
def _head(msg):  print("\n" + "="*60 + "\n  " + msg + "\n" + "="*60)


# ---- DB 接続 -----------------------------------------------------------
def get_test_db():
    from database import get_dsn, DBConn
    import psycopg2
    conn = psycopg2.connect(**get_dsn(long_timeout=True))
    conn.autocommit = False
    return DBConn(conn)


def safe_rollback(db):
    """エラー後にトランザクションをリセットする"""
    try:
        db.rollback()
    except Exception:
        pass


# -----------------------------------------------------------------------
# テスト 1: インデックス使用状況
# -----------------------------------------------------------------------
def test_indexes(db):
    _head("TEST 1: インデックス使用状況")
    checks = [
        ("sales_history",    "ix_sales_history_jan_date"),
        ("sales_history",    "ix_sales_history_jan"),
        ("sales_history",    "ix_sales_history_sale_date"),
        ("stocks",           "ix_stocks_jan"),
        ("stocks",           "ix_stocks_jan_qty"),
        ("order_receipts",   "ix_order_receipts_order_id"),
        ("order_history",    "ix_order_history_jan"),
        ("products",         "ix_products_is_active"),
        ("products",         "ix_products_supplier_cd"),
        ("weekly_md_plans",  "ix_weekly_md_plans_jan_year"),
        ("promotion_plans",  "ix_promotion_plans_jan_date"),
        ("demand_plans",     "ix_demand_plans_jan_date"),
        ("sales_daily_agg",  "ix_sda_sale_dt"),
        ("sales_daily_agg",  "ix_sda_jan"),
    ]
    passed = failed = 0
    for tbl, idx in checks:
        try:
            row = db.execute("""
                SELECT indexname FROM pg_indexes
                WHERE tablename=%s AND indexname=%s
            """, [tbl, idx]).fetchone()
            if row:
                _ok(idx + " on " + tbl)
                passed += 1
            else:
                _fail(idx + " on " + tbl + " -- インデックス未作成")
                failed += 1
        except Exception as e:
            safe_rollback(db)
            _fail(idx + " on " + tbl + " -- エラー: " + str(e))
            failed += 1

    # インデックスが実際に使われているか EXPLAIN で確認
    explain_queries = [
        ("sales_history jan+date スキャン",
         "EXPLAIN SELECT * FROM sales_history "
         "WHERE sale_date::date >= CURRENT_DATE - INTERVAL '180 days' LIMIT 1"),
        ("stocks jan スキャン",
         "EXPLAIN SELECT SUM(quantity) FROM stocks WHERE jan='9784040000000' AND quantity>0"),
        ("order_receipts JOIN スキャン",
         "EXPLAIN SELECT o.id, COALESCE(r.total,0) FROM order_history o "
         "LEFT JOIN (SELECT order_history_id, SUM(received_qty) AS total "
         "FROM order_receipts GROUP BY order_history_id) r ON r.order_history_id=o.id LIMIT 1"),
    ]
    for label, sql in explain_queries:
        try:
            rows = db.execute(sql).fetchall()
            plan = " ".join(str(r) for r in rows)
            uses_index = "Index" in plan or "index" in plan
            if uses_index:
                _ok(label + " -> インデックス使用")
            else:
                _warn(label + " -> Seq Scan（データが少ない場合は正常）")
            if VERBOSE:
                for r in rows:
                    _info(str(r))
        except Exception as e:
            safe_rollback(db)
            _warn(label + " -> EXPLAIN エラー: " + str(e))

    print("\n  結果: " + str(passed) + " PASS / " + str(failed) + " FAIL")
    return failed == 0


# -----------------------------------------------------------------------
# テスト 2: 予測クエリ実行時間
# -----------------------------------------------------------------------
def test_forecast_timing(db):
    _head("TEST 2: 予測クエリ実行時間")
    passed = True

    # 2-a: sales_daily_agg の準備状況
    use_agg = False
    try:
        cnt = db.execute(
            "SELECT COUNT(*) AS c FROM sales_daily_agg "
            "WHERE sale_dt >= CURRENT_DATE - INTERVAL '7 days'"
        ).fetchone()['c']
        _info("sales_daily_agg 直近7日レコード数: " + str(cnt))
        use_agg = int(cnt or 0) > 0
    except Exception as e:
        safe_rollback(db)
        _warn("sales_daily_agg 確認エラー: " + str(e))

    # 2-b: sales_daily_agg を使った予測クエリ（利用可能な場合）
    if use_agg:
        t0 = time.perf_counter()
        try:
            r = db.execute("""
                WITH daily AS (
                    SELECT jan, sale_dt, dow, qty FROM sales_daily_agg
                    WHERE sale_dt >= CURRENT_DATE - INTERVAL '180 days'
                ), monthly AS (
                    SELECT jan, EXTRACT(MONTH FROM sale_dt)::int AS mon,
                           SUM(qty) AS qty
                    FROM daily
                    WHERE sale_dt >= DATE_TRUNC('month',CURRENT_DATE) - INTERVAL '12 months'
                    GROUP BY jan, mon
                ), avg_all AS (
                    SELECT jan, AVG(qty) AS avg_monthly FROM monthly GROUP BY jan
                )
                SELECT COUNT(*) AS c FROM avg_all
            """).fetchone()
            elapsed = time.perf_counter() - t0
            _ok("sales_daily_agg 使用クエリ: " + str(round(elapsed*1000)) + "ms (" + str(r['c']) + "商品)")
            if elapsed > 3.0:
                _warn("  3秒超え (" + str(round(elapsed,1)) + "s) -- インデックス・統計情報を確認してください")
        except Exception as e:
            safe_rollback(db)
            _fail("sales_daily_agg クエリエラー: " + str(e))
            passed = False
    else:
        _warn("sales_daily_agg 未準備 -- sales_history 直接クエリでテスト")

    # 2-c: sales_history 直接クエリ
    t0 = time.perf_counter()
    try:
        r = db.execute("""
            SELECT COUNT(DISTINCT jan) AS c FROM sales_history
            WHERE sale_date::date >= CURRENT_DATE - INTERVAL '180 days'
        """).fetchone()
        elapsed = time.perf_counter() - t0
        label = "sales_history 直接クエリ（180日 jan数）"
        if elapsed < 2.0:
            _ok(label + ": " + str(round(elapsed*1000)) + "ms (" + str(r['c']) + "商品)")
        elif elapsed < 5.0:
            _warn(label + ": " + str(round(elapsed*1000)) + "ms -- やや遅い")
        else:
            _fail(label + ": " + str(round(elapsed,1)) + "s -- インデックス未使用の可能性")
            passed = False
    except Exception as e:
        safe_rollback(db)
        _fail("sales_history クエリエラー: " + str(e))
        passed = False

    # 2-d: 全体の _build_forecast_rows_raw 実行時間
    t0 = time.perf_counter()
    try:
        import app as _app
        rows = _app._build_forecast_rows_raw(db)
        elapsed = time.perf_counter() - t0
        if elapsed < 3.0:
            _ok("_build_forecast_rows_raw: " + str(round(elapsed*1000)) + "ms (" + str(len(rows)) + "商品)")
        elif elapsed < 8.0:
            _warn("_build_forecast_rows_raw: " + str(round(elapsed,1)) + "s -- データ量増加に注意")
        else:
            _fail("_build_forecast_rows_raw: " + str(round(elapsed,1)) + "s -- 最適化が必要")
            passed = False
        _info("  2回目（キャッシュ経由）:")
        t1 = time.perf_counter()
        rows2 = _app._build_forecast_rows(db, q='')
        elapsed2 = time.perf_counter() - t1
        _ok("  _build_forecast_rows (キャッシュ): " + str(round(elapsed2*1000, 1)) + "ms")
    except Exception as e:
        safe_rollback(db)
        _fail("_build_forecast_rows_raw エラー: " + str(e))
        passed = False

    return passed


# -----------------------------------------------------------------------
# テスト 3: N+1 修正確認
# -----------------------------------------------------------------------
def test_n1_fix(db):
    _head("TEST 3: N+1 修正確認（order_receipts 相関サブクエリ排除）")
    passed = True

    # 旧クエリ（相関サブクエリ）
    t_old = None
    try:
        t0 = time.perf_counter()
        db.execute("""
            SELECT oh.jan,
                   GREATEST(oh.order_qty - COALESCE(
                       (SELECT SUM(received_qty) FROM order_receipts r
                        WHERE r.order_history_id=oh.id),0),0) AS outstanding_qty
            FROM order_history oh
            LIMIT 200
        """).fetchall()
        t_old = time.perf_counter() - t0
        _info("旧クエリ（相関サブクエリ）: " + str(round(t_old*1000)) + "ms")
    except Exception as e:
        safe_rollback(db)
        _warn("旧クエリ エラー: " + str(e))

    # 新クエリ（JOIN）
    try:
        t0 = time.perf_counter()
        db.execute("""
            SELECT oh.jan,
                   GREATEST(oh.order_qty - COALESCE(rcpt.total_received,0),0) AS outstanding_qty
            FROM order_history oh
            LEFT JOIN (
                SELECT order_history_id, SUM(received_qty) AS total_received
                FROM order_receipts GROUP BY order_history_id
            ) rcpt ON rcpt.order_history_id=oh.id
            LIMIT 200
        """).fetchall()
        t_new = time.perf_counter() - t0
        _ok("新クエリ（JOIN集約）: " + str(round(t_new*1000)) + "ms")
        if t_old is not None:
            ratio = t_old / max(t_new, 0.0001)
            suffix = "(改善)" if ratio >= 1.0 else "(同等)"
            _ok("速度改善比: " + str(round(ratio, 1)) + "x " + suffix)
    except Exception as e:
        safe_rollback(db)
        _fail("新クエリ エラー: " + str(e))
        passed = False

    return passed


# -----------------------------------------------------------------------
# テスト 4: sales_daily_agg テーブル確認
# -----------------------------------------------------------------------
def test_sales_daily_agg(db):
    _head("TEST 4: sales_daily_agg 集計テーブル")
    passed = True

    try:
        total = db.execute("SELECT COUNT(*) AS c FROM sales_daily_agg").fetchone()['c']
        recent = db.execute(
            "SELECT COUNT(*) AS c FROM sales_daily_agg "
            "WHERE sale_dt >= CURRENT_DATE - INTERVAL '30 days'"
        ).fetchone()['c']
        jans = db.execute(
            "SELECT COUNT(DISTINCT jan) AS c FROM sales_daily_agg"
        ).fetchone()['c']
        oldest = db.execute(
            "SELECT MIN(sale_dt) AS d FROM sales_daily_agg"
        ).fetchone()['d']
        newest = db.execute(
            "SELECT MAX(sale_dt) AS d FROM sales_daily_agg"
        ).fetchone()['d']

        if total > 0:
            _ok("レコード数: " + str(total) + "行 / " + str(jans) + "商品 / " + str(oldest) + " - " + str(newest))
            _ok("直近30日: " + str(recent) + "行")
        else:
            _warn("sales_daily_agg が空です -- 初回起動後に自動構築されます")

        # sales_history との整合性確認
        sh_cnt = db.execute(
            "SELECT COUNT(DISTINCT jan) AS c FROM sales_history "
            "WHERE sale_date::date >= CURRENT_DATE - INTERVAL '30 days'"
        ).fetchone()['c']
        agg_cnt = db.execute(
            "SELECT COUNT(DISTINCT jan) AS c FROM sales_daily_agg "
            "WHERE sale_dt >= CURRENT_DATE - INTERVAL '30 days'"
        ).fetchone()['c']
        if sh_cnt == 0 or abs(sh_cnt - agg_cnt) <= sh_cnt * 0.05:
            _ok("sales_history との整合性OK: sh=" + str(sh_cnt) + "商品 / agg=" + str(agg_cnt) + "商品")
        else:
            _warn("整合性差異: sh=" + str(sh_cnt) + "商品 / agg=" + str(agg_cnt) + "商品 -- 再集計を推奨")
    except Exception as e:
        safe_rollback(db)
        _fail("sales_daily_agg 確認エラー: " + str(e))
        passed = False

    # UPSERT テスト（1件だけ試す）
    try:
        db.execute("""
            INSERT INTO sales_daily_agg (jan, sale_dt, dow, qty)
            VALUES ('__test__', CURRENT_DATE, 1, 0)
            ON CONFLICT (jan, sale_dt) DO UPDATE SET qty=EXCLUDED.qty
        """)
        db.execute("DELETE FROM sales_daily_agg WHERE jan='__test__'")
        db.commit()
        _ok("UPSERT テスト OK")
    except Exception as e:
        safe_rollback(db)
        _fail("UPSERT テスト エラー: " + str(e))
        passed = False

    return passed


# -----------------------------------------------------------------------
# テスト 5: キャッシュ動作確認
# -----------------------------------------------------------------------
def test_cache(db):
    _head("TEST 5: 予測キャッシュ動作確認")
    passed = True

    try:
        import app as _app

        # 5-a: キャッシュを無効化して空を確認
        _app.invalidate_forecast_cache(background_refresh=False)
        with _app._fc_lock:
            empty = not _app._fc_store
        if empty:
            _ok("invalidate_forecast_cache() でキャッシュが空になることを確認")
        else:
            _fail("キャッシュが空にならない")
            passed = False

        # 5-b: 1回目（キャッシュミス = 同期計算）
        t0 = time.perf_counter()
        rows1 = _app._build_forecast_rows(db, q='')
        t1 = time.perf_counter() - t0
        _ok("1回目（同期計算）: " + str(round(t1*1000)) + "ms / " + str(len(rows1)) + "商品")

        # 5-c: 2回目（キャッシュヒット）
        t0 = time.perf_counter()
        rows2 = _app._build_forecast_rows(db, q='')
        t2 = time.perf_counter() - t0
        if t2 < 0.05:  # 50ms 未満ならキャッシュヒット
            _ok("2回目（キャッシュヒット）: " + str(round(t2*1000, 1)) + "ms -- 大幅高速化")
        else:
            _warn("2回目: " + str(round(t2*1000)) + "ms -- キャッシュが効いていない可能性")
        speedup = t1 / max(t2, 0.0001)
        _ok("キャッシュ効果: " + str(round(speedup)) + "x 高速化")

        # 5-d: キャッシュの一貫性（1回目と2回目で件数が同じ）
        if len(rows1) == len(rows2):
            _ok("キャッシュ一貫性OK: " + str(len(rows1)) + "件 = " + str(len(rows2)) + "件")
        else:
            _fail("キャッシュ一貫性NG: " + str(len(rows1)) + "件 != " + str(len(rows2)) + "件")
            passed = False

        # 5-e: 背景再構築テスト
        _app.invalidate_forecast_cache(background_refresh=True)
        _info("背景再構築を起動...")
        time.sleep(0.5)
        if _app._fc_computing or _app._fc_store:
            _ok("背景再構築スレッドが動作中または完了")
        else:
            _warn("背景再構築スレッドの状態を確認できない")

    except Exception as e:
        safe_rollback(db)
        _fail("キャッシュテスト エラー: " + str(e))
        passed = False

    return passed


# -----------------------------------------------------------------------
# テスト 6: DB テーブルサイズ・統計情報
# -----------------------------------------------------------------------
def test_table_stats(db):
    _head("TEST 6: テーブルサイズ・統計情報")
    passed = True

    tables = [
        "sales_history", "sales_daily_agg", "stocks", "products",
        "order_history", "order_receipts", "weekly_md_plans",
    ]
    print("  " + "テーブル".ljust(25) + "行数".rjust(10) + "サイズ".rjust(12))
    print("  " + "-"*25 + " " + "-"*10 + " " + "-"*12)
    for tbl in tables:
        try:
            cnt = db.execute("SELECT COUNT(*) AS c FROM " + tbl).fetchone()['c']
            size_row = db.execute(
                "SELECT pg_size_pretty(pg_total_relation_size('" + tbl + "'::regclass)) AS sz"
            ).fetchone()
            sz = size_row['sz'] if size_row else "N/A"
            print("  " + tbl.ljust(25) + str(cnt).rjust(10) + sz.rjust(12))
        except Exception as e:
            safe_rollback(db)
            print("  " + tbl.ljust(25) + "ERROR".rjust(10) + str(e)[:20].rjust(12))

    # VACUUM 統計で古い統計情報を検出
    try:
        old_stats = db.execute("""
            SELECT relname, last_analyze, last_autoanalyze
            FROM pg_stat_user_tables
            WHERE relname = ANY(%s)
              AND (last_analyze IS NULL OR last_analyze < NOW() - INTERVAL '1 day')
              AND (last_autoanalyze IS NULL OR last_autoanalyze < NOW() - INTERVAL '1 day')
        """, [tables]).fetchall()
        if old_stats:
            _warn("統計情報が古いテーブルあり: " + str([r['relname'] for r in old_stats]))
            _warn("ANALYZE を実行すると EXPLAIN プランが改善される場合があります")
        else:
            _ok("全テーブルの統計情報が新しい")
    except Exception as e:
        safe_rollback(db)
        _warn("統計情報確認エラー: " + str(e))

    return passed


# -----------------------------------------------------------------------
# メイン実行
# -----------------------------------------------------------------------
def main():
    print("\n" + "="*60)
    print("  在庫管理システム パフォーマンステスト")
    print("="*60)

    try:
        db = get_test_db()
        _ok("DB 接続成功")
    except Exception as e:
        _fail("DB 接続失敗: " + str(e))
        print("\n.env ファイルの PG_HOST / PG_DBNAME / PG_USER / PG_PASSWORD を確認してください")
        sys.exit(1)

    # テーブル・インデックスを最新化（migrate_db は独自接続を開く）
    _info("migrate_db() を実行中...")
    try:
        from database import migrate_db
        migrate_db()
        _ok("migrate_db() 完了")
    except Exception as e:
        _warn("migrate_db() エラー（続行）: " + str(e))

    results = {}
    tests = [
        ("インデックス",       test_indexes),
        ("予測クエリ速度",     test_forecast_timing),
        ("N+1 修正",          test_n1_fix),
        ("sales_daily_agg",   test_sales_daily_agg),
        ("キャッシュ動作",    test_cache),
        ("テーブル統計",      test_table_stats),
    ]
    for name, fn in tests:
        try:
            results[name] = fn(db)
        except Exception as e:
            safe_rollback(db)
            _fail(name + " テスト中に例外: " + str(e))
            results[name] = False

    # サマリー
    _head("テスト結果サマリー")
    all_pass = True
    for name, ok in results.items():
        if ok:
            _ok(name)
        else:
            _fail(name)
            all_pass = False

    print()
    if all_pass:
        print("  [OK] 全テスト PASS")
    else:
        print("  [NG] 一部テスト FAIL -- 上記の詳細を確認してください")

    db.close()
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
