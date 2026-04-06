"""
CSV取込テスト: 未登録JAN取込改善の動作検証
- salesタイプ日次CSV: 未登録JANでもsales_historyに保存されること
- salesタイプ月次CSV: 未登録JANでもsales_historyに保存されること
- import_logsのステータスが'ok'（partial_skipでなく）になること
- 未登録JAN一覧がunrg_jans_jsonに記録されること
実行: python tests/test_csv_unregistered.py
"""
import sys, os, tempfile, shutil, json, csv as csv_module, traceback, calendar
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv('.env')
from datetime import date

PASS = []
FAIL = []

def ok(name):
    PASS.append(name)
    print(f"  [PASS] {name}")

def ng(name, err):
    FAIL.append(name)
    print(f"  [FAIL] {name}: {err}")

def cleanup_db():
    """テストデータをDBから削除"""
    from database import get_db
    db = get_db()
    db.execute(
        "DELETE FROM import_logs WHERE setting_id IN "
        "(SELECT id FROM csv_import_settings WHERE name LIKE %s)",
        ['__TEST_CSV_%']
    )
    db.execute("DELETE FROM csv_import_settings WHERE name LIKE %s", ['__TEST_CSV_%'])
    db.execute("DELETE FROM sales_history WHERE jan=%s", [UNRG_JAN])
    db.commit()
    db.close()

from database import get_db
from auto_check import run_csv_import, run_month_end_import

UNRG_JAN = '9999999999998'   # productsに存在しないJAN
TEST_DATE = date.today()
TEST_YM   = TEST_DATE.strftime('%Y%m')
LAST_DAY  = calendar.monthrange(TEST_DATE.year, TEST_DATE.month)[1]
ME_DATE_STR = f"{TEST_DATE.strftime('%Y-%m')}-{LAST_DAY:02d}"

daily_tmpdir   = None
monthly_tmpdir = None

# ─── 前提確認 ──────────────────────────────────────────────────────
print("\n=== 前提確認 ===")
db = get_db()
existing = db.execute("SELECT 1 FROM products WHERE jan=%s", [UNRG_JAN]).fetchone()
if existing:
    db.close()
    print(f"[ERROR] テスト用JAN {UNRG_JAN} がproductsに存在します。別のJANを使用してください。")
    sys.exit(1)
ok(f"テスト用JAN {UNRG_JAN} は未登録（正常）")

registered = db.execute(
    "SELECT jan, product_name FROM products WHERE is_active=1 LIMIT 1"
).fetchone()
if registered:
    ok(f"登録済み商品取得: JAN={registered['jan']} ({registered['product_name']})")
else:
    ok("登録済み商品なし（未登録JANのみでテスト）")
db.close()

# ─── 事前クリーンアップ ────────────────────────────────────────────
print("\n=== 事前クリーンアップ ===")
try:
    cleanup_db()
    ok("事前クリーンアップ完了")
except Exception as e:
    ok(f"事前クリーンアップ（残存データなし）: {e}")

try:
    # ─── テスト1: 日次CSV salesタイプ ─────────────────────────────
    print("\n=== テスト1: 日次CSV salesタイプ（未登録JAN取込） ===")
    daily_tmpdir = tempfile.mkdtemp(prefix='inv_test_daily_')
    csv_name = f'test_{TEST_DATE.strftime("%Y%m%d")}.csv'
    csv_path = os.path.join(daily_tmpdir, csv_name)

    with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv_module.writer(f)
        w.writerow(['JANコード', '数量', '納品日', '伝票番号', '行番号'])
        if registered:
            w.writerow([registered['jan'], 5, TEST_DATE.strftime('%Y-%m-%d'), 'TSLIP001', '1'])
        w.writerow([UNRG_JAN, 3, TEST_DATE.strftime('%Y-%m-%d'), 'TSLIP001', '2'])

    db = get_db()
    cur = db.execute("""
        INSERT INTO csv_import_settings
          (name, import_type, folder_path, filename_pattern, encoding,
           col_jan, col_qty, col_date, col_slip_no, col_row_no, is_active)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """, ['__TEST_CSV_DAILY__', 'sales', daily_tmpdir, '*.csv', 'utf-8-sig',
          'JANコード', '数量', '納品日', '伝票番号', '行番号', 0])
    daily_sid = cur.fetchone()['id']
    db.commit()
    db.close()

    results = run_csv_import(setting_id=daily_sid, all_files=True, trigger_type='test')

    db = get_db()
    # 未登録JANがsales_historyに保存されているか
    row_unrg = db.execute(
        "SELECT * FROM sales_history WHERE jan=%s AND source_file=%s",
        [UNRG_JAN, csv_name]
    ).fetchone()
    if row_unrg:
        ok(f"未登録JAN({UNRG_JAN})がsales_historyに保存された (qty={row_unrg['quantity']})")
    else:
        ng("日次CSV未登録JAN保存", f"sales_historyにJAN {UNRG_JAN} が見つからない")

    if registered:
        row_reg = db.execute(
            "SELECT * FROM sales_history WHERE jan=%s AND source_file=%s",
            [registered['jan'], csv_name]
        ).fetchone()
        if row_reg:
            ok(f"登録済みJAN({registered['jan']})もsales_historyに保存された")
        else:
            ng("日次CSV登録済みJAN保存", "sales_historyに登録済みJANが見つからない")

    log = db.execute(
        "SELECT * FROM import_logs WHERE setting_id=%s ORDER BY id DESC LIMIT 1",
        [daily_sid]
    ).fetchone()
    if log:
        if log['status'] == 'ok':
            ok(f"日次ステータス='ok' (detail: {log['detail'][:70]})")
        else:
            ng("日次ステータス確認", f"期待値='ok', 実際='{log['status']}'")

        if log['unrg_jans_json']:
            unrg_list = json.loads(log['unrg_jans_json'])
            if UNRG_JAN in unrg_list:
                ok(f"unrg_jans_jsonに未登録JANが記録された: {unrg_list}")
            else:
                ng("unrg_jans_json確認", f"{UNRG_JAN} が含まれない: {unrg_list}")
        else:
            ng("unrg_jans_json確認", "unrg_jans_jsonが空")

        if 'マスタ未登録の為在庫引当なし' in (log['detail'] or ''):
            ok("日次ログメッセージ正常（'マスタ未登録の為在庫引当なし'）")
        else:
            ng("日次ログメッセージ", f"期待のメッセージがない: {log['detail']}")
    else:
        ng("日次import_logs", "ログが記録されていない")
    db.close()

    # ─── テスト2: 月次CSV salesタイプ ─────────────────────────────
    print("\n=== テスト2: 月次CSV salesタイプ（未登録JAN取込） ===")
    monthly_tmpdir = tempfile.mkdtemp(prefix='inv_test_me_')
    me_csv_name = f'{TEST_YM}_売上実績.csv'
    me_csv_path = os.path.join(monthly_tmpdir, me_csv_name)

    with open(me_csv_path, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv_module.writer(f)
        w.writerow(['JANコード', '数量', '納品日', '伝票番号', '行番号'])
        if registered:
            w.writerow([registered['jan'], 10, ME_DATE_STR, 'TSLIP002', '1'])
        w.writerow([UNRG_JAN, 7, ME_DATE_STR, 'TSLIP002', '2'])

    db = get_db()
    cur = db.execute("""
        INSERT INTO csv_import_settings
          (name, import_type, folder_path, filename_pattern, encoding,
           col_jan, col_qty, col_date, col_slip_no, col_row_no, is_active,
           month_end_enabled, month_end_folder, month_end_pattern, month_end_date_col)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
    """, ['__TEST_CSV_ME__', 'sales', monthly_tmpdir, '*.csv', 'utf-8-sig',
          'JANコード', '数量', '納品日', '伝票番号', '行番号', 0,
          1, monthly_tmpdir, '{yyyymm}_売上実績.csv', '納品日'])
    monthly_sid = cur.fetchone()['id']
    db.commit()
    db.close()

    results_me = run_month_end_import(
        setting_id=monthly_sid, target_ym=TEST_YM,
        all_dates=True, trigger_type='test'
    )

    # 月次CSVのsource_fileはlog_key形式: month_end_{ym}_{filename}
    me_log_key = f"month_end_{TEST_YM}_{me_csv_name}"
    db = get_db()
    row_me_unrg = db.execute(
        "SELECT * FROM sales_history WHERE jan=%s AND source_file=%s",
        [UNRG_JAN, me_log_key]
    ).fetchone()
    if row_me_unrg:
        ok(f"月次CSV: 未登録JAN({UNRG_JAN})がsales_historyに保存された (qty={row_me_unrg['quantity']})")
    else:
        ng("月次CSV未登録JAN保存", f"sales_historyにJAN {UNRG_JAN} が見つからない (月次 log_key={me_log_key})")

    if registered:
        row_me_reg = db.execute(
            "SELECT * FROM sales_history WHERE jan=%s AND source_file=%s",
            [registered['jan'], me_log_key]
        ).fetchone()
        if row_me_reg:
            ok(f"月次CSV: 登録済みJAN({registered['jan']})もsales_historyに保存された")
        else:
            ng("月次CSV登録済みJAN保存", "sales_historyに登録済みJANが見つからない (月次)")

    log_me = db.execute(
        "SELECT * FROM import_logs WHERE setting_id=%s ORDER BY id DESC LIMIT 1",
        [monthly_sid]
    ).fetchone()
    if log_me:
        if log_me['status'] == 'ok':
            ok(f"月次ステータス='ok' (detail: {log_me['detail'][:70]})")
        else:
            ng("月次ステータス確認", f"期待値='ok', 実際='{log_me['status']}'")

        if log_me['unrg_jans_json']:
            me_unrg_list = json.loads(log_me['unrg_jans_json'])
            if UNRG_JAN in me_unrg_list:
                ok(f"月次unrg_jans_jsonに未登録JANが記録された: {me_unrg_list}")
            else:
                ng("月次unrg_jans_json確認", f"{UNRG_JAN} が含まれない: {me_unrg_list}")
        else:
            ng("月次unrg_jans_json確認", "unrg_jans_jsonが空")

        if 'マスタ未登録の為在庫引当なし' in (log_me['detail'] or ''):
            ok("月次ログメッセージ正常（'マスタ未登録の為在庫引当なし'）")
        else:
            ng("月次ログメッセージ", f"期待のメッセージがない: {log_me['detail']}")
    else:
        ng("月次import_logs", "ログが記録されていない")
    db.close()

finally:
    # ─── クリーンアップ ────────────────────────────────────────────
    print("\n=== クリーンアップ ===")
    try:
        cleanup_db()
        ok("テストデータ削除完了")
    except Exception as e:
        ng("クリーンアップ", str(e))
    if daily_tmpdir:
        shutil.rmtree(daily_tmpdir, ignore_errors=True)
    if monthly_tmpdir:
        shutil.rmtree(monthly_tmpdir, ignore_errors=True)

# ─── 結果サマリー ──────────────────────────────────────────────────
print("\n" + "=" * 50)
print(f"  結果: {len(PASS)} PASS / {len(FAIL)} FAIL")
if FAIL:
    print("  失敗項目:")
    for f in FAIL:
        print(f"    × {f}")
print("=" * 50)
sys.exit(0 if not FAIL else 1)
