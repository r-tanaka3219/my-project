"""
自動チェックエンジン
① CSVインポート（時間設定・ファイル名パターン・当日日付・販売引き当て）
② 発注点チェック・ロット数チェック → 自動発注メール
③ 賞味期限アラート
④ 発注点の前年実績自動更新
"""
import os
import threading
import time
import csv
import calendar
import math
import logging
import json
from datetime import date, datetime, timedelta
from pathlib import Path

logger = logging.getLogger('inventory.scheduler')

def get_db_long():
    """CSVインポートなど長時間処理用のDB接続（タイムアウト無効）"""
    import psycopg2
    from database import get_dsn, DBConn
    conn = psycopg2.connect(**get_dsn(long_timeout=True))
    conn.autocommit = False
    return DBConn(conn)
from mail_service import send_expiry_alert, flush_order_mail, queue_order


# ─── ファイル名パターン解決 ─────────────────────────────────────
def resolve_filename_pattern(pattern: str, target_date: date = None) -> str:
    """
    パターン例:
      {yyyymm}_売上実績.csv   →  202603_売上実績.csv   ← 月次ファイル（推奨）
      {yymm}_売上実績.csv     →  2603_売上実績.csv
      *{yyyymmdd}.csv         →  *20260309.csv          ← 日次ファイル
      sales_{yyyymmdd}.csv    →  sales_20260309.csv
      {yyyy}{mm}{dd}.csv      →  20260309.csv
    """
    if target_date is None:
        target_date = date.today()
    result = pattern
    # 月次プレースホルダ（先に処理：yyyymmdd より前に yyyymm を置換）
    result = result.replace('{yyyymm}',  target_date.strftime('%Y%m'))
    result = result.replace('{yymm}',    target_date.strftime('%y%m'))
    # 日次プレースホルダ
    result = result.replace('{yyyymmdd}', target_date.strftime('%Y%m%d'))
    result = result.replace('{yymmdd}',   target_date.strftime('%y%m%d'))
    result = result.replace('{yyyy}',     target_date.strftime('%Y'))
    result = result.replace('{mm}',       target_date.strftime('%m'))
    result = result.replace('{dd}',       target_date.strftime('%d'))
    return result




def _normalize_jan(raw: str) -> str:
    """指数表記(4.90312E+12)や小数点付きJANを正規化"""
    s = raw.strip()
    if not s:
        return ''
    try:
        # 指数表記 or 小数点の場合
        if 'E' in s.upper() or ('.' in s and not s.replace('.','').isdigit() is False):
            return str(int(float(s)))
    except Exception:
        pass
    return s

def _unc_server(folder_path: str) -> str:
    """UNCパスからサーバー\\共有 部分を取得"""
    import os as _os
    p = folder_path.replace('/', _os.sep)
    if not p.startswith('\\\\'):
        return ''
    parts = p.lstrip('\\').split('\\')
    if len(parts) < 2:
        return ''
    return '\\\\' + parts[0] + '\\' + parts[1]


def _net_use_connect(folder_path: str, net_user: str, net_pass: str) -> tuple:
    """UNCパスにnet useで接続。成功時(True, unc_server)、不要時(True, '')"""
    import subprocess as _sp
    unc = _unc_server(folder_path)
    if not unc or not net_user:
        return True, ''
    try:
        _sp.run(['net', 'use', unc, '/delete', '/y'],
                capture_output=True, timeout=10)
        r = _sp.run(
            ['net', 'use', unc, net_pass or '', f'/user:{net_user}', '/persistent:no'],
            capture_output=True, text=True, timeout=15,
            encoding='cp932', errors='replace'
        )
        if r.returncode == 0:
            return True, unc
        return False, f'net use 失敗: {r.stderr.strip() or r.stdout.strip()}'
    except Exception as e:
        return False, f'net use 例外: {e}'


def _net_use_disconnect(unc: str):
    """net use 接続を切断"""
    import subprocess as _sp
    if unc:
        try:
            _sp.run(['net', 'use', unc, '/delete', '/y'],
                    capture_output=True, timeout=10)
        except Exception:
            pass


def find_csv_files(folder_path: str, pattern: str, target_date: date = None, all_files: bool = False) -> list:
    """フォルダからパターンに一致する未処理CSVファイルを返す。all_files=Trueの場合は全CSVを返す"""
    if not folder_path:
        return []

    import os as _os, fnmatch as _fnmatch
    fp = folder_path.replace('/', _os.sep)
    resolved = resolve_filename_pattern(pattern, target_date)

    # UNCパス（\\server\share）はos.listdirで処理
    if fp.startswith('\\\\') or fp.startswith('//'):
        try:
            entries = _os.listdir(fp)
            if all_files:
                matches = [Path(_os.path.join(fp, e)) for e in entries
                           if e.lower().endswith('.csv')]
            else:
                matches = [Path(_os.path.join(fp, e)) for e in entries
                           if _fnmatch.fnmatch(e, resolved)]
            return sorted(matches)
        except Exception as _e:
            logger.warning(f"[find_csv_files] UNCパスエラー: {_e}")
            return []

    folder = Path(folder_path)
    if all_files:
        try:
            return sorted(folder.glob('*.csv')) + sorted(folder.glob('*.CSV'))
        except Exception:
            return []
    try:
        matches = list(folder.glob(resolved))
    except Exception:
        try:
            import glob as g
            matches = [Path(p) for p in g.glob(str(folder / resolved))]
        except Exception:
            matches = []
    return sorted(matches)


# ─── 月末判定 ──────────────────────────────────────────────────
def is_month_end(d: date) -> bool:
    """指定日がその月の末日かどうかを返す"""
    last = calendar.monthrange(d.year, d.month)[1]
    return d.day == last


def _scheduler_already_ran(key, today_str):
    """当日すでに実行済みかDBで確認し、未実行なら登録してFalseを返す"""
    try:
        db = get_db_long()
        already = db.execute(
            "SELECT 1 FROM scheduler_run_log WHERE job_key=%s AND run_date=%s",
            [key, today_str]
        ).fetchone()
        if not already:
            db.execute(
                "INSERT INTO scheduler_run_log (job_key, run_date) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                [key, today_str]
            )
            db.commit()
        db.close()
        return bool(already)
    except Exception:
        return False


def run_month_end_import(setting_id=None, target_ym=None, all_dates=False, progress_cb=None, trigger_type='manual'):
    """
    月末月次CSVインポート
    - target_ym: 'YYYYMM' 文字列（例: '202602'）
      省略時は「今日が月末なら当月、月末でなければ直前月」を自動判定
    - month_end_folder / month_end_pattern で指定したCSVを取込む
    - month_end_date_col 列の値が当月末日のデータだけを絞り込んでインポート
    """
    import calendar as _cal
    today = date.today()

    if target_ym:
        try:
            yr, mo = int(target_ym[:4]), int(target_ym[4:6])
        except Exception:
            yr, mo = today.year, today.month
    else:
        # 今日が月末なら当月、それ以外なら直前月
        if is_month_end(today):
            yr, mo = today.year, today.month
        else:
            first = today.replace(day=1)
            prev  = first - timedelta(days=1)
            yr, mo = prev.year, prev.month

    last_day   = _cal.monthrange(yr, mo)[1]
    month_end_date = date(yr, mo, last_day)          # 例: 2026-02-28
    ym_str         = month_end_date.strftime('%Y%m') # 例: 202602
    target_date_obj = date(yr, mo, 1)                # resolve用（月先頭でOK）

    db = get_db_long()
    if setting_id:
        cfgs = db.execute(
            "SELECT * FROM csv_import_settings WHERE id=%s AND month_end_enabled=1",
            [int(setting_id)]
        ).fetchall()
    else:
        cfgs = db.execute(
            "SELECT * FROM csv_import_settings WHERE is_active=1 AND month_end_enabled=1"
        ).fetchall()

    all_results = []

    for cfg in cfgs:
        folder  = (cfg['month_end_folder']  or '').strip()
        pattern = (cfg['month_end_pattern'] or '{yyyymm}_売上実績.csv').strip()
        date_col = (cfg['month_end_date_col'] or cfg['col_date'] or '納品日').strip()

        if not folder:
            all_results.append({
                'name': cfg['name'], 'status': 'skip',
                'detail': '月末月次フォルダが未設定です'
            })
            continue

        # ── ネットワーク共有フォルダ認証 ────────────────────────
        net_user = (cfg.get('net_user') or '').strip()
        net_pass = (cfg.get('net_pass') or '').strip()
        ok, unc_or_err = _net_use_connect(folder, net_user, net_pass)
        if not ok:
            all_results.append({'name': cfg['name'], 'status': 'error',
                                'detail': f'フォルダ接続エラー: {unc_or_err}'})
            continue
        connected_unc = unc_or_err

        files = find_csv_files(folder, pattern, target_date_obj)
        if not files:
            _net_use_disconnect(connected_unc)
            resolved = resolve_filename_pattern(pattern, target_date_obj)
            all_results.append({
                'name': cfg['name'], 'status': 'skip',
                'detail': f'月末月次ファイルなし ({folder}\\{resolved})'
            })
            continue

        # 月末日文字列の表現バリエーション（CSV側の表記ゆれに対応）
        end_day_variants = {
            month_end_date.strftime('%Y/%m/%d'),   # 2026/02/28
            month_end_date.strftime('%Y-%m-%d'),   # 2026-02-28
            month_end_date.strftime('%Y%m%d'),     # 20260228
            f"{yr}/{mo}/{last_day}",               # 2026/2/28
            f"{yr}/{mo:02d}/{last_day:02d}",       # 2026/02/28
            f"{mo}/{last_day}",                    # 2/28
            f"{mo:02d}/{last_day:02d}",            # 02/28
            str(month_end_date),
        }

        for csv_path in files:
            # 月末月次ログキー（ファイル名 + 月末日）で重複チェック
            # all_dates=Trueの場合は強制再取込（ログチェックスキップ）
            log_key = f"month_end_{ym_str}_{csv_path.name}"
            if not all_dates:
                done = db.execute(
                    "SELECT COUNT(*) AS _cnt FROM import_logs WHERE filename=%s AND status IN ('ok','partial_skip')",
                    [log_key]
                ).fetchone()['_cnt']
                if done:
                    all_results.append({
                        'name': cfg['name'], 'status': 'skip',
                        'detail': f'取込済み ({csv_path.name} 月末分 {month_end_date})'
                    })
                    continue

            rows_ok = rows_err = rows_skip = rows_skip_unrg = 0
            unrg_jans = {}
            errors = []
            import_type = cfg['import_type'] or 'sales'

            try:
                enc = cfg['encoding'] or 'utf-8-sig'
                with open(csv_path, encoding=enc, errors='replace') as f:
                    reader = csv.DictReader(f)

                    # フィルター設定（通常インポートと共通）
                    filter_col    = (cfg.get('col_filter_cd') or '').strip()
                    raw_vals      = (cfg.get('filter_cd_values') or '').strip()
                    filter_values = [
                        v.strip() for v in raw_vals.replace('、', ',').split(',') if v.strip()
                    ] if raw_vals else []

                    # 総行数を事前カウント（進捗用）
                    total_rows = sum(1 for _ in reader)

                with open(csv_path, encoding=enc, errors='replace') as f_inner:
                    reader2 = csv.DictReader(f_inner)
                    if progress_cb:
                        progress_cb({'phase': 'start', 'file': csv_path.name, 'total': total_rows})

                    for i, row in enumerate(reader2, 1):
                        if progress_cb and i % 1000 == 0:
                            progress_cb({'phase': 'progress', 'file': csv_path.name,
                                        'current': i, 'total': total_rows,
                                        'ok': rows_ok, 'err': rows_err, 'skip': rows_skip})
                        try:
                            # ── 月末日フィルター ─────────────────────
                            row_date = str(row.get(date_col, '') or '').strip()
                            if not all_dates and row_date not in end_day_variants:
                                rows_skip += 1
                                continue

                            # ── 担当CDフィルター ─────────────────────
                            if filter_values and filter_col:
                                row_cd = str(row.get(filter_col, '') or '').strip()
                                if row_cd not in filter_values:
                                    rows_skip += 1
                                    continue

                            # ── JANコード ────────────────────────────
                            jan = _normalize_jan(str(row.get(cfg['col_jan'], '') or ''))
                            if not jan:
                                continue

                            # ── 行ハッシュ（重複チェック）────────────
                            import hashlib as _hashlib2
                            slip_col_me = (cfg.get('col_slip_no') or '伝票番号').strip()
                            row_no_col_me = (cfg.get('col_row_no') or '行番号').strip()
                            chain_col_me = (cfg.get('col_chain_cd') or 'チェーンCD').strip()
                            slip_no_me  = str(row.get(slip_col_me, '') or '').strip()
                            row_no_me   = str(row.get(row_no_col_me, '') or '').strip()
                            chain_cd_me = str(row.get(chain_col_me, '') or '').strip()
                            row_hash_me = _hashlib2.md5(
                                f"{row_date}|{chain_cd_me}|{slip_no_me}|{row_no_me}".encode()
                            ).hexdigest()
                            dup_me = db.execute(
                                "SELECT 1 FROM sales_history WHERE row_hash=%s", [row_hash_me]
                            ).fetchone()
                            if dup_me:
                                rows_skip += 1
                                continue

                            # ── 数量計算 ─────────────────────────────
                            qty = 0
                            cases_raw  = str(row.get('ケース',  '') or '').replace(',', '').strip()
                            pieces_raw = str(row.get('ピース',  '') or '').replace(',', '').strip()
                            if cases_raw or pieces_raw:
                                cases  = int(float(cases_raw))  if cases_raw  else 0
                                pieces = int(float(pieces_raw)) if pieces_raw else 0
                                unit_raw = str(row.get('入数', '') or '').replace(',', '').strip()
                                if unit_raw:
                                    unit = int(float(unit_raw))
                                else:
                                    p_tmp = db.execute(
                                        "SELECT unit_qty FROM products WHERE jan=%s AND is_active=1",
                                        [jan]
                                    ).fetchone()
                                    unit = p_tmp['unit_qty'] if p_tmp else 1
                                qty = cases * unit + pieces
                            else:
                                qty_raw = str(row.get(cfg['col_qty'], '0') or '0')
                                qty = int(float(qty_raw.replace(',', '').strip() or 0))

                            if qty <= 0:
                                continue

                            # ── 商品マスタ照合 ───────────────────────
                            product = db.execute(
                                "SELECT * FROM products WHERE jan=%s AND is_active=1", [jan]
                            ).fetchone()
                            product_name = product['product_name'] if product else jan

                            expiry = str(row.get(cfg.get('col_expiry', '賞味期限'), '') or '').strip()

                            if import_type == 'record_only':
                                # 引き当てなし・記録のみ（商品マスタ未登録でも取込む）
                                result_me = db.execute("""
                                    INSERT INTO sales_history
                                    (jan,product_name,quantity,sale_date,source_file,row_hash)
                                    VALUES (%s,%s,%s,%s,%s,%s)
                                    ON CONFLICT (row_hash) WHERE row_hash IS NOT NULL AND row_hash <> '' DO NOTHING
                                """, [jan, product_name, qty, row_date, log_key, row_hash_me])
                                if result_me.rowcount == 0:
                                    rows_skip += 1
                                    continue
                            elif import_type == 'sales':
                                if not product:
                                    rows_skip_unrg += 1
                                    unrg_jans[jan] = jan
                                    continue
                                _deduct_stock(db, product, qty, row_date, log_key)
                                result_me = db.execute("""
                                    INSERT INTO sales_history
                                    (jan,product_name,quantity,sale_date,source_file,row_hash)
                                    VALUES (%s,%s,%s,%s,%s,%s)
                                    ON CONFLICT (row_hash) WHERE row_hash IS NOT NULL AND row_hash <> '' DO NOTHING
                                """, [jan, product_name, qty, row_date, log_key, row_hash_me])
                                if result_me.rowcount == 0:
                                    rows_skip += 1
                                    continue
                            else:
                                if not product:
                                    rows_skip_unrg += 1
                                    unrg_jans[jan] = jan
                                    continue
                                _add_stock(db, product, qty, expiry, log_key)

                            rows_ok += 1

                        except Exception as e:
                            errors.append(f"行{i}: {e}")
                            rows_err += 1

                        # 1000行ごとに中間コミット（DBロック防止）
                        if i % 1000 == 0:
                            try:
                                db.commit()
                            except Exception:
                                db.rollback()

                db.commit()
                if all_dates:
                    skip_note = f" スキップ{rows_skip}行(重複)" if rows_skip else ""
                else:
                    skip_note = f" スキップ{rows_skip}行(月末日以外)" if rows_skip else ""
                unrg_note = f" 未登録商品スキップ{rows_skip_unrg}行(商品登録後に再取込可)" if rows_skip_unrg else ""
                detail = (
                    f"月末月次取込({month_end_date}) 成功{rows_ok}行{skip_note}{unrg_note}"
                    + (f" エラー{rows_err}行: {'; '.join(errors[:3])}" if errors else "")
                )
                if rows_skip_unrg > 0 and rows_err == 0:
                    status = 'partial_skip'
                elif rows_err == 0:
                    status = 'ok'
                else:
                    status = 'partial'

                # ログキーで記録（all_dates=Trueの場合は既存ログを削除して上書き）
                if all_dates:
                    db.execute("DELETE FROM import_logs WHERE filename=%s", [log_key])
                db.execute("""
                    INSERT INTO import_logs
                    (setting_id,filename,rows_ok,rows_err,status,detail,trigger_type,unrg_jans_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, [cfg['id'], log_key, rows_ok, rows_err, status, detail, trigger_type,
                      json.dumps(sorted(unrg_jans.keys()), ensure_ascii=False) if unrg_jans else ''])
                db.execute("""
                    UPDATE csv_import_settings
                    SET last_run_at=NOW(), last_result=%s WHERE id=%s
                """, [detail, cfg['id']])
                db.commit()

                all_results.append({
                    'name': cfg['name'], 'file': csv_path.name,
                    'month_end_date': str(month_end_date),
                    'status': status, 'rows_ok': rows_ok, 'rows_err': rows_err,
                    'detail': detail
                })
                # CSV取込後の自動発注チェックは廃止（誤発注防止）

            except Exception as e:
                db.execute("""
                    INSERT INTO import_logs
                    (setting_id,filename,rows_ok,rows_err,status,detail,trigger_type)
                    VALUES (%s,%s,0,0,'error',%s,%s)
                """, [cfg['id'], log_key, str(e), trigger_type])
                db.commit()
                all_results.append({
                    'name': cfg['name'], 'status': 'error', 'detail': str(e)
                })

        _net_use_disconnect(connected_unc)

    # 売上・在庫データが更新されたため予測関連を更新（遅延インポートで循環参照を回避）
    try:
        import sys as _sys
        if 'app' in _sys.modules:
            _app = _sys.modules['app']
            # sales_daily_agg を再集計してからキャッシュを無効化・再構築
            threading.Thread(target=_app._bg_refresh_sales_daily_agg, daemon=True).start()
            _app.invalidate_forecast_cache(background_refresh=True)
    except Exception:
        pass

    return all_results


# ─── CSV インポート実行 ──────────────────────────────────────────
def run_csv_import(setting_id=None, target_date=None, target_ym=None, progress_cb=None, trigger_type='auto', all_files=False):
    """
    target_date : date オブジェクト（日次ファイル用）
    target_ym   : 'YYYYMM' 文字列（月次ファイル用）。指定時は当月1日を target_date として使用
    """
    # target_ym が指定された場合（例: '202603'）は当月1日に変換
    if target_ym:
        try:
            target_date = date(int(target_ym[:4]), int(target_ym[4:6]), 1)
        except Exception:
            target_date = date.today()
    elif target_date is None:
        target_date = date.today()

    db = get_db_long()
    if setting_id:
        settings = db.execute(
            "SELECT * FROM csv_import_settings WHERE id=%s", [int(setting_id)]
        ).fetchall()
    else:
        settings = db.execute(
            "SELECT * FROM csv_import_settings WHERE is_active=1"
        ).fetchall()

    all_results = []

    for cfg in settings:
        connected_unc = ''
        pattern = cfg['filename_pattern'] or '*{yyyymmdd}.csv'

        # ── ネットワーク共有フォルダ認証 ────────────────────────
        net_user = (cfg.get('net_user') or '').strip()
        net_pass = (cfg.get('net_pass') or '').strip()
        ok, unc_or_err = _net_use_connect(cfg['folder_path'], net_user, net_pass)
        if not ok:
            all_results.append({'name': cfg['name'], 'status': 'error',
                                'detail': f'フォルダ接続エラー: {unc_or_err}'})
            continue
        connected_unc = unc_or_err

        try:
            files = find_csv_files(cfg['folder_path'], pattern, target_date, all_files=all_files)
        except Exception as _fe:
            _net_use_disconnect(connected_unc)
            connected_unc = ''
            all_results.append({'name': cfg['name'], 'status': 'error',
                                'detail': f'フォルダ読込エラー: {_fe}'})
            continue

        if not files:
            _net_use_disconnect(connected_unc)
            connected_unc = ''
            resolved = resolve_filename_pattern(pattern, target_date)
            all_results.append({
                'name': cfg['name'], 'status': 'skip',
                'detail': f"対象ファイルなし ({cfg['folder_path']}/{resolved})"
            })
            continue

        for csv_path in files:
            rows_ok = rows_err = rows_skip = rows_skip_unrg = 0
            unrg_jans = {}
            errors = []
            import_type = cfg['import_type'] or 'sales'
            # チェーンCD・店舗CD除外キャッシュ（DB問い合わせ削減）
            _chain_exclude_cache = {}
            _store_exclude_cache = {}

            # 総行数カウント
            try:
                with open(csv_path, encoding=cfg['encoding'] or 'utf-8-sig', errors='replace') as _fc:
                    total_rows = sum(1 for _ in _fc) - 1  # ヘッダー除く
            except Exception:
                total_rows = 0
            if progress_cb:
                progress_cb({'phase': 'start', 'file': csv_path.name,
                             'total': total_rows, 'ok': 0, 'skip': 0, 'err': 0})

            try:
                enc = cfg['encoding'] or 'utf-8-sig'
                with open(csv_path, encoding=enc, errors='replace') as f:
                    reader = csv.DictReader(f)

                    # ── フィルター設定（担当CDなど）──────────────
                    filter_col = (cfg.get('col_filter_cd') or '担当CD').strip()
                    raw_vals   = (cfg.get('filter_cd_values') or '').strip()
                    # カンマ・読点・スペース区切りで複数値をリスト化
                    filter_values = [
                        v.strip() for v in raw_vals.replace('、', ',').replace('　', ',').split(',')
                        if v.strip()
                    ] if raw_vals else []

                    rows_skip = 0
                    for i, row in enumerate(reader, 1):
                        if progress_cb and i % 100 == 0:
                            progress_cb({'phase': 'progress', 'file': csv_path.name,
                                         'current': i, 'total': total_rows,
                                         'ok': rows_ok, 'skip': rows_skip, 'err': rows_err})
                        try:
                            # ── 担当CDフィルター ─────────────────────
                            if filter_values and filter_col:
                                row_cd = str(row.get(filter_col, '') or '').strip()
                                if row_cd not in filter_values:
                                    rows_skip += 1
                                    continue

                            # ── JANコード ────────────────────────────
                            jan = _normalize_jan(str(row.get(cfg['col_jan'], '') or ''))
                            if not jan:
                                continue

                            # ── 数量計算（ケース×入数＋ピース 優先）─
                            qty = 0
                            cases_raw  = str(row.get('ケース',  '') or '').replace(',', '').strip()
                            pieces_raw = str(row.get('ピース',  '') or '').replace(',', '').strip()

                            if cases_raw or pieces_raw:
                                cases  = int(float(cases_raw))  if cases_raw  else 0
                                pieces = int(float(pieces_raw)) if pieces_raw else 0
                                # 入数列があれば使用、なければ商品マスタの unit_qty
                                unit_raw = str(row.get('入数', '') or '').replace(',', '').strip()
                                if unit_raw:
                                    unit = int(float(unit_raw))
                                else:
                                    p_tmp = db.execute(
                                        "SELECT unit_qty FROM products WHERE jan=%s AND is_active=1", [jan]
                                    ).fetchone()
                                    unit = p_tmp['unit_qty'] if p_tmp else 1
                                qty = cases * unit + pieces
                            else:
                                qty_raw = str(row.get(cfg['col_qty'], '0') or '0')
                                qty = int(float(qty_raw.replace(',', '').strip() or 0))

                            if qty <= 0:
                                continue

                            # ── 日付・賞味期限 ───────────────────────
                            sale_date = str(row.get(cfg['col_date'], '') or '').strip() or str(target_date)
                            # yyyymmdd -> yyyy-mm-dd に変換
                            if sale_date and len(sale_date) == 8 and sale_date.isdigit():
                                sale_date = f"{sale_date[:4]}-{sale_date[4:6]}-{sale_date[6:8]}" 
                            expiry    = str(row.get(cfg.get('col_expiry', '賞味期限'), '') or '').strip()

                            # ── 商品マスタ照合（任意）───────────────────────
                            product = db.execute(
                                "SELECT * FROM products WHERE jan=%s AND is_active=1", [jan]
                            ).fetchone()
                            # 商品マスタ未登録でもCSVデータは取り込む
                            product_name = product['product_name'] if product else jan

                            if import_type in ('sales', 'record_only'):
                                # 行ハッシュ（納品日 + チェーンCD + 伝票番号 + 行番号）で重複チェック
                                slip_col    = (cfg.get('col_slip_no')  or '伝票番号').strip()
                                chain_col   = (cfg.get('col_chain_cd') or 'チェーンCD').strip()
                                row_no_col  = (cfg.get('col_row_no')   or '行番号').strip()
                                client_col  = (cfg.get('col_client_name') or '社名').strip()
                                store_cd_col= (cfg.get('col_store_cd')  or '得意先CD').strip()
                                store_nm_col= (cfg.get('col_store_name') or '店舗名').strip()
                                slip_no     = str(row.get(slip_col,    '') or '').strip()
                                chain_cd    = str(row.get(chain_col,   '') or '').strip()
                                row_no      = str(row.get(row_no_col,  '') or '').strip()
                                client_name = str(row.get(client_col,  '') or '').strip()
                                store_cd    = str(row.get(store_cd_col,'') or '').strip()
                                store_name  = str(row.get(store_nm_col,'') or '').strip()
                                import hashlib as _hashlib
                                row_hash = _hashlib.md5(
                                    f"{sale_date}|{chain_cd}|{slip_no}|{row_no}".encode()
                                ).hexdigest()
                                dup = db.execute(
                                    "SELECT 1 FROM sales_history WHERE row_hash=%s",
                                    [row_hash]
                                ).fetchone()
                                if dup:
                                    rows_skip += 1
                                    continue
                                # チェーンCD・店舗CDの在庫引き当て除外チェック（キャッシュ使用）
                                exclude = False
                                if chain_cd:
                                    if chain_cd not in _chain_exclude_cache:
                                        cm = db.execute(
                                            "SELECT exclude_deduct FROM chain_masters WHERE chain_cd=%s",
                                            [chain_cd]
                                        ).fetchone()
                                        if cm:
                                            _chain_exclude_cache[chain_cd] = bool(cm['exclude_deduct'])
                                        else:
                                            db.execute(
                                                "INSERT INTO chain_masters (chain_cd,chain_name) VALUES (%s,%s) ON CONFLICT (chain_cd) DO NOTHING",
                                                [chain_cd, client_name]
                                            )
                                            _chain_exclude_cache[chain_cd] = False
                                    exclude = _chain_exclude_cache[chain_cd]
                                if store_cd and not exclude:
                                    if store_cd not in _store_exclude_cache:
                                        sm = db.execute(
                                            "SELECT exclude_deduct FROM store_masters WHERE store_cd=%s",
                                            [store_cd]
                                        ).fetchone()
                                        if sm:
                                            _store_exclude_cache[store_cd] = bool(sm['exclude_deduct'])
                                        else:
                                            db.execute(
                                                "INSERT INTO store_masters (store_cd,store_name,chain_cd,client_name) VALUES (%s,%s,%s,%s) ON CONFLICT (store_cd) DO NOTHING",
                                                [store_cd, store_name, chain_cd, client_name]
                                            )
                                            _store_exclude_cache[store_cd] = False
                                    exclude = _store_exclude_cache[store_cd]
                                if import_type == 'record_only':
                                    pass  # 引き当てなし
                                elif import_type == 'sales':
                                    if not product:
                                        rows_skip_unrg += 1
                                        unrg_jans[jan] = jan
                                    elif not exclude:
                                        _deduct_stock(db, product, qty, sale_date, csv_path.name)
                                result = db.execute("""
                                    INSERT INTO sales_history
                                    (jan,product_name,quantity,sale_date,source_file,row_hash,
                                     chain_cd,client_name,store_cd,store_name)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                    ON CONFLICT (row_hash) WHERE row_hash IS NOT NULL AND row_hash <> '' DO NOTHING
                                """, [jan, product_name, qty, sale_date, csv_path.name, row_hash,
                                        chain_cd, client_name, store_cd, store_name])
                                if result.rowcount == 0:
                                    rows_skip += 1
                                    rows_ok -= 1
                            else:
                                if not product:
                                    rows_skip_unrg += 1
                                    unrg_jans[jan] = jan
                                else:
                                    _add_stock(db, product, qty, expiry, csv_path.name)

                            rows_ok += 1

                        except Exception as e:
                            errors.append(f"行{i}: {e}")
                            rows_err += 1

                        # 1000行ごとに中間コミット（DBロック防止）
                        if i % 1000 == 0:
                            try:
                                db.commit()
                            except Exception as ce:
                                db.rollback()
                                errors.append(f"中間コミットエラー: {ce}")

                db.commit()
                skip_note = f" スキップ{rows_skip}行(重複・フィルター)" if rows_skip else ""
                unrg_note = f" 未登録商品スキップ{rows_skip_unrg}行(商品登録後に再取込可)" if rows_skip_unrg else ""
                detail = f"成功{rows_ok}行{skip_note}{unrg_note}" + (f" エラー{rows_err}行: {'; '.join(errors[:3])}" if errors else "")
                if rows_skip_unrg > 0 and rows_err == 0:
                    status = 'partial_skip'
                elif rows_err == 0:
                    status = 'ok'
                else:
                    status = 'partial'
                db.execute("""
                    INSERT INTO import_logs (setting_id,filename,rows_ok,rows_err,status,detail,trigger_type,unrg_jans_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, [cfg['id'], csv_path.name, rows_ok, rows_err, status, detail, trigger_type,
                      json.dumps(sorted(unrg_jans.keys()), ensure_ascii=False) if unrg_jans else ''])
                db.execute("""
                    UPDATE csv_import_settings
                    SET last_run_at=NOW(), last_result=%s
                    WHERE id=%s
                """, [detail, cfg['id']])
                db.commit()
                all_results.append({
                    'name': cfg['name'], 'file': csv_path.name,
                    'status': status, 'rows_ok': rows_ok, 'rows_err': rows_err,
                    'detail': detail
                })
                # CSV取込後の自動発注チェックは廃止（誤発注防止）
                # run_order_check() は毎日スケジューラーのみで実行

            except Exception as e:
                db.execute("""
                    INSERT INTO import_logs (setting_id,filename,rows_ok,rows_err,status,detail,trigger_type)
                    VALUES (%s,%s,0,0,'error',%s,%s)
                """, [cfg['id'], csv_path.name, str(e), trigger_type])
                db.commit()
                all_results.append({
                    'name': cfg['name'], 'file': csv_path.name,
                    'status': 'error', 'detail': str(e)
                })

        # UNC接続を切断
        _net_use_disconnect(connected_unc)

    # 売上・在庫データが更新されたため予測関連を更新（遅延インポートで循環参照を回避）
    try:
        import sys as _sys
        if 'app' in _sys.modules:
            _app = _sys.modules['app']
            threading.Thread(target=_app._bg_refresh_sales_daily_agg, daemon=True).start()
            _app.invalidate_forecast_cache(background_refresh=True)
    except Exception:
        pass

    # 売上異常値チェック（取込完了後）
    try:
        _check_sales_anomaly(get_db_long(), target_date)
    except Exception as _ae:
        logger.warning(f'[AnomalyCheck] エラー: {_ae}')

    return all_results


def _check_sales_anomaly(db, target_date=None):
    """
    取込した日付の日次売上が前週同曜日比 3倍超かつ10個以上の場合に
    alert_logs へ '売上異常値' アラートを記録する。
    """
    if not target_date:
        target_date = str(date.today())
    try:
        prev_date = str(date.fromisoformat(str(target_date)) - timedelta(days=7))
    except Exception:
        return
    # 当日の JAN 別合計
    today_rows = db.execute("""
        SELECT jan, SUM(quantity) AS qty
        FROM sales_history
        WHERE sale_date = %s
        GROUP BY jan
    """, [target_date]).fetchall()
    if not today_rows:
        return
    # 前週同曜日の JAN 別合計
    prev_rows = db.execute("""
        SELECT jan, SUM(quantity) AS qty
        FROM sales_history
        WHERE sale_date = %s
        GROUP BY jan
    """, [prev_date]).fetchall()
    prev_map = {r['jan']: int(r['qty'] or 0) for r in prev_rows}

    for r in today_rows:
        jan = r['jan']
        today_qty = int(r['qty'] or 0)
        prev_qty  = prev_map.get(jan, 0)
        if today_qty < 10:
            continue  # 少量は無視
        if prev_qty > 0 and today_qty >= prev_qty * 3:
            prod = db.execute(
                "SELECT product_name FROM products WHERE jan=%s LIMIT 1", [jan]
            ).fetchone()
            pname = prod['product_name'] if prod else jan
            # 同日・同JANの重複アラートを防ぐ
            exists = db.execute("""
                SELECT 1 FROM alert_logs
                WHERE alert_type='売上異常値' AND jan=%s
                  AND created_at::date = %s::date
                LIMIT 1
            """, [jan, target_date]).fetchone()
            if not exists:
                db.execute("""
                    INSERT INTO alert_logs (alert_type, jan, product_name, message, mail_sent)
                    VALUES ('売上異常値', %s, %s, %s, 0)
                """, [jan, pname,
                      f"前週比 {round(today_qty/prev_qty,1)}倍 ({prev_qty}→{today_qty}個) [{target_date}]"])
                db.commit()
                logger.info(f'[AnomalyCheck] 売上異常値 JAN:{jan} {prev_qty}→{today_qty}個')


def _calc_forecast_accuracy(db):
    """30日前の予測値と実績を比較してMAPEをforecast_accuracyテーブルへUPSERT"""
    from datetime import date, timedelta
    target_dt = date.today() - timedelta(days=30)

    # forecast_cache から30日前時点の予測値を取得（q50_daily × 30 を予測量とする）
    fc_rows = db.execute("""
        SELECT jan, q50_daily
        FROM forecast_cache
        WHERE updated_at::date = %s::date
          AND q50_daily IS NOT NULL AND q50_daily > 0
    """, [target_dt]).fetchall()
    if not fc_rows:
        logger.info('[MAPE] forecast_cache に対象データなし（30日前: %s）', target_dt)
        return

    jans = [r['jan'] for r in fc_rows]
    predicted_map = {r['jan']: float(r['q50_daily']) * 30 for r in fc_rows}

    # sales_history から30日間の実績合計を取得
    actual_rows = db.execute("""
        SELECT jan, SUM(quantity) AS actual_qty
        FROM sales_history
        WHERE jan = ANY(%s)
          AND sale_date::date >= %s::date
          AND sale_date::date < %s::date
        GROUP BY jan
    """, [jans, target_dt, date.today()]).fetchall()
    actual_map = {r['jan']: float(r['actual_qty']) for r in actual_rows}

    upserted = 0
    for jan, predicted in predicted_map.items():
        actual = actual_map.get(jan)
        if actual is None or actual == 0:
            continue
        mape = abs(predicted - actual) / actual * 100
        db.execute("""
            INSERT INTO forecast_accuracy (jan, forecast_dt, predicted, actual, mape, calc_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (jan, forecast_dt) DO UPDATE
              SET predicted = EXCLUDED.predicted,
                  actual    = EXCLUDED.actual,
                  mape      = EXCLUDED.mape,
                  calc_date = EXCLUDED.calc_date
        """, [jan, target_dt, predicted, actual, round(mape, 2), date.today()])
        upserted += 1

    db.commit()
    logger.info('[MAPE] %d 件のMAPE更新完了（基準日: %s）', upserted, target_dt)


def _toggle_seasonal_products(db, today=None):
    """season_start_mmdd / season_end_mmdd に基づいて商品を自動有効化/無効化"""
    from datetime import date
    if today is None:
        today = date.today()
    mmdd = today.strftime('%m-%d')

    # 季節設定のある商品を全取得
    rows = db.execute("""
        SELECT id, jan, product_name, is_active, season_start_mmdd, season_end_mmdd
        FROM products
        WHERE season_start_mmdd IS NOT NULL AND season_end_mmdd IS NOT NULL
          AND season_start_mmdd <> '' AND season_end_mmdd <> ''
    """).fetchall()

    activated = deactivated = 0
    for r in rows:
        start = r['season_start_mmdd']  # MM-DD
        end   = r['season_end_mmdd']    # MM-DD
        # 期間内かどうか判定（年跨ぎ対応: start > end なら12月〜翌年などを許容）
        if start <= end:
            in_season = start <= mmdd <= end
        else:
            in_season = mmdd >= start or mmdd <= end

        new_active = 1 if in_season else 0
        if r['is_active'] != new_active:
            db.execute("UPDATE products SET is_active=%s WHERE id=%s", [new_active, r['id']])
            action = '有効化' if new_active else '無効化'
            logger.info(f'[Season] {action} JAN:{r["jan"]} {r["product_name"]} ({start}〜{end})')
            if new_active:
                activated += 1
            else:
                deactivated += 1

    if activated or deactivated:
        db.commit()
    logger.info(f'[Season] 季節品切替完了: 有効化={activated} 無効化={deactivated} ({mmdd})')


def _deduct_stock(db, product, qty_to_deduct, sale_date, source_file):
    """FIFO: 賞味期限が近い在庫から順番に引き算"""
    jan = product['jan']
    remaining = qty_to_deduct
    stocks = db.execute("""
        SELECT * FROM stocks WHERE jan=%s AND quantity>0
        ORDER BY CASE WHEN expiry_date='' THEN '9999-99-99' ELSE expiry_date END ASC
        FOR UPDATE
    """, [jan]).fetchall()
    before_total = sum(s['quantity'] for s in stocks)
    for s in stocks:
        if remaining <= 0:
            break
        deduct = min(s['quantity'], remaining)
        db.execute("UPDATE stocks SET quantity=quantity-%s WHERE id=%s", [deduct, s['id']])
        remaining -= deduct
    actual_deducted = qty_to_deduct - remaining
    after_total = before_total - actual_deducted
    db.execute("""
        INSERT INTO stock_movements
        (jan,product_name,move_type,quantity,before_qty,after_qty,note,source_file,move_date)
        VALUES (%s,%s,'sale',%s,%s,%s,%s,%s,%s)
    """, [jan, product['product_name'], actual_deducted,
          before_total, after_total, f"CSV販売取込: {source_file}", source_file, sale_date])


def _add_stock(db, product, qty, expiry, source_file):
    """入庫: 在庫に加算"""
    jan = product['jan']
    before = db.execute(
        "SELECT COALESCE(SUM(quantity),0) AS _sum FROM stocks WHERE jan=%s", [jan]
    ).fetchone()['_sum']
    db.execute("""
        INSERT INTO stocks
        (product_id,jan,product_name,supplier_cd,supplier_name,
         product_cd,unit_qty,quantity,expiry_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, [product['id'], jan, product['product_name'],
          product['supplier_cd'], product['supplier_name'],
          product['product_cd'], product['unit_qty'], qty, expiry])
    db.execute("""
        INSERT INTO stock_movements
        (jan,product_name,move_type,quantity,before_qty,after_qty,note,source_file,move_date)
        VALUES (%s,%s,'receipt',%s,%s,%s,%s,%s,%s)
    """, [jan, product['product_name'], qty, before, before + qty,
          f"CSV入庫取込: {source_file}", source_file, str(date.today())])
    # 入庫したら発注済みフラグをクリア
    db.execute("UPDATE products SET ordered_at='' WHERE jan=%s", [jan])


# ─── 発注チェック（混載ロット対応）──────────────────────────────
def run_order_check():
    """
    発注点以下 or ロット数以上 → 混載グループ条件を確認してから発注
    ・混載グループあり → ペンディングキューに積み、グループ合計ケースがlot_cases以上で一括発注
    ・mixed_force_days日超過 → 強制発注
    ・混載グループなし → 即時発注
    """
    db = get_db_long()
    today = str(date.today())
    results = []

    for p in db.execute("SELECT * FROM products WHERE is_active=1 AND (ordered_at IS NULL OR ordered_at='')").fetchall():
        # 有効在庫（賞味期限アラート日数を超えて出荷できる在庫）
        alert_days = p['expiry_alert_days'] or 30
        valid_stock = db.execute("""
            SELECT COALESCE(SUM(quantity),0) AS _sum FROM stocks
            WHERE jan=%s AND quantity>0
            AND (expiry_date=''
                 OR expiry_date::date > (CURRENT_DATE + INTERVAL '1 day' * %s))
        """, [p['jan'], int(alert_days)]).fetchone()['_sum']

        stock_qty = db.execute(
            "SELECT COALESCE(SUM(quantity),0) AS _sum FROM stocks WHERE jan=%s", [p['jan']]
        ).fetchone()['_sum']

        # 本日発注確定済み → スキップ
        already_ordered = db.execute("""
            SELECT COUNT(*) AS _cnt FROM order_history
            WHERE jan=%s AND order_date=%s AND trigger_type IN ('reorder','lot','mixed','forced')
        """, [p['jan'], today]).fetchone()['_cnt']
        if already_ordered:
            continue

        # 既にペンディング中 → スキップ（後でグループチェックに任せる）
        already_pending = db.execute(
            "SELECT COUNT(*) AS _cnt FROM order_pending WHERE jan=%s AND status='pending'",
            [p['jan']]
        ).fetchone()['_cnt']
        if already_pending:
            continue

        # 発注トリガー判定
        trigger = None
        if p['reorder_point'] > 0 and valid_stock <= p['reorder_point']:
            trigger = 'reorder'
        elif p['lot_size'] > 0 and stock_qty >= p['lot_size']:
            trigger = 'lot'
        if not trigger:
            continue

        # ケース数（order_qty ÷ unit_qty を切り上げ、最低1ケース）
        # order_cases = 不足数 ÷ order_qty（何ロット必要か）
        _shortage = max(0, int(p['reorder_point'] or 0) - int(valid_stock))
        order_cases = max(1, math.ceil(_shortage / max(int(p['order_qty'] or 1), 1)))

        # ── 発注数量をケース単位（unit_qty）に切り上げ ──
        unit_qty = int(p.get('unit_qty') or 1)
        base_order_qty = int(p.get('order_qty') or unit_qty)
        if unit_qty > 1 and base_order_qty % unit_qty != 0:
            base_order_qty = math.ceil(base_order_qty / unit_qty) * unit_qty

        if p['mixed_group']:
            # ── 混載グループあり → ペンディングキューに積む ──
            force_date = str(date.today() + timedelta(days=int(p['mixed_force_days'] or 3)))
            db.execute("""
                INSERT INTO order_pending
                (supplier_cd,supplier_name,supplier_email,mixed_group,
                 mixed_lot_mode,mixed_lot_cases,
                 jan,product_cd,product_name,order_qty,order_cases,
                 trigger_type,pending_since,force_send_date,status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending')
            """, [p['supplier_cd'], p['supplier_name'], p['supplier_email'],
                  p['mixed_group'],
                  p['mixed_lot_mode'] or 'gte',
                  int(p['mixed_lot_cases'] or 3),
                  p['jan'], p['product_cd'], p['product_name'],
                  p['order_qty'], order_cases, trigger, today, force_date])
            results.append({
                'product': p['product_name'], 'trigger': trigger,
                'status': 'pending', 'group': p['mixed_group'],
                 # order_qty は既に unit_qty 切り上げ済み
            })
        else:
            # ── 混載グループなし → 即時発注 ──
            ok, msg = _do_order(db, p, p['order_qty'], trigger, today)
            results.append({
                'product': p['product_name'], 'trigger': trigger,
                'status': 'sent' if ok else 'error', 'msg': msg,
            })

    # ペンディング登録を確定してから混載グループチェック
    db.commit()
    _check_mixed_groups(db, today, results)
    db.close()
    # 全発注をまとめてメール送信
    ok, msg = flush_order_mail()
    if results:
        logger.info(f'[Order] メール送信: {msg}')
        # 本日の発注履歴のmail_sent/mail_resultを更新
        try:
            db2 = get_db_long()
            db2.execute(
                "UPDATE order_history SET mail_sent=%s, mail_result=%s WHERE order_date=%s AND mail_sent=0",
                [1 if ok else 0, msg, today]
            )
            db2.commit()
            db2.close()
        except Exception as e:
            logger.warning(f'[Order] mail_result更新エラー: {e}')
    return results


def _do_order(db, product, order_qty, trigger, today):
    """発注履歴に記録してメール送信（ケース単位に切り上げ）"""
    unit_qty = int(product.get('unit_qty') or 1)
    if unit_qty > 1 and order_qty % unit_qty != 0:
        order_qty = math.ceil(order_qty / unit_qty) * unit_qty
    db.execute("""
        INSERT INTO order_history
        (supplier_cd,supplier_name,supplier_email,jan,product_cd,product_name,
         order_qty,trigger_type,order_date,mail_sent,mail_result)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,0,'')
    """, [product['supplier_cd'], product['supplier_name'], product['supplier_email'],
          product['jan'], product['product_cd'], product['product_name'],
          order_qty, trigger, today])
    # 発注済みフラグをセット（翌日の重複発注を防止）
    db.execute("UPDATE products SET ordered_at=%s WHERE jan=%s",
               [str(today), product['jan']])
    db.commit()
    # メールはキューに追加（run_order_check完了後にまとめて送信）
    queue_order(dict(product), order_qty, trigger)
    db.execute(
        "SELECT id FROM order_history WHERE jan=%s AND order_date=%s ORDER BY id DESC LIMIT 1",
        [product['jan'], today]).fetchone()
    label = {'reorder':'発注点到達','lot':'ロット数到達',
             'mixed':'混載ロット達成','forced':'期限強制発注','forced_manual':'手動調整強制発注'}.get(trigger, trigger)
    db.execute("""
        INSERT INTO alert_logs (alert_type,jan,product_name,message,mail_sent)
        VALUES (%s,%s,%s,%s,%s)
    """, [label, product['jan'], product['product_name'],
          f"発注{order_qty}個", 0])
    db.commit()
    return True, 'queued'


def _check_mixed_groups(db, today, results):
    """
    mixed_lot_mode='gte'  : グループ合計 >= mixed_lot_cases で発注
    mixed_lot_mode='unit' : グループ合計を mixed_lot_cases の倍数に切り上げて発注
    force_send_date 超過  : どちらのモードでも強制発注
    """
    groups = db.execute("""
        SELECT DISTINCT op.mixed_group, p.mixed_lot_mode, p.mixed_lot_cases
        FROM order_pending op
        JOIN products p ON op.jan = p.jan
        WHERE op.status='pending'
    """).fetchall()

    for g in groups:
        group_name = g['mixed_group']
        mode       = g['mixed_lot_mode'] or 'gte'
        lot_cases  = int(g['mixed_lot_cases'] or 3)

        items = db.execute("""
            SELECT op.*, p.mixed_force_days, p.mixed_lot_mode,
                   p.mixed_lot_cases, p.unit_qty
            FROM order_pending op
            JOIN products p ON op.jan = p.jan
            WHERE op.mixed_group=%s AND op.status='pending'
            ORDER BY op.pending_since ASC
        """, [group_name]).fetchall()

        if not items:
            continue

        total_cases   = sum(it['order_cases'] for it in items)
        oldest        = items[0]
        force_exceeded = oldest['force_send_date'] < today

        # ── 発注判定 ──────────────────────────────────
        if mode == 'gte':
            # 以上モード: 合計が lot_cases 以上なら発注可
            ready = total_cases >= lot_cases
        else:
            # 単位モード: 合計を lot_cases の倍数に切り上げ（常に発注可だが数量を補正）
            ready = True  # 単位モードはケースが積み上がり次第発注

        if not ready and not force_exceeded:
            continue  # 未達かつ期限内 → 保留継続

        trigger_type = 'forced' if force_exceeded and not ready else 'mixed'

        # ── 発注数量の決定 ────────────────────────────
        if mode == 'unit':
            # 単位モード: グループ合計を lot_cases 倍数に切り上げ
            adj_total = math.ceil(total_cases / lot_cases) * lot_cases
        else:
            # 以上モード: 各アイテムの order_qty をそのまま使用
            # 強制発注時: lot_casesに合わせて均等配分
            adj_total = lot_cases if force_exceeded else total_cases

        for it in items:
            product = db.execute(
                "SELECT * FROM products WHERE jan=%s", [it['jan']]
            ).fetchone()
            if not product:
                continue

            if mode == 'unit' and adj_total != total_cases:
                # 単位モードで切り上げが発生した場合
                # 比率を保って各アイテムの qty を調整
                ratio     = adj_total / total_cases if total_cases else 1
                order_qty = math.ceil(it['order_cases'] * ratio) * int(product['unit_qty'] or 1)
            elif force_exceeded and not ready and mode == 'gte' and total_cases < lot_cases:
                # 強制発注均等配分: 不足分をアイテム数で割り振り
                shortage = lot_cases - total_cases
                n = len(items)
                # 在庫数が少ない順にソートして余りを配分
                stocks_map2 = {}
                for _it in items:
                    _st = db.execute(
                        "SELECT COALESCE(SUM(quantity),0) AS s FROM stocks WHERE jan=%s",
                        [_it['jan']]
                    ).fetchone()['s']
                    stocks_map2[_it['jan']] = int(_st)
                items_by_stock = sorted(items, key=lambda x: stocks_map2.get(x['jan'], 0))
                idx2 = [x['jan'] for x in items_by_stock].index(it['jan'])
                extra = shortage // n + (1 if idx2 < shortage % n else 0)
                order_qty = (it['order_cases'] + extra) * int(it['order_qty'] or 1)
            elif force_exceeded and not ready and mode == 'gte' and total_cases < lot_cases:
                shortage2 = lot_cases - total_cases
                n2 = len(items)
                items_by_cases = sorted(items, key=lambda x: x['order_cases'])
                idx2 = [x['jan'] for x in items_by_cases].index(it['jan'])
                extra2 = shortage2 // n2 + (1 if idx2 < shortage2 % n2 else 0)
                order_qty = (it['order_cases'] + extra2) * int(product['unit_qty'] or 1)
            else:
                order_qty = it['order_qty']

            ok, msg = _do_order(db, product, order_qty, trigger_type, today)
            db.execute("UPDATE order_pending SET status='sent' WHERE id=%s", [it['id']])
            db.commit()

            days_p = (
                (date.today() - date.fromisoformat(oldest['pending_since'])).days
                if force_exceeded else 0
            )
            results.append({
                'product':      it['product_name'],
                'trigger':      trigger_type,
                'status':       'sent' if ok else 'error',
                'group':        group_name,
                'mode':         mode,
                'total_cases':  total_cases,
                'adj_cases':    adj_total,
                'lot_cases':    lot_cases,
                'forced':       force_exceeded,
                'days_pending': days_p,
                'msg':          msg,
            })


def get_pending_orders(db):
    """画面表示用: ペンディング中の発注一覧（グループ集計つき）"""
    items = db.execute("""
        SELECT op.*,
               p.mixed_lot_mode,
               p.mixed_lot_cases,
               p.mixed_force_days,
               p.unit_qty,
               (SELECT SUM(op2.order_cases)
                FROM order_pending op2
                WHERE op2.mixed_group = op.mixed_group
                AND op2.status = 'pending') as group_total_cases
        FROM order_pending op
        JOIN products p ON op.jan = p.jan
        WHERE op.status = 'pending'
        ORDER BY op.mixed_group, op.pending_since
    """).fetchall()
    return items


# ─── 賞味期限アラート ────────────────────────────────────────────
def run_expiry_check():
    db = get_db_long()
    today = date.today()
    products = {p['jan']: p for p in
                db.execute("SELECT * FROM products WHERE is_active=1").fetchall()}
    alert_items = []
    for s in db.execute(
        "SELECT * FROM stocks WHERE quantity>0 AND expiry_date!='' ORDER BY expiry_date ASC"
    ).fetchall():
        p = products.get(s['jan'])
        if not p:
            continue
        try:
            exp = date.fromisoformat(s['expiry_date'])
        except ValueError:
            continue
        days_left = (exp - today).days
        if days_left <= (p['expiry_alert_days'] or 30):
            already = db.execute("""
                SELECT COUNT(*) AS _cnt FROM alert_logs
                WHERE alert_type='賞味期限アラート' AND jan=%s
                AND message LIKE %s
                AND created_at::date >= CURRENT_DATE - INTERVAL '7 days'
            """, [s['jan'], f"%（{s['expiry_date']}）%"]).fetchone()['_cnt']
            if not already:
                alert_items.append({
                    'jan':          s['jan'],
                    'product_name': s['product_name'],
                    'product_cd':   p.get('product_cd') or '',
                    'supplier_cd':  p.get('supplier_cd') or '',
                    'supplier_name':p.get('supplier_name') or '',
                    'lot_no':       s.get('lot_no') or '',
                    'quantity':     s['quantity'],
                    'expiry_date':  s['expiry_date'],
                    'days_left':    days_left,
                })
    if not alert_items:
        return []
    ok, msg = send_expiry_alert(db, alert_items)
    for it in alert_items:
        db.execute("""
            INSERT INTO alert_logs (alert_type,jan,product_name,message,mail_sent)
            VALUES ('賞味期限アラート',%s,%s,%s,%s)
        """, [it['jan'], it['product_name'],
              f"残{it['days_left']}日（{it['expiry_date']}）在庫{it['quantity']}個",
              1 if ok else 0])
    db.commit()
    db.close()
    return alert_items


# ─── 発注点自動更新（モード切替対応）────────────────────────────
def update_reorder_points():
    """
    発注点・発注数自動計算（毎月1日 自動実行）

    商品ごとの reorder_auto 値に応じてモードを振り分ける:
      reorder_auto=1 → AIモード（需要予測AIエンジンを使用）
      reorder_auto=2 → 前年実績モード（前年同月実績ベース）
      reorder_auto=0 → 手動（更新しない）
    """
    db = get_db_long()
    ai_cnt  = db.execute("SELECT COUNT(*) AS c FROM products WHERE is_active=1 AND reorder_auto=1").fetchone()['c']
    ly_cnt  = db.execute("SELECT COUNT(*) AS c FROM products WHERE is_active=1 AND reorder_auto=2").fetchone()['c']
    logger.info(f'[update_reorder_points] AIモード={ai_cnt}商品 / 前年実績モード={ly_cnt}商品')

    updated_ai = _update_reorder_points_ai(db) if ai_cnt > 0 else 0
    # _update_reorder_points_ly は自前で DB 接続を開くため、ai 側で使った db は閉じる
    # ただし ai が db.close() 済みのため、ly 用に新規接続
    updated_ly = _update_reorder_points_ly() if ly_cnt > 0 else 0
    return (updated_ai or 0) + (updated_ly or 0)


def _update_reorder_points_ai(db):
    """
    AIモード: app._build_forecast_rows_raw() の予測結果を元に発注点・発注数を更新。
    ダンピング（50%〜200%）・ロット丸め・変動閾値チェックを適用。
    """
    import sys as _sys

    # app モジュールから予測行を取得（遅延インポートで循環参照を回避）
    try:
        if 'app' not in _sys.modules:
            logger.warning('[update_reorder_points] app未ロード → 前年実績モードで代替実行')
            return _update_reorder_points_ly(db)
        _app = _sys.modules['app']
        forecast_rows = _app._build_forecast_rows_raw(db)
    except Exception as e:
        logger.warning(f'[update_reorder_points] AI予測取得エラー: {e} → 前年実績モードで代替実行')
        return _update_reorder_points_ly(db)

    # reorder_auto=1（AIモード）の商品セットを取得
    auto_jans = {r['jan'] for r in db.execute(
        "SELECT jan FROM products WHERE is_active=1 AND reorder_auto=1"
    ).fetchall()}

    updated = 0
    for r in forecast_rows:
        if r['jan'] not in auto_jans:
            continue

        new_rp = int(r.get('suggested_reorder_point') or 0)
        new_oq = int(r.get('suggested_order_qty')     or 0)
        if new_rp == 0 and new_oq == 0:
            continue   # 売上データなし

        unit_qty   = max(1, int(r.get('unit_qty')    or 1))
        order_unit = max(1, int(r.get('order_unit')  or unit_qty))
        current_rp = int(r.get('reorder_point') or 0)
        current_oq = int(r.get('order_qty')     or 0)
        locked_oq  = int(r.get('lock_order_qty') or 0)

        # ── 急激な変動を抑制（50%〜200% の範囲に制限）──
        if current_rp > 0:
            clamped = max(current_rp * 0.5, min(current_rp * 2.0, new_rp))
            new_rp = math.ceil(clamped / order_unit) * order_unit if order_unit > 1 else int(clamped + 0.9999)
            new_rp = max(order_unit, new_rp)
        if current_oq > 0:
            clamped = max(current_oq * 0.5, min(current_oq * 2.0, new_oq))
            new_oq = math.ceil(clamped / unit_qty) * unit_qty if unit_qty > 1 else int(clamped + 0.9999)
            new_oq = max(unit_qty, new_oq)

        # ── 変動幅が小さすぎる場合はスキップ（5% 未満かつ unit 未満の差）──
        rp_changed = (new_rp != current_rp) and (
            current_rp == 0
            or abs(new_rp - current_rp) / max(current_rp, 1) >= 0.05
            or abs(new_rp - current_rp) >= order_unit
        )
        oq_changed = (not locked_oq) and (new_oq != current_oq) and (
            current_oq == 0
            or abs(new_oq - current_oq) / max(current_oq, 1) >= 0.05
            or abs(new_oq - current_oq) >= unit_qty
        )
        if not rp_changed and not oq_changed:
            continue

        changes       = []
        update_fields = []
        update_vals   = []
        if rp_changed:
            update_fields.append("reorder_point=%s")
            update_vals.append(new_rp)
            changes.append(f"発注点 {current_rp}→{new_rp}")
        if oq_changed:
            update_fields.append("order_qty=%s")
            update_vals.append(new_oq)
            changes.append(f"発注数 {current_oq}→{new_oq}")

        update_vals.append(r['product_id'])
        db.execute(
            f"UPDATE products SET {', '.join(update_fields)} WHERE id=%s",
            update_vals
        )
        mode_label = r.get('rp_mode_label', 'AI')
        daily = round(float(r.get('next_daily_forecast') or 0), 2)
        db.execute("""
            INSERT INTO alert_logs (alert_type, jan, product_name, message)
            VALUES ('発注点更新', %s, %s, %s)
        """, [r['jan'], r['product_name'],
              f"AIモード({mode_label}) 日次予測{daily}個/日"
              f" LT{r.get('lead_time_days') or 3}日"
              f" ロット{order_unit}/入数{unit_qty}"
              f" → {' / '.join(changes)}"])
        updated += 1

    db.commit()
    db.close()
    return updated


def _update_reorder_points_ly(db=None):
    """
    前年実績モード: reorder_auto=2 の商品を前年同月実績ベースで更新。

    計算式:
      発注点  = 日次平均 × リードタイム × 安全係数 → order_unit の倍数に切り上げ
      発注数  = 日次平均 × (リードタイム + 14日)   → unit_qty の倍数に切り上げ

    急激な変動抑制:
      - 現行値の 50%〜200% の範囲に制限
      - 変動幅が 5% 未満かつ unit 単位未満の場合は更新スキップ

    賞味期限考慮:
      - 期限切れ間近の在庫がある場合は計算値を 10% 上乗せ
    """
    if db is None:
        db = get_db_long()
    today = date.today()
    last_year = today.year - 1
    month = today.month
    days_in_month = calendar.monthrange(last_year, month)[1]
    # sale_date は TEXT 型 → BETWEEN 文字列比較でインデックスを活用
    ly_start = f'{last_year}-{month:02d}-01'
    ly_end   = f'{last_year}-{month:02d}-{days_in_month:02d}'
    updated = 0

    for p in db.execute(
        "SELECT * FROM products WHERE is_active=1 AND reorder_auto=2"
    ).fetchall():
        # 前年同月の合計出荷数（インデックス有効な BETWEEN 比較）
        total = db.execute("""
            SELECT COALESCE(SUM(quantity),0) AS _sum FROM sales_history
            WHERE jan=%s AND sale_date BETWEEN %s AND %s
        """, [p['jan'], ly_start, ly_end]).fetchone()['_sum']

        if total == 0:
            continue

        lead     = max(1, int(p['lead_time_days'] or 3))
        safety   = max(1.0, float(p['safety_factor'] or 1.3))
        unit_qty  = max(1, int(p['unit_qty']   or 1))
        order_unit = max(1, int(p['order_unit'] or unit_qty))
        daily_avg = total / days_in_month

        # 基本計算
        raw_rp = daily_avg * lead * safety
        raw_oq = daily_avg * (lead + 14)

        # 賞味期限考慮: 期限切れ間近在庫がある場合は 10% 上乗せ
        alert_days = p['expiry_alert_days'] or 30
        expiry_risk_qty = db.execute("""
            SELECT COALESCE(SUM(quantity),0) AS _sum FROM stocks
            WHERE jan=%s AND quantity>0 AND expiry_date!=''
            AND expiry_date::date <= (CURRENT_DATE + INTERVAL '1 day' * %s)
        """, [p['jan'], alert_days]).fetchone()['_sum']
        if expiry_risk_qty > 0:
            raw_rp *= 1.1
            raw_oq *= 1.1

        # ── 急激な変動を抑制（現行値の 50%〜200% の範囲に制限）──
        current_rp = int(p['reorder_point'] or 0)
        current_oq = int(p['order_qty'] or 0)
        if current_rp > 0:
            raw_rp = max(current_rp * 0.5, min(current_rp * 2.0, raw_rp))
        if current_oq > 0:
            raw_oq = max(current_oq * 0.5, min(current_oq * 2.0, raw_oq))

        # ── ロット単位に切り上げ ──
        # 発注点: order_unit の倍数（例: raw_rp=180, order_unit=200 → 200）
        new_rp = math.ceil(raw_rp / order_unit) * order_unit if order_unit > 1 else int(raw_rp + 0.9999)
        new_rp = max(order_unit, new_rp)

        # 発注数: unit_qty の倍数（例: raw_oq=350, unit_qty=200 → 400）
        new_oq = math.ceil(raw_oq / unit_qty) * unit_qty if unit_qty > 1 else int(raw_oq + 0.9999)
        new_oq = max(unit_qty, new_oq)

        locked_oq = int(p.get('lock_order_qty') or 0)

        # ── 変動幅が小さすぎる場合はスキップ（5% 未満かつ unit 未満の差）──
        rp_changed = (new_rp != current_rp) and (
            current_rp == 0 or abs(new_rp - current_rp) / current_rp >= 0.05
            or abs(new_rp - current_rp) >= order_unit
        )
        oq_changed = (not locked_oq) and (new_oq != current_oq) and (
            current_oq == 0 or abs(new_oq - current_oq) / max(current_oq, 1) >= 0.05
            or abs(new_oq - current_oq) >= unit_qty
        )

        if not rp_changed and not oq_changed:
            continue

        expiry_note = f"（期限リスク在庫{expiry_risk_qty}個+10%）" if expiry_risk_qty > 0 else ""
        changes = []
        update_fields = []
        update_vals   = []

        if rp_changed:
            update_fields.append("reorder_point=%s")
            update_vals.append(new_rp)
            changes.append(f"発注点 {current_rp}→{new_rp}")

        if oq_changed:
            update_fields.append("order_qty=%s")
            update_vals.append(new_oq)
            changes.append(f"発注数 {current_oq}→{new_oq}")

        update_vals.append(p['id'])
        db.execute(
            f"UPDATE products SET {', '.join(update_fields)} WHERE id=%s",
            update_vals
        )
        db.execute("""
            INSERT INTO alert_logs (alert_type,jan,product_name,message)
            VALUES ('発注点更新',%s,%s,%s)
        """, [p['jan'], p['product_name'],
              f"前年実績 {last_year}/{month:02d} 合計{total}個÷{days_in_month}日"
              f" LT{lead}日 安全{safety} ロット{order_unit}/入数{unit_qty}"
              f" → {' / '.join(changes)}{expiry_note}"])
        updated += 1

    db.commit()
    db.close()
    return updated


# ─── 棚卸データ生成 ──────────────────────────────────────────────
def create_inventory_count(count_date: str = None):
    if count_date is None:
        count_date = str(date.today())
    db = get_db_long()
    inserted = 0
    for p in db.execute("SELECT * FROM products WHERE is_active=1").fetchall():
        sys_qty = db.execute(
            "SELECT COALESCE(SUM(quantity),0) AS _sum FROM stocks WHERE jan=%s", [p['jan']]
        ).fetchone()['_sum']
        existing = db.execute(
            "SELECT * FROM inventory_count WHERE count_date=%s AND jan=%s",
            [count_date, p['jan']]).fetchone()
        if existing:
            if not existing['adjusted']:
                db.execute("""
                    UPDATE inventory_count SET system_qty=%s, actual_qty=%s, diff_qty=0
                    WHERE count_date=%s AND jan=%s
                """, [sys_qty, sys_qty, count_date, p['jan']])
        else:
            db.execute("""
                INSERT INTO inventory_count
                (count_date,jan,product_name,system_qty,actual_qty,diff_qty)
                VALUES (%s,%s,%s,%s,%s,0)
            """, [count_date, p['jan'], p['product_name'], sys_qty, sys_qty])
            inserted += 1
    db.commit()
    db.close()
    return inserted


def cleanup_old_data():
    """保持期間を超えた古いデータを削除"""
    db = get_db_long()
    try:
        def get_months(key, default=12):
            row = db.execute("SELECT value FROM settings WHERE key=%s", [key]).fetchone()
            val = int(row['value']) if row else default
            return val

        # 発注履歴の削除
        order_months = get_months('order_history_months', 12)
        if order_months > 0:
            db.execute(
                "DELETE FROM order_history WHERE created_at < NOW() - (INTERVAL '1 month' * %s)",
                [order_months]
            )
            logger.info(f"[Cleanup] 発注履歴: {order_months}ヶ月以上前を削除")
        # 廃棄退避データの削除
        disposed_months = get_months('disposed_months', 12)
        if disposed_months > 0:
            db.execute(
                "DELETE FROM disposed_stocks WHERE created_at < NOW() - (INTERVAL '1 month' * %s)",
                [disposed_months]
            )
            logger.info(f"[Cleanup] 廃棄退避: {disposed_months}ヶ月以上前を削除")
        # CSVインポートデータの削除
        sales_months = get_months('sales_history_months', 12)
        if sales_months > 0:
            db.execute(
                "DELETE FROM sales_history WHERE sale_date < (CURRENT_DATE - (INTERVAL '1 month' * %s))::date",
                [sales_months]
            )
            logger.info(f"[Cleanup] CSVインポートデータ: {sales_months}ヶ月以上前を削除")
        # CSV取込ログの削除
        csv_log_months = get_months('csv_log_months', 6)
        if csv_log_months > 0:
            db.execute(
                "DELETE FROM import_logs WHERE imported_at < NOW() - (INTERVAL '1 month' * %s)",
                [csv_log_months]
            )
            logger.info(f"[Cleanup] CSV取込ログ: {csv_log_months}ヶ月以上前を削除")
        db.commit()
    except Exception as e:
        logger.warning(f"[Cleanup] エラー: {e}")
        db.rollback()
    finally:
        db.close()


def _run_weekly_md_annual(year):
    """毎年1月1日に当年分の52週MDプランを全商品分生成する"""
    try:
        from wholesale_forecast import generate_weekly_md_plan
    except ImportError:
        logger.warning('[Scheduler] wholesale_forecastをインポートできません')
        return
    db = get_db_long()
    try:
        prods = db.execute(
            "SELECT jan FROM products WHERE is_active=TRUE OR is_active=1"
        ).fetchall()
        total = 0
        for p in prods:
            try:
                result = generate_weekly_md_plan(db, p['jan'], year)
                if result:
                    total += result
            except Exception as e2:
                logger.warning(f'[Scheduler] WeeklyMD jan={p["jan"]}: {e2}')
        db.execute(
            "INSERT INTO scheduler_log (task_name, result) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING",
            [f'weekly_md_annual_{year}', f'{len(prods)}品目 {total}週分生成完了']
        )
        db.commit()
        logger.info(f'[Scheduler] WeeklyMD: {year}年度 全{len(prods)}品目 {total}週分生成完了')
    except Exception as e:
        logger.error(f'[Scheduler] WeeklyMD生成エラー: {e}')
    finally:
        try:
            db.close()
        except Exception:
            pass


def _fetch_weather_api_auto():
    """Open-Meteo APIから自動的に気温データを取得してDBに保存する"""
    import urllib.request
    import json as _json
    # DBから自動取得設定を確認
    _check_db = get_db_long()
    try:
        _row = _check_db.execute(
            "SELECT value FROM settings WHERE key='weather_auto_fetch_enabled'"
        ).fetchone()
        if _row and str(_row['value']) == '0':
            logger.info('[weather_auto] 自動取得が無効化されているためスキップします')
            return
    except Exception:
        pass
    finally:
        try:
            _check_db.close()
        except Exception:
            pass

    db = get_db_long()
    try:
        # 設定: DBの weather_auto_fetch_locations があればそれを使用
        _loc_db = None
        try:
            _loc_row = db.execute(
                "SELECT value FROM settings WHERE key='weather_auto_fetch_locations'"
            ).fetchone()
            if _loc_row and _loc_row['value']:
                _loc_db = _loc_row['value']
        except Exception:
            pass
        # DB設定優先、なければ.env設定を使用
        locations_json = _loc_db or os.environ.get('WEATHER_LOCATIONS_JSON', '')
        if locations_json:
            try:
                import json as _json2
                locations = _json2.loads(locations_json)
            except Exception:
                locations = []
        else:
            locations = [{
                'name': os.environ.get('WEATHER_LOCATION', '東京'),
                'lat':  float(os.environ.get('WEATHER_LAT', '35.6897')),
                'lon':  float(os.environ.get('WEATHER_LON', '139.6922')),
            }]

        # 取得日数を設定から読み込む（前後N日）
        try:
            _days_row = db.execute(
                "SELECT value FROM settings WHERE key='weather_auto_fetch_days'"
            ).fetchone()
            _fetch_days = int(_days_row['value']) if _days_row and _days_row['value'] else 3
            _fetch_days = max(1, min(_fetch_days, 30))
        except Exception:
            _fetch_days = 3

        if _fetch_days == 1:
            # 当日のみ
            start = date.today().isoformat()
            end   = date.today().isoformat()
        else:
            # 前後N日
            start = (date.today() - timedelta(days=_fetch_days)).isoformat()
            end   = (date.today() + timedelta(days=_fetch_days)).isoformat()

        logger.info(f'[weather_auto] 取得範囲: {start} 〜 {end}（設定: {_fetch_days}日）')
        total_inserted = 0

        for loc in locations:
            url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={loc['lat']}&longitude={loc['lon']}"
                f"&daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum"
                f"&timezone=Asia%2FTokyo&start_date={start}&end_date={end}"
            )
            with urllib.request.urlopen(url, timeout=15) as resp:
                data = _json.loads(resp.read())
            daily = data.get('daily', {})
            dates  = daily.get('time', [])
            t_mean = daily.get('temperature_2m_mean', [])
            t_max  = daily.get('temperature_2m_max', [])
            t_min  = daily.get('temperature_2m_min', [])
            precip = daily.get('precipitation_sum', [])
            inserted = 0
            for i, d in enumerate(dates):
                db.execute("""
                    INSERT INTO weather_data
                      (obs_date, location, avg_temp, max_temp, min_temp, precipitation, source)
                    VALUES (%s, %s, %s, %s, %s, %s, 'api')
                    ON CONFLICT (obs_date, location) DO UPDATE
                      SET avg_temp        = EXCLUDED.avg_temp,
                          max_temp        = EXCLUDED.max_temp,
                          min_temp        = EXCLUDED.min_temp,
                          precipitation   = EXCLUDED.precipitation,
                          source          = 'api'
                """, [
                    d,
                    loc['name'],
                    t_mean[i] if i < len(t_mean) else None,
                    t_max[i]  if i < len(t_max)  else None,
                    t_min[i]  if i < len(t_min)  else None,
                    float(precip[i]) if i < len(precip) and precip[i] is not None else 0.0,
                ])
                inserted += 1
            db.commit()
            total_inserted += inserted
            logger.info(f'[Scheduler] WeatherAPI: {loc["name"]} {inserted}日分取得・保存完了')

        # 保持期間を超えた古いデータを削除
        try:
            _ret_row = db.execute(
                "SELECT value FROM settings WHERE key='weather_data_retention_days'"
            ).fetchone()
            _retention = int(_ret_row['value']) if _ret_row and _ret_row['value'] else 365
            if _retention > 0:
                cutoff = (date.today() - timedelta(days=_retention)).isoformat()
                deleted = db.execute(
                    "DELETE FROM weather_data WHERE obs_date < %s", [cutoff]
                ).rowcount
                db.commit()
                if deleted:
                    logger.info(f'[weather_auto] 古いデータ削除: {deleted}件（{cutoff}より前）')
        except Exception as _e:
            logger.warning(f'[weather_auto] 古いデータ削除エラー: {_e}')

        return total_inserted
    except Exception as e:
        logger.error(f'[Scheduler] WeatherAPI取得エラー: {e}')
        return 0
    finally:
        try:
            db.close()
        except Exception:
            pass


# ─── バックグラウンドスケジューラー ─────────────────────────────
_scheduler_running = False
_scheduler_lock = threading.Lock()

def start_scheduler():
    global _scheduler_running
    if _scheduler_running:
        return
    _scheduler_running = True

    def loop():
        last_run = {}

        while _scheduler_running:
            now = datetime.now()
            today = now.date()

            # CSV設定ごとの時刻チェック（1日複数回対応）
            # まずIDのみ取得（文字化けフィールドがあっても整数IDは安全）
            try:
                _db_ids = get_db_long()
                _csv_ids = [r['id'] for r in _db_ids.execute(
                    "SELECT id FROM csv_import_settings WHERE is_active=1"
                ).fetchall()]
                _db_ids.close()
            except Exception as _e:
                logger.warning(f"[Scheduler] CSV設定ID取得エラー: {_e}")
                _csv_ids = []

            for _csv_sid in _csv_ids:
                # 設定を1件ずつ取得してエンコードエラーを個別にキャッチ
                try:
                    _db_cfg = get_db_long()
                    _cfg_row = _db_cfg.execute(
                        "SELECT * FROM csv_import_settings WHERE id=%s", [_csv_sid]
                    ).fetchone()
                    _db_cfg.close()
                    if not _cfg_row:
                        continue
                    cfg = dict(_cfg_row)
                except UnicodeDecodeError as _ude:
                    logger.warning(
                        f"[Scheduler] CSV設定ID={_csv_sid} エンコードエラー（Shift-JIS文字が含まれている可能性）: {_ude} "
                        f"― 設定画面で該当設定を開き直して保存してください"
                    )
                    continue
                except Exception as _e:
                    logger.warning(f"[Scheduler] CSV設定ID={_csv_sid} 読込エラー: {_e}")
                    continue

                try:
                    # run_times="06:00,12:00,18:00" 形式を解析
                    # 旧フィールド(run_hour/run_minute)との後方互換も維持
                    run_times_raw = (cfg.get('run_times') or '').strip()
                    if run_times_raw:
                        time_slots = [t.strip() for t in run_times_raw.replace('、',',').split(',') if t.strip()]
                    else:
                        time_slots = [f"{int(cfg.get('run_hour') or 6):02d}:{int(cfg.get('run_minute') or 0):02d}"]

                    for slot in time_slots:
                        try:
                            sh, sm = map(int, slot.split(':'))
                        except ValueError:
                            continue
                        key = f"csv_{cfg['id']}_{sh:02d}{sm:02d}"
                        if now.hour == sh and now.minute == sm and last_run.get(key) != today:
                            with _scheduler_lock:
                                if last_run.get(key) == today:
                                    continue
                                if _scheduler_already_ran(key, str(today)):
                                    last_run[key] = today
                                    continue
                                last_run[key] = today
                            logger.info(f'[Scheduler] CSV実行開始: setting_id={cfg["id"]} key={key} {now}')
                            run_csv_import(setting_id=cfg['id'], all_files=False, trigger_type='auto', target_date=today)
                            logger.info(f'[Scheduler] CSV実行完了: setting_id={cfg["id"]} key={key}')
                except Exception as _e:
                    logger.warning(f"[Scheduler] CSV setting_id={cfg.get('id')} 処理エラー: {_e}")
            # 毎日 設定時刻 に発注・期限チェック
            try:
                daily_h = int(os.getenv('DAILY_MAIL_HOUR',   '8'))
                daily_m = int(os.getenv('DAILY_MAIL_MINUTE', '0'))
                if now.hour == daily_h and now.minute == daily_m and last_run.get('daily') != today:
                    with _scheduler_lock:
                        if last_run.get('daily') == today:
                            pass
                        elif _scheduler_already_ran('daily', str(today)):
                            last_run['daily'] = today
                        else:
                            last_run['daily'] = today
                            run_order_check()
                            run_expiry_check()
                            # 季節品の自動有効化/無効化
                            try:
                                _toggle_seasonal_products(get_db_long(), today)
                            except Exception as _se:
                                logger.warning(f'[Scheduler] 季節品切替エラー: {_se}')
            except Exception as e:
                logger.info(f"[Scheduler] Daily: {e}")
            # 毎月1日05:00 前月分の月末月次取込（monthフォルダ）
            try:
                if today.day == 1 and now.hour == 5 and now.minute == 0 and last_run.get('month_end') != today:
                    with _scheduler_lock:
                        if last_run.get('month_end') == today:
                            pass
                        elif _scheduler_already_ran('month_end_import', str(today)):
                            last_run['month_end'] = today
                        else:
                            last_run['month_end'] = today
                            # 前月を計算
                            first_of_this_month = today.replace(day=1)
                            last_month = first_of_this_month - timedelta(days=1)
                            prev_ym = last_month.strftime('%Y%m')
                            logger.info(f'[Scheduler] 月末月次取込開始: {prev_ym}')
                            run_month_end_import(target_ym=prev_ym, trigger_type='auto')
                            logger.info(f'[Scheduler] 月末月次取込完了: {prev_ym}')
            except Exception as e:
                logger.info(f"[Scheduler] MonthEnd: {e}")
            # 毎月1日0:05 発注点更新
            try:
                if today.day == 1 and now.hour == 0 and now.minute == 5 and last_run.get('monthly') != today:
                    with _scheduler_lock:
                        if last_run.get('monthly') == today:
                            pass
                        elif _scheduler_already_ran('monthly', str(today)):
                            last_run['monthly'] = today
                        else:
                            last_run['monthly'] = today
                            update_reorder_points()
                            cleanup_old_data()
                            # 月次：気温感応度を自動再計算
                            try:
                                from wholesale_forecast import recalc_temp_sensitivity
                                from datetime import datetime as _dt2
                                _sens_db = get_db_long()
                                _sens_n  = recalc_temp_sensitivity(_sens_db)
                                if _sens_n:
                                    _sens_val = (
                                        f"{_dt2.now().strftime('%Y-%m-%d %H:%M')}"
                                        f" ／ {_sens_n}商品 ／ 月次自動"
                                    )
                                    _sens_db.execute("""
                                        INSERT INTO settings (key, value)
                                        VALUES ('temp_sensitivity_last_run', %s)
                                        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                                    """, [_sens_val])
                                    _sens_db.commit()
                                    logger.info(f'[Scheduler] 気温感応度 月次再計算完了: {_sens_n}商品')
                                _sens_db.close()
                            except Exception as _se:
                                logger.warning(f'[Scheduler] 気温感応度再計算エラー: {_se}')
                            # 月次：予測精度MAPE計算（30日前の予測 vs 実績）
                            try:
                                _calc_forecast_accuracy(get_db_long())
                                logger.info('[Scheduler] 予測精度(MAPE)更新完了')
                            except Exception as _mape_e:
                                logger.warning(f'[Scheduler] MAPE計算エラー: {_mape_e}')
            except Exception as e:
                logger.info(f"[Scheduler] Monthly: {e}")
            # 毎年1月1日 00:10 に52週MDプラン自動生成
            try:
                if today.month == 1 and today.day == 1 and now.hour == 0 and now.minute == 10:
                    _key = f'weekly_md_annual_{today.year}'
                    if last_run.get(_key) != today:
                        last_run[_key] = today
                        threading.Thread(
                            target=_run_weekly_md_annual,
                            args=(today.year,),
                            daemon=True
                        ).start()
                        logger.info(f'[Scheduler] 52週MDプラン自動生成スレッド起動')
            except Exception as _e:
                logger.warning(f'[Scheduler] WeeklyMD annual: {_e}')
            # 毎日 03:30 に予測キャッシュを事前ウォームアップ（朝の初回アクセスを高速化）
            try:
                if now.hour == 3 and now.minute == 30 and last_run.get('fc_warmup') != today:
                    with _scheduler_lock:
                        if last_run.get('fc_warmup') == today:
                            pass
                        elif _scheduler_already_ran('fc_warmup', str(today)):
                            last_run['fc_warmup'] = today
                        else:
                            last_run['fc_warmup'] = today
                            def _warmup():
                                try:
                                    import sys as _sys
                                    if 'app' in _sys.modules:
                                        _app = _sys.modules['app']
                                        _app.invalidate_forecast_cache()
                                        _wdb = get_db_long()
                                        _app._build_forecast_rows_raw(_wdb)
                                        _wdb.close()
                                        logger.info('[Scheduler] 予測キャッシュ ウォームアップ完了')
                                except Exception as _we:
                                    logger.warning(f'[Scheduler] 予測キャッシュ ウォームアップエラー: {_we}')
                            threading.Thread(target=_warmup, daemon=True).start()
            except Exception as _e:
                logger.warning(f'[Scheduler] FcWarmup: {_e}')
            # 毎日 設定時刻 に Open-Meteo から気温データを自動取得
            try:
                try:
                    _wh_db = get_db_long()
                    _wh_row = _wh_db.execute(
                        "SELECT value FROM settings WHERE key='weather_auto_fetch_hour'"
                    ).fetchone()
                    _weather_hour = int(_wh_row['value']) if _wh_row and _wh_row['value'] else 3
                    _wh_db.close()
                except Exception:
                    _weather_hour = 3
                if now.hour == _weather_hour and now.minute == 0:
                    if last_run.get('weather_api') != today:
                        last_run['weather_api'] = today
                        threading.Thread(
                            target=_fetch_weather_api_auto,
                            daemon=True
                        ).start()
                        logger.info(f'[Scheduler] 気象API自動取込スレッド起動（設定時刻: {_weather_hour:02d}:00）')
            except Exception as _e:
                logger.warning(f'[Scheduler] WeatherAPI: {_e}')
            time.sleep(60)

    t = threading.Thread(target=loop, daemon=True, name='InventoryScheduler')
    t.start()
    logger.info("OK: Scheduler started.")