"""共有ヘルパー関数・定数"""
import io, csv, logging
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from datetime import date, timedelta, datetime

logger = logging.getLogger('inventory.helpers')

_EXPORT_ROW_LIMIT = 10_000


def _normalize_jan(val):
    """JANコード正規化: 指数表記(4.90123E+12)を整数文字列に変換"""
    if val is None:
        return ''
    s = str(val).strip()
    if not s:
        return ''
    try:
        # 指数表記の場合は数値として解釈して整数化
        f = float(s)
        if 'e' in s.lower() or '.' in s:
            return str(int(f))
        return s
    except (ValueError, OverflowError):
        return s

def _normalize_date(val):
    """日付正規化: 各種形式をYYYY-MM-DDに統一
    対応: 2026/3/30, 2026/03/30, 2026-3-30, 20260330, datetime等
    """
    if val is None:
        return ''
    import datetime as _dt
    # すでにdatetime/dateオブジェクト
    if isinstance(val, (_dt.datetime, _dt.date)):
        return val.strftime('%Y-%m-%d')
    s = str(val).strip()
    if not s:
        return ''
    # YYYY-MM-DD（そのまま）
    if len(s) == 10 and s[4] == '-' and s[7] == '-':
        return s
    # YYYY/MM/DD or YYYY/M/D
    if '/' in s:
        parts = s.split('/')
        if len(parts) == 3:
            try:
                y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
                return f'{y:04d}-{m:02d}-{d:02d}'
            except ValueError:
                pass
    # YYYYMMDD
    if len(s) == 8 and s.isdigit():
        return f'{s[:4]}-{s[4:6]}-{s[6:8]}'
    # YYYY-M-D
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 3:
            try:
                y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
                return f'{y:04d}-{m:02d}-{d:02d}'
            except ValueError:
                pass
    return s


def _to_int(v, default=0):
    try:
        return int(float(str(v)))
    except Exception:
        return default

def _safe_date(s):
    try:
        return datetime.strptime(str(s), '%Y-%m-%d').date()
    except Exception:
        return None

def _resolve_product_by_code(db, code):
    code = (code or '').strip()
    if not code:
        return None
    return db.execute("SELECT jan, product_cd, product_name, supplier_name FROM products WHERE (jan=%s OR product_cd=%s) AND is_active=1 ORDER BY jan LIMIT 1", [code, code]).fetchone()

def _record_receipt(db, product, qty, expiry, lot_no='', location_code='', source='manual', note=''):
    before = db.execute("SELECT COALESCE(SUM(quantity),0) AS _sum FROM stocks WHERE jan=%s", [product['jan']]).fetchone()['_sum']
    db.execute("""
        INSERT INTO stocks
        (product_id,jan,product_name,supplier_cd,supplier_name,
         product_cd,unit_qty,quantity,expiry_date,lot_no,location_code)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, [product['id'],product['jan'],product['product_name'],
          product['supplier_cd'],product['supplier_name'],product['product_cd'],
          product['unit_qty'],qty,expiry,lot_no,location_code or product.get('location_code','') or ''])
    db.execute("""
        INSERT INTO stock_movements
        (jan,product_name,move_type,quantity,before_qty,after_qty,note,source_file,move_date,expiry_date)
        VALUES (%s,%s,'receipt',%s,%s,%s,%s,%s,%s,%s)
    """, [product['jan'],product['product_name'],qty,before,before+qty,
          note or '', source, str(date.today()), expiry])


def _build_promotion_calendar(db, horizon_days=35):
    rows = db.execute("""
        SELECT pp.jan,
               TO_CHAR(pp.promo_date, 'YYYY-MM-DD') AS promo_date,
               pp.uplift_factor, pp.promo_name
        FROM promotion_plans pp
        WHERE pp.promo_date BETWEEN CURRENT_DATE - INTERVAL '1 day'
                                AND CURRENT_DATE + (%s || ' days')::interval
    """, [horizon_days]).fetchall()
    mp = {}
    for r in rows:
        mp.setdefault(r['jan'], {})[r['promo_date']] = {
            'uplift_factor': float(r.get('uplift_factor') or 1.0),
            'promo_name': r.get('promo_name') or ''
        }
    return mp


def _build_demand_plan_map(db, horizon_days=35):
    rows = db.execute("""
        SELECT jan,
               TO_CHAR(demand_date, 'YYYY-MM-DD') AS demand_date,
               SUM(demand_qty) AS demand_qty
        FROM demand_plans
        WHERE demand_date BETWEEN CURRENT_DATE - INTERVAL '1 day'
                              AND CURRENT_DATE + (%s || ' days')::interval
        GROUP BY jan, demand_date
    """, [horizon_days]).fetchall()
    mp = {}
    for r in rows:
        mp.setdefault(r['jan'], {})[r['demand_date']] = int(r.get('demand_qty') or 0)
    return mp


def _abc_rank_from_ratio(ratio):
    if ratio <= 0.7:
        return 'A'
    if ratio <= 0.9:
        return 'B'
    return 'C'

def _get_forecast_feature_flags(db):
    # forecast_ai_mode: '1'=AIモードON(全機能) / '0'=前年実績モード(シンプル)
    row_ai = db.execute("SELECT value FROM settings WHERE key=%s", ['forecast_ai_mode']).fetchone()
    ai_mode = str(row_ai['value'] if row_ai else '1').strip() in ('1','true','True','on','yes')
    # P2発注点モード: sf/p80/p90（AIモードON/OFF共通で有効）
    row_rm = db.execute("SELECT value FROM settings WHERE key=%s", ['forecast_reorder_mode']).fetchone()
    reorder_mode = (row_rm['value'] if row_rm else 'sf') or 'sf'
    return {
        'forecast_ai_mode':      ai_mode,
        'forecast_reorder_mode': reorder_mode,
    }



def _build_forecast_rows(db, q=''):
    flags = _get_forecast_feature_flags(db)
    ai_mode      = flags['forecast_ai_mode']      # True=AIモード / False=前年実績モード
    reorder_mode = flags['forecast_reorder_mode']  # sf/p80/p90

    # AIモード時のみ販促・受注予定マップを取得
    promo_map  = _build_promotion_calendar(db, 35) if ai_mode else {}
    demand_map = _build_demand_plan_map(db, 35)  # AIモードON/OFF共通で受注予定を反映

    # 前年実績モード用: 前年同月の日次平均を取得
    if not ai_mode:
        import calendar as _cal
        _today = date.today()
        _last_year  = _today.year - 1
        _this_month = _today.month
        _days_in_month = _cal.monthrange(_last_year, _this_month)[1]
        _ly_rows = db.execute("""
            SELECT jan, COALESCE(SUM(quantity),0) AS total_qty
            FROM sales_history
            WHERE to_char(sale_date::date,'YYYY')=%s
              AND to_char(sale_date::date,'MM')=%s
            GROUP BY jan
        """, [str(_last_year), f"{_this_month:02d}"]).fetchall()
        _ly_map = {r['jan']: float(r['total_qty'] or 0) / _days_in_month for r in _ly_rows}
    else:
        _ly_map = {}

    rows = db.execute("""
        WITH daily AS (
            SELECT sh.jan,
                   sh.sale_date::date AS sale_dt,
                   EXTRACT(ISODOW FROM sh.sale_date::date)::int AS dow,
                   SUM(sh.quantity) AS qty
            FROM sales_history sh
            WHERE sh.sale_date::date >= CURRENT_DATE - INTERVAL '180 days'
            GROUP BY sh.jan, sh.sale_date::date, dow
        ), monthly AS (
            SELECT jan,
                   EXTRACT(MONTH FROM sale_dt)::int AS mon,
                   DATE_TRUNC('month', sale_dt) AS ym,
                   SUM(qty) AS qty
            FROM daily
            WHERE sale_dt >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months'
            GROUP BY jan, mon, ym
        ), avg_all AS (
            SELECT jan, AVG(qty) AS avg_monthly
            FROM monthly GROUP BY jan
        ), season AS (
            SELECT m.jan, m.mon,
                   AVG(m.qty) AS mon_avg,
                   a.avg_monthly,
                   CASE WHEN COALESCE(a.avg_monthly,0)=0 THEN 1 ELSE AVG(m.qty)/a.avg_monthly END AS season_idx
            FROM monthly m
            JOIN avg_all a ON a.jan=m.jan
            GROUP BY m.jan, m.mon, a.avg_monthly
        ), dow_avg AS (
            SELECT jan, dow, AVG(qty) AS dow_qty
            FROM daily
            WHERE sale_dt >= CURRENT_DATE - INTERVAL '84 days'
            GROUP BY jan, dow
        ), wma_daily AS (
            -- P1改善: 加重移動平均(WMA-30日) 直近ほど重視
            SELECT jan,
                   SUM(qty * day_weight) / NULLIF(SUM(day_weight), 0) AS avg_daily
            FROM (
                SELECT jan, qty,
                       ROW_NUMBER() OVER (PARTITION BY jan ORDER BY sale_dt) AS day_weight
                FROM daily
                WHERE sale_dt >= CURRENT_DATE - INTERVAL '30 days'
            ) w
            GROUP BY jan
        ), base_daily AS (
            -- WMAが取れない場合(データ不足)は従来の84日単純平均にフォールバック
            SELECT d.jan,
                   COALESCE(w.avg_daily, plain.avg_daily) AS avg_daily
            FROM (SELECT DISTINCT jan FROM daily) d
            LEFT JOIN wma_daily w ON w.jan = d.jan
            LEFT JOIN (
                SELECT jan, AVG(qty) AS avg_daily
                FROM daily
                WHERE sale_dt >= CURRENT_DATE - INTERVAL '84 days'
                GROUP BY jan
            ) plain ON plain.jan = d.jan
        ), dow_idx AS (
            SELECT d.jan, d.dow,
                   CASE WHEN COALESCE(b.avg_daily,0)=0 THEN 1 ELSE d.dow_qty / b.avg_daily END AS dow_idx
            FROM dow_avg d
            JOIN base_daily b ON b.jan=d.jan
        ), stock AS (
            SELECT jan, SUM(quantity) AS stock_qty FROM stocks WHERE quantity>0 GROUP BY jan
        )
        SELECT p.id AS product_id, p.supplier_cd, p.supplier_name, p.product_cd, p.jan, p.product_name,
               p.reorder_point, p.order_unit, p.order_qty, p.lead_time_days, p.safety_factor,
               p.shelf_face_qty, p.shelf_replenish_point,
               COALESCE(s.stock_qty,0) AS stock_qty,
               ROUND(COALESCE(a.avg_monthly,0),1) AS avg_monthly,
               ROUND(COALESCE(b.avg_daily,0),2) AS avg_daily,
               ROUND(COALESCE(b.avg_daily,0),2) AS avg_daily_wma,
               ROUND(COALESCE(se_this.season_idx,1),4) AS season_idx_this,
               ROUND(COALESCE(se_next.season_idx,1),4) AS season_idx_next,
               ROUND(COALESCE(di1.dow_idx,1),4) AS dow_idx_1,
               ROUND(COALESCE(di2.dow_idx,1),4) AS dow_idx_2,
               ROUND(COALESCE(di3.dow_idx,1),4) AS dow_idx_3,
               ROUND(COALESCE(di4.dow_idx,1),4) AS dow_idx_4,
               ROUND(COALESCE(di5.dow_idx,1),4) AS dow_idx_5,
               ROUND(COALESCE(di6.dow_idx,1),4) AS dow_idx_6,
               ROUND(COALESCE(di7.dow_idx,1),4) AS dow_idx_7
        FROM products p
        LEFT JOIN avg_all a ON a.jan=p.jan
        LEFT JOIN season se_this ON se_this.jan=p.jan AND se_this.mon=EXTRACT(MONTH FROM CURRENT_DATE)::int
        LEFT JOIN season se_next ON se_next.jan=p.jan AND se_next.mon=EXTRACT(MONTH FROM CURRENT_DATE + INTERVAL '1 month')::int
        LEFT JOIN base_daily b ON b.jan=p.jan
        LEFT JOIN dow_idx di1 ON di1.jan=p.jan AND di1.dow=1
        LEFT JOIN dow_idx di2 ON di2.jan=p.jan AND di2.dow=2
        LEFT JOIN dow_idx di3 ON di3.jan=p.jan AND di3.dow=3
        LEFT JOIN dow_idx di4 ON di4.jan=p.jan AND di4.dow=4
        LEFT JOIN dow_idx di5 ON di5.jan=p.jan AND di5.dow=5
        LEFT JOIN dow_idx di6 ON di6.jan=p.jan AND di6.dow=6
        LEFT JOIN dow_idx di7 ON di7.jan=p.jan AND di7.dow=7
        LEFT JOIN stock s ON s.jan=p.jan
        WHERE p.is_active=1
        ORDER BY p.supplier_cd, p.product_cd
    """).fetchall()

    import calendar, statistics as _stats

    reorder_mode = flags.get('forecast_reorder_mode', 'sf')  # P2: 'sf'/'p80'/'p90'

    # P2: 分位点計算のために過去30日の日次売上を取得
    quantile_map = {}
    try:
        qrows = db.execute("""
            SELECT jan,
                   array_agg(qty ORDER BY sale_dt) AS daily_qtys
            FROM (
                SELECT jan,
                       sale_date::date AS sale_dt,
                       SUM(quantity) AS qty
                FROM sales_history
                WHERE sale_date::date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY jan, sale_date::date
            ) d
            GROUP BY jan
        """).fetchall()
        for qr in qrows:
            qtys = sorted([int(x) for x in (qr['daily_qtys'] or []) if x is not None])
            if len(qtys) >= 5:  # 前日データのみ取込環境を考慮して5件以上で算出
                n = len(qtys)
                quantile_map[qr['jan']] = {
                    'p80':  qtys[int(n * 0.80)],
                    'p90':  qtys[int(n * 0.90)],
                    'mean': _stats.mean(qtys),
                    'std':  _stats.stdev(qtys) if len(qtys) >= 2 else 0.0,
                }
    except Exception:
        pass

    out = []
    q = (q or '').strip().lower()
    today = date.today()
    for r in rows:
        if q and not any(q in str(r.get(k) or '').lower() for k in ('jan','product_cd','product_name','supplier_cd','supplier_name')):
            continue

        r = dict(r)

        if not ai_mode:
            # == 前年実績モード ==
            # 前年同月の日次平均をそのまま使用（WMA/季節/曜日/販促/受注/P3 全て無効）
            ly_daily = _ly_map.get(r['jan'], 0.0)
            base_next_30   = ly_daily * 30.0
            adjusted30       = base_next_30   # 販促・受注予定は加算しない
            base_next_daily_display = ly_daily
            avg_daily           = ly_daily
            season_idx_display  = 1.0
            dow_idx_display     = 1.0
            promo_count         = 0
            promo_uplift_qty    = 0.0
            # AIモードOFF時でも受注予定は反映する
            direct_demand_qty   = 0
            direct_demand_days  = 0
            demand_days_ly = demand_map.get(r['jan'], {})
            for _i in range(35):
                _tday = today + timedelta(days=_i)
                _planned = int(demand_days_ly.get(str(_tday), 0) or 0)
                if _planned > 0:
                    direct_demand_qty += _planned
                    direct_demand_days += 1
            p80_daily = p90_daily = daily_std = None
        else:
            # == AIモード（全機能） ==
            # P1: WMAベースのavg_daily
            avg_daily = float(r.get('avg_daily') or 0)

            # 曜日指数（全7曜日・常時ON）
            dow_idx_map = {
                d: float(r.get(f'dow_idx_{d}') or 1)
                for d in range(1, 8)
            }

            # 季節指数（今月・来月按分・常時ON）
            season_idx_this = float(r.get('season_idx_this') or 1)
            season_idx_next = float(r.get('season_idx_next') or 1)

            season_idx_display = season_idx_next
            dow_idx_display = dow_idx_map[today.isoweekday()]

            # P3: 手動調整係数（商品ごとの係数で制御）
            manual_adj = max(0.1, float(r.get('manual_adj_factor') or 1.0))

            base_next_30 = 0.0
            promo_days  = promo_map.get(r['jan'], {})
            demand_days = demand_map.get(r['jan'], {})

            promo_uplift_qty = 0.0
            promo_count = 0
            direct_demand_qty = 0
            direct_demand_days = 0

            for i in range(30):
                target_day = today + timedelta(days=i)
                d_idx = dow_idx_map[target_day.isoweekday()]
                s_idx = season_idx_this if target_day.month == today.month else season_idx_next
                daily_fc = avg_daily * s_idx * d_idx * manual_adj
                base_next_30 += daily_fc

                ds = str(target_day)
                promo = promo_days.get(ds)
                if promo and float(promo.get('uplift_factor', 1.0) or 1.0) > 1:
                    promo_count += 1
                    promo_uplift_qty += daily_fc * (float(promo['uplift_factor']) - 1.0)
                planned = int(demand_days.get(ds, 0) or 0)
                if planned > 0:
                    direct_demand_qty += planned
                    direct_demand_days += 1

            adjusted30 = base_next_30 + promo_uplift_qty + direct_demand_qty
            base_next_daily_display = base_next_30 / 30.0

            # P2: 分位点データ（AIモード時のみ）
            qdata = quantile_map.get(r['jan'], {})
            p80_daily = qdata.get('p80', None)
            p90_daily = qdata.get('p90', None)
            daily_std = qdata.get('std', None)


        lt = max(int(r.get('lead_time_days') or 1), 1)
        sf = max(float(r.get('safety_factor') or 1.0), 1.0)
        next_daily = round(adjusted30 / 30.0, 2) if adjusted30 else 0

        # P2: 発注点モード（AIモード・前年実績モード共通）
        if not ai_mode:
            # 前年実績モード: 前年日次平均を基準に発注点計算
            ly_daily_for_rp = _ly_map.get(r['jan'], 0.0)
            if reorder_mode == 'p90' and ly_daily_for_rp > 0:
                suggested_rp = int(max(0, ly_daily_for_rp * lt * 1.1 + 0.9999))  # P90相当: +10%
                rp_mode_label = 'P90'
            elif reorder_mode == 'p80' and ly_daily_for_rp > 0:
                suggested_rp = int(max(0, ly_daily_for_rp * lt * 1.05 + 0.9999))  # P80相当: +5%
                rp_mode_label = 'P80'
            else:
                suggested_rp = int(max(0, next_daily * lt * sf + 0.9999))
                rp_mode_label = 'SF'
        else:
            # AIモード: WMA分位点ベースで発注点計算
            if reorder_mode == 'p90' and p90_daily is not None:
                suggested_rp = int(max(0, p90_daily * lt + 0.9999))
                rp_mode_label = 'P90'
            elif reorder_mode == 'p80' and p80_daily is not None:
                suggested_rp = int(max(0, p80_daily * lt + 0.9999))
                rp_mode_label = 'P80'
            else:
                suggested_rp = int(max(0, next_daily * lt * sf + 0.9999))
                rp_mode_label = 'SF'

        r['season_idx']         = round(season_idx_display, 2)
        r['avg_dow_idx']        = round(dow_idx_display, 2)
        r['manual_adj_factor']  = round(manual_adj, 2)          # P3
        r['promo_days']         = promo_count
        r['promo_uplift_qty']   = round(promo_uplift_qty, 1)
        r['direct_demand_qty']  = int(direct_demand_qty)
        r['direct_demand_days'] = int(direct_demand_days)
        r['next_30d_forecast']  = round(adjusted30, 1)
        r['next_daily_forecast']= next_daily
        r['weighted_daily_forecast'] = round(base_next_daily_display, 2)
        r['next_month_forecast']= round(adjusted30, 1)
        r['cover_days']         = round(float(r.get('stock_qty') or 0) / next_daily, 1) if next_daily else None
        r['suggested_reorder_point'] = suggested_rp
        r['rp_mode_label']      = rp_mode_label                 # P2
        r['p80_daily']          = p80_daily                     # P2
        r['p90_daily']          = p90_daily                     # P2
        r['daily_std']          = round(daily_std, 2) if daily_std is not None else None  # P2
        # 推奨発注数 = (LT + 14) × 日次予測
        r['suggested_order_qty']= int(max(0, next_daily * (lt + 14) + 0.9999))
        out.append(r)
    return out

def _build_picking_plan(db, days=7, q=''):
    rows = db.execute("""
        WITH demand AS (
            SELECT jan,
                   CEIL(SUM(quantity) / GREATEST(COUNT(DISTINCT sale_date::date),1) * %s) AS need_qty
            FROM sales_history
            WHERE sale_date::date >= CURRENT_DATE - (%s || ' days')::interval
            GROUP BY jan
        )
        SELECT p.supplier_cd, p.supplier_name, p.product_cd, p.jan, p.product_name,
               COALESCE(d.need_qty, 0) AS need_qty,
               s.id AS stock_id, s.expiry_date, s.lot_no,
               COALESCE(NULLIF(s.location_code,''), NULLIF(p.location_code,''), '') AS location_code,
               s.quantity
        FROM products p
        LEFT JOIN demand d ON d.jan=p.jan
        JOIN stocks s ON s.jan=p.jan AND s.quantity>0
        WHERE p.is_active=1 AND COALESCE(d.need_qty,0) > 0
        ORDER BY p.supplier_cd, p.product_cd,
                 CASE WHEN s.expiry_date='' THEN '9999-99-99' ELSE s.expiry_date END ASC,
                 COALESCE(NULLIF(s.location_code,''), NULLIF(p.location_code,''), '') ASC
    """, [days, days]).fetchall()
    q = (q or '').strip().lower()
    if q:
        rows=[r for r in rows if q in (r['jan'] or '').lower() or q in (r['product_cd'] or '').lower() or q in (r['product_name'] or '').lower() or q in (r['location_code'] or '').lower()]
    plan=[]
    current_jan=None
    remaining=0
    for r in rows:
        if r['jan'] != current_jan:
            current_jan=r['jan']
            remaining=int(r['need_qty'] or 0)
        if remaining <= 0:
            continue
        pick=min(int(r['quantity'] or 0), remaining)
        if pick <= 0:
            continue
        remaining -= pick
        x=dict(r)
        x['pick_qty']=pick
        x['remaining_after']=remaining
        x['location_code']=x['location_code'] or '未設定'
        plan.append(x)
    return plan


def _build_shortage_rows(db, q=''):
    forecast_rows = _build_forecast_rows(db, q='')
    forecast_map = {r['jan']: r for r in forecast_rows}
    today = date.today()
    inbound_rows = db.execute("""
        SELECT oh.jan,
               COALESCE(NULLIF(oh.expected_receipt_date,''), TO_CHAR((oh.order_date::date + (COALESCE(p.lead_time_days,3) || ' days')::interval)::date, 'YYYY-MM-DD')) AS eta,
               GREATEST(oh.order_qty - COALESCE((SELECT SUM(received_qty) FROM order_receipts r WHERE r.order_history_id=oh.id),0),0) AS outstanding_qty
        FROM order_history oh
        JOIN products p ON p.jan=oh.jan
        WHERE GREATEST(oh.order_qty - COALESCE((SELECT SUM(received_qty) FROM order_receipts r WHERE r.order_history_id=oh.id),0),0) > 0
    """).fetchall()
    inbound = {}
    for r in inbound_rows:
        eta = r['eta'] or str(today + timedelta(days=3))
        inbound.setdefault(r['jan'], {}).setdefault(eta, 0)
        inbound[r['jan']][eta] += int(r['outstanding_qty'] or 0)

    abc_rows = db.execute("""
        WITH sales AS (
            SELECT sh.jan, SUM(sh.quantity * COALESCE(p.cost_price,0)) AS sales_value
            FROM sales_history sh
            JOIN products p ON p.jan=sh.jan
            WHERE sh.sale_date::date >= CURRENT_DATE - INTERVAL '365 days'
            GROUP BY sh.jan
        ), ranked AS (
            SELECT jan, sales_value,
                   SUM(sales_value) OVER () AS total_sales,
                   SUM(sales_value) OVER (ORDER BY sales_value DESC NULLS LAST, jan) AS running_sales
            FROM sales
        )
        SELECT jan, COALESCE(sales_value,0) AS sales_value,
               CASE WHEN COALESCE(total_sales,0)=0 THEN 'C'
                    WHEN (running_sales/NULLIF(total_sales,0)) <= 0.7 THEN 'A'
                    WHEN (running_sales/NULLIF(total_sales,0)) <= 0.9 THEN 'B'
                    ELSE 'C' END AS abc_class
        FROM ranked
    """).fetchall()
    abc_map = {r['jan']: {'abc_class': r['abc_class'], 'sales_value': float(r['sales_value'] or 0)} for r in abc_rows}

    delay_rows = db.execute("""
        SELECT oh.jan,
               COUNT(*) FILTER (WHERE GREATEST(oh.order_qty - COALESCE((SELECT SUM(received_qty) FROM order_receipts r WHERE r.order_history_id=oh.id),0),0) > 0
                                AND COALESCE(NULLIF(oh.expected_receipt_date,''), oh.order_date) < CURRENT_DATE::text) AS overdue_count
        FROM order_history oh
        GROUP BY oh.jan
    """).fetchall()
    delay_map = {r['jan']: r for r in delay_rows}

    stock_rows = db.execute("""
        SELECT p.id AS product_id,p.supplier_cd,p.supplier_name,p.product_cd,p.jan,p.product_name,p.reorder_point,p.order_qty,p.lead_time_days,
               COALESCE(SUM(s.quantity),0) AS stock_qty,
               COALESCE(p.cost_price,0) AS cost_price,
               p.ordered_at
        FROM products p
        LEFT JOIN stocks s ON s.jan=p.jan
        WHERE p.is_active=1
        GROUP BY p.id
        ORDER BY p.supplier_cd,p.product_cd
    """).fetchall()
    out=[]
    risk_order = {'入荷前欠品': 4, '予測欠品': 3, '要注意': 2, '安全': 1}
    abc_weight = {'A': 3, 'B': 2, 'C': 1}
    for r in stock_rows:
        fr = forecast_map.get(r['jan'], {})
        daily = float(fr.get('next_daily_forecast') or 0)
        if q and not any(q in str(r.get(k) or '').lower() for k in ('jan','product_cd','product_name','supplier_cd','supplier_name')):
            continue
        projected = float(r.get('stock_qty') or 0)
        stockout_date = None
        worst_stock = projected
        first_inbound = None
        first_inbound_qty = 0
        for i in range(0, 31):
            d = str(today + timedelta(days=i))
            if d in inbound.get(r['jan'], {}):
                projected += inbound[r['jan']][d]
                if first_inbound is None:
                    first_inbound = d
                    first_inbound_qty = inbound[r['jan']][d]
            projected -= daily
            worst_stock = min(worst_stock, projected)
            if stockout_date is None and projected < 0:
                stockout_date = d
        risk = '安全'
        if stockout_date:
            if first_inbound and stockout_date < first_inbound:
                risk = '入荷前欠品'
            else:
                risk = '予測欠品'
        elif projected <= int(r.get('reorder_point') or 0):
            risk = '要注意'
        abc = abc_map.get(r['jan'], {'abc_class': 'C', 'sales_value': 0})
        overdue_count = int((delay_map.get(r['jan']) or {}).get('overdue_count') or 0)
        days_to_stockout = ((_safe_date(stockout_date) - today).days if stockout_date else None)
        priority_score = risk_order.get(risk, 0) * 100 + abc_weight.get(abc['abc_class'], 1) * 10 + (20 - min(max(days_to_stockout or 20, 0), 20))
        action = '監視'
        if risk == '入荷前欠品':
            action = '即発注候補'
        elif risk == '予測欠品':
            action = '追加発注候補'
        elif risk == '要注意' and abc['abc_class'] == 'A':
            action = '前倒し確認'
        out.append({**dict(r),
            'daily_forecast': round(daily,2),
            'forecast_30d': round(daily*30,1),
            'expected_receipt_date': first_inbound or '',
            'expected_receipt_qty': first_inbound_qty,
            'stockout_date': stockout_date or '',
            'projected_30d_stock': round(projected,1),
            'worst_projected_stock': round(worst_stock,1),
            'risk_level': risk,
            'days_to_stockout': days_to_stockout,
            'abc_class': abc['abc_class'],
            'annual_sales_value': round(abc['sales_value'], 0),
            'priority_score': int(priority_score),
            'recommended_action': action,
            'overdue_order_count': overdue_count,
        })
    out.sort(key=lambda x:(-x['priority_score'], x['days_to_stockout'] if x['days_to_stockout'] is not None else 9999, x['supplier_cd'], x['product_cd']))
    return out


def _build_replenishment_rows(db, q=''):
    rows = db.execute("""
        WITH shelf AS (
            SELECT p.jan,
                   COALESCE(SUM(CASE WHEN COALESCE(s.location_code,'') = COALESCE(NULLIF(p.location_code,''), '__none__') THEN s.quantity ELSE 0 END),0) AS shelf_qty,
                   COALESCE(SUM(CASE WHEN COALESCE(s.location_code,'') <> COALESCE(NULLIF(p.location_code,''), '__none__') THEN s.quantity ELSE 0 END),0) AS reserve_qty,
                   MIN(CASE WHEN COALESCE(s.location_code,'') <> COALESCE(NULLIF(p.location_code,''), '__none__') THEN s.expiry_date END) AS reserve_oldest_expiry
            FROM products p
            LEFT JOIN stocks s ON s.jan=p.jan AND s.quantity>0
            WHERE p.is_active=1
            GROUP BY p.jan, p.location_code
        )
        SELECT p.id AS product_id,p.supplier_cd,p.supplier_name,p.product_cd,p.jan,p.product_name,
               COALESCE(NULLIF(p.location_code,''),'未設定') AS shelf_location,
               p.shelf_face_qty,p.shelf_replenish_point,
               COALESCE(sh.shelf_qty,0) AS shelf_qty,
               COALESCE(sh.reserve_qty,0) AS reserve_qty,
               sh.reserve_oldest_expiry
        FROM products p
        LEFT JOIN shelf sh ON sh.jan=p.jan
        WHERE p.is_active=1
        ORDER BY p.supplier_cd,p.product_cd
    """).fetchall()
    out=[]
    for r in rows:
        if q and not any(q in str(r.get(k) or '').lower() for k in ('jan','product_cd','product_name','supplier_cd','supplier_name','shelf_location')):
            continue
        target = int(r.get('shelf_face_qty') or 0)
        trigger = int(r.get('shelf_replenish_point') or 0)
        if target <= 0:
            target = max(int(r.get('shelf_qty') or 0), 0)
        if trigger <= 0:
            trigger = max(int(round(target * 0.4)), 1 if target > 0 else 0)
        shelf_qty = int(r.get('shelf_qty') or 0)
        reserve_qty = int(r.get('reserve_qty') or 0)
        need = max(target - shelf_qty, 0)
        suggested = min(need, reserve_qty)
        status = '十分'
        if shelf_qty <= 0 and reserve_qty <= 0:
            status = '欠品'
        elif shelf_qty <= trigger:
            status = '要補充'
        elif shelf_qty < target:
            status = '補充推奨'
        if status != '十分':
            out.append({**dict(r), 'shelf_target':target, 'shelf_trigger':trigger, 'suggested_replenish_qty':suggested, 'status':status})
    order={'欠品':0,'要補充':1,'補充推奨':2,'十分':3}
    out.sort(key=lambda x:(order.get(x['status'],9), x['supplier_cd'], x['product_cd']))
    return out


def _excel_bytes_from_rows(title, headers, rows):
    from io import BytesIO
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = title[:31]
    ws.append(headers)
    for row in rows:
        ws.append(row)
    for col in ws.columns:
        max_len = 0
        col_letter = col[0].column_letter
        for cell in col:
            val = '' if cell.value is None else str(cell.value)
            if len(val) > max_len:
                max_len = len(val)
        ws.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 40)
    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.getvalue()


_PRODUCT_COLS = [
    ('jan',            'JANコード',          'text'),
    ('product_cd',     '商品コード',          'text'),
    ('product_name',   '商品名',              'text'),
    ('supplier_cd',    '仕入先コード',        'text'),
    ('supplier_name',  '仕入先名',            'text'),
    ('unit_qty',       '入数',                'int'),
    ('order_unit',     '発注単位',            'int'),
    ('order_qty',      '発注数量',            'int'),
    ('reorder_point',  '発注点',              'int'),
    ('reorder_auto',   '発注点自動更新',      'int'),
    ('lead_time_days', 'リードタイム日数',    'int'),
    ('safety_factor',  '安全係数',            'float'),
    ('lot_size',       'メーカーロット数',    'int'),
    ('shelf_life_days','賞味期限日数',        'int'),
    ('expiry_alert_days','期限アラート日数',  'int'),
    ('mixed_group',    '混載グループ名',      'text'),
    ('mixed_lot_mode', '混載ロットルール',    'text'),
    ('mixed_lot_cases','混載ケース数',        'int'),
    ('mixed_force_days','強制発注日数',       'int'),
    ('cost_price',     '原価',                'float'),
    ('sell_price',     '売価',                'float'),
]
