"""
問屋向けハイブリッド需要予測エンジン

アルゴリズム構成:
  Aランク商品: ホルト・ウィンタース指数平滑法 + Q25/Q50/Q75分位点 + 気温補正 + 動的安全在庫
  B/Cランク商品: 単純移動平均 + 固定安全在庫
  動的安全在庫: σ_AI = (Q75 - Q25) / 1.349 → SS = Z × σ_AI × √L
"""
from __future__ import annotations
import math
import logging
from datetime import date, timedelta
from statistics import mean, stdev

import numpy as np
from scipy import stats as scipy_stats

logger = logging.getLogger('inventory.forecast')


# ── 定数 ──────────────────────────────────────────────────────────────────
_IQR_TO_STD = 1.349          # 四分位範囲→標準偏差換算係数
_DEFAULT_Z   = 1.65           # サービスレベル95%
_MIN_DAYS    = 14             # 分位点計算に必要な最低日数
_HW_ALPHA    = 0.3            # Holt-Winters: レベル平滑化係数
_HW_BETA     = 0.1            # Holt-Winters: トレンド平滑化係数
_HW_GAMMA    = 0.2            # Holt-Winters: 季節平滑化係数
_SEASON_LEN  = 52             # 季節周期（週）


# ──────────────────────────────────────────────────────────────────────────
# ABC分析
# ──────────────────────────────────────────────────────────────────────────

def calc_abc_rank(sales_value: float, running_ratio: float,
                  a_threshold: float = 0.70, b_threshold: float = 0.90) -> str:
    """売上累計比率からABCランクを返す"""
    if running_ratio <= a_threshold:
        return 'A'
    if running_ratio <= b_threshold:
        return 'B'
    return 'C'


def build_abc_map(db, a_threshold: float = 0.70, b_threshold: float = 0.90) -> dict:
    """
    過去365日の売上金額でABC分析を行い {jan: rank} を返す。
    sales_history に cost_price がない場合は数量ベースで代替。
    """
    rows = db.execute("""
        WITH sales AS (
            SELECT sh.jan,
                   SUM(sh.quantity * COALESCE(NULLIF(p.cost_price,0), 1)) AS sales_value
            FROM sales_history sh
            LEFT JOIN products p ON p.jan = sh.jan
            WHERE sh.sale_date::date >= CURRENT_DATE - INTERVAL '365 days'
            GROUP BY sh.jan
        ), ranked AS (
            SELECT jan, sales_value,
                   SUM(sales_value) OVER () AS total_sales,
                   SUM(sales_value) OVER (ORDER BY sales_value DESC NULLS LAST, jan) AS running_sales
            FROM sales
        )
        SELECT jan, sales_value,
               CASE WHEN COALESCE(total_sales,0)=0 THEN 1.0
                    ELSE running_sales / NULLIF(total_sales, 0) END AS running_ratio
        FROM ranked
    """).fetchall()

    abc_map = {}
    for r in rows:
        abc_map[r['jan']] = calc_abc_rank(
            float(r['sales_value'] or 0),
            float(r['running_ratio'] or 1.0),
            a_threshold, b_threshold
        )
    return abc_map


# ──────────────────────────────────────────────────────────────────────────
# 気温補正
# ──────────────────────────────────────────────────────────────────────────

def calc_temp_sensitivity(daily_sales: list[float], daily_temps: list[float]) -> dict:
    """
    日次売上と気温の線形回帰で気温感応度を計算する。
    Returns: {temp_coef, r_squared, base_temp}
    """
    if len(daily_sales) < 10 or len(daily_temps) < 10:
        return {'temp_coef': 0.0, 'r_squared': 0.0, 'base_temp': 20.0}

    n = min(len(daily_sales), len(daily_temps))
    xs = np.array(daily_temps[:n], dtype=float)
    ys = np.array(daily_sales[:n], dtype=float)

    if xs.std() < 0.1:
        return {'temp_coef': 0.0, 'r_squared': 0.0, 'base_temp': float(xs.mean())}

    slope, intercept, r_value, p_value, _ = scipy_stats.linregress(xs, ys)
    r2 = r_value ** 2

    # p値0.05以上（有意でない）は係数を0とみなす
    if p_value > 0.05 or abs(r2) < 0.05:
        slope = 0.0
        r2 = 0.0

    return {
        'temp_coef':  float(slope),
        'r_squared':  float(r2),
        'base_temp':  float(xs.mean()),
    }


def get_temp_adj_factor(jan: str, forecast_temp: float, temp_sens_map: dict) -> float:
    """
    予測気温に基づく売上調整係数を返す (1.0 = 補正なし)
    調整係数 = 1 + coef × (forecast_temp - base_temp) / base_daily
    ただし過剰補正を ±30% に制限
    """
    sens = temp_sens_map.get(jan)
    if not sens or sens['temp_coef'] == 0:
        return 1.0
    coef      = sens['temp_coef']
    base_temp = sens['base_temp']
    base_daily = sens.get('base_daily', 1.0) or 1.0
    adj = coef * (forecast_temp - base_temp) / base_daily
    return max(0.7, min(1.3, 1.0 + adj))


# ──────────────────────────────────────────────────────────────────────────
# ホルト・ウィンタース指数平滑法（Aランク用）
# ──────────────────────────────────────────────────────────────────────────

def holt_winters_forecast(series: list[float], horizon: int = 30,
                           alpha: float = _HW_ALPHA,
                           beta:  float = _HW_BETA) -> list[float]:
    """
    加法型ホルト（トレンドあり・季節性なし）による予測。
    季節性はDBの季節指数で別途補正するため、ここではレベル+トレンドのみ。
    series: 日次売上（古い順）
    Returns: horizon日分の予測値リスト
    """
    if len(series) < 2:
        avg = mean(series) if series else 0.0
        return [avg] * horizon

    # 初期化
    level = series[0]
    trend = series[1] - series[0]

    for obs in series[1:]:
        prev_level = level
        level = alpha * obs + (1 - alpha) * (level + trend)
        trend = beta  * (level - prev_level) + (1 - beta) * trend

    return [max(0.0, level + (i + 1) * trend) for i in range(horizon)]


def quantile_forecast(daily_qtys: list[float]) -> dict:
    """
    過去日次データからQ25/Q50/Q75分位点と標準偏差を計算する。
    Returns: {q25, q50, q75, std, iqr_std}
    """
    if len(daily_qtys) < _MIN_DAYS:
        avg = mean(daily_qtys) if daily_qtys else 0.0
        return {'q25': avg, 'q50': avg, 'q75': avg, 'std': 0.0, 'iqr_std': 0.0}

    arr = np.array(daily_qtys, dtype=float)
    q25 = float(np.percentile(arr, 25))
    q50 = float(np.percentile(arr, 50))
    q75 = float(np.percentile(arr, 75))
    std = float(arr.std())
    iqr_std = (q75 - q25) / _IQR_TO_STD   # 動的標準偏差

    return {'q25': q25, 'q50': q50, 'q75': q75, 'std': std, 'iqr_std': iqr_std}


def dynamic_safety_stock(iqr_std: float, lead_time: int,
                          z: float = _DEFAULT_Z) -> float:
    """
    動的安全在庫 = Z × σ_AI × √L
    σ_AI = (Q75 - Q25) / 1.349
    """
    if iqr_std <= 0 or lead_time <= 0:
        return 0.0
    return z * iqr_std * math.sqrt(lead_time)


# ──────────────────────────────────────────────────────────────────────────
# メイン予測関数
# ──────────────────────────────────────────────────────────────────────────

def build_wholesale_forecast_rows(db, q: str = '') -> list[dict]:
    """
    問屋向けハイブリッド需要予測を実行し、全商品の予測結果を返す。

    Aランク: ホルト・ウィンタース + Q25/Q50/Q75 + 気温補正 + 動的安全在庫
    B/Cランク: 移動平均 + 固定安全在庫
    """
    today    = date.today()
    q_str    = (q or '').strip().lower()

    # ── 設定取得 ──────────────────────────────────────────────────────
    def _setting(key, default):
        row = db.execute("SELECT value FROM settings WHERE key=%s", [key]).fetchone()
        return row['value'] if row else default

    z_score      = float(_setting('safety_level_z', str(_DEFAULT_Z)))
    a_threshold  = float(_setting('abc_a_threshold', '0.70'))
    b_threshold  = float(_setting('abc_b_threshold', '0.90'))

    # ── ABC分析マップ ─────────────────────────────────────────────────
    abc_map = build_abc_map(db, a_threshold, b_threshold)

    # ── 商品一覧 ─────────────────────────────────────────────────────
    products = db.execute("""
        SELECT p.id AS product_id, p.jan, p.product_cd, p.product_name,
               p.supplier_cd, p.supplier_name,
               p.reorder_point, p.order_unit, p.order_qty,
               p.lead_time_days, p.safety_factor, p.manual_adj_factor,
               COALESCE(s.stock_qty, 0) AS stock_qty
        FROM products p
        LEFT JOIN (
            SELECT jan, SUM(quantity) AS stock_qty
            FROM stocks WHERE quantity > 0 GROUP BY jan
        ) s ON s.jan = p.jan
        WHERE p.is_active = 1
        ORDER BY p.supplier_cd, p.product_cd
    """).fetchall()

    # ── 過去180日の日次売上データを一括取得 ──────────────────────────
    sales_rows = db.execute("""
        SELECT jan,
               sale_date::date AS sale_dt,
               SUM(quantity)   AS qty
        FROM sales_history
        WHERE sale_date::date >= CURRENT_DATE - INTERVAL '180 days'
        GROUP BY jan, sale_date::date
        ORDER BY jan, sale_date::date
    """).fetchall()

    # jan → [(date, qty)] の辞書
    sales_by_jan: dict[str, list] = {}
    for r in sales_rows:
        sales_by_jan.setdefault(r['jan'], []).append((r['sale_dt'], int(r['qty'] or 0)))

    # ── 気温データ（過去90日・今後30日予測は平均で代替）────────────────
    temp_rows = db.execute("""
        SELECT obs_date, avg_temp
        FROM weather_data
        WHERE obs_date >= CURRENT_DATE - INTERVAL '90 days'
        ORDER BY obs_date
    """).fetchall()
    temp_map: dict[date, float] = {r['obs_date']: float(r['avg_temp'] or 20) for r in temp_rows}
    avg_temp_recent = mean(temp_map.values()) if temp_map else 20.0

    # ── 気温感応度マップ（temp_sensitivityテーブルからキャッシュ読込）──
    sens_rows = db.execute("SELECT * FROM temp_sensitivity").fetchall()
    temp_sens_map: dict[str, dict] = {
        r['jan']: {'temp_coef': float(r['temp_coef'] or 0),
                   'r_squared': float(r['r_squared'] or 0),
                   'base_temp': float(r['base_temp'] or 20),
                   'base_daily': 1.0}
        for r in sens_rows
    }

    # ── 受注予定マップ（demand_plans）────────────────────────────────
    demand_rows = db.execute("""
        SELECT jan, demand_date::text AS dd, SUM(demand_qty) AS qty
        FROM demand_plans
        WHERE demand_date >= CURRENT_DATE - INTERVAL '1 day'
          AND demand_date <= CURRENT_DATE + INTERVAL '35 days'
        GROUP BY jan, demand_date
    """).fetchall()
    demand_map: dict[str, dict] = {}
    for r in demand_rows:
        demand_map.setdefault(r['jan'], {})[r['dd']] = int(r['qty'] or 0)

    # ── 販促マップ（promotion_plans）────────────────────────────────
    promo_rows = db.execute("""
        SELECT jan, promo_date::text AS pd, promo_name, uplift_factor
        FROM promotion_plans
        WHERE promo_date >= CURRENT_DATE - INTERVAL '1 day'
          AND promo_date <= CURRENT_DATE + INTERVAL '35 days'
    """).fetchall()
    promo_map: dict[str, dict] = {}
    for r in promo_rows:
        promo_map.setdefault(r['jan'], {})[r['pd']] = float(r['uplift_factor'] or 1.0)

    # ── 52週MDプラン（今週のplan_qtyを取得）─────────────────────────
    week_start = today - timedelta(days=today.weekday())
    md_rows = db.execute("""
        SELECT jan, plan_qty, actual_qty
        FROM weekly_md_plans
        WHERE week_start = %s
    """, [week_start]).fetchall()
    md_map: dict[str, dict] = {r['jan']: dict(r) for r in md_rows}

    # ── 商品ごとの予測計算 ────────────────────────────────────────────
    out = []
    for p in products:
        p = dict(p)
        jan = p['jan']

        if q_str and not any(q_str in str(p.get(k) or '').lower()
                              for k in ('jan', 'product_cd', 'product_name', 'supplier_cd', 'supplier_name')):
            continue

        abc_rank = abc_map.get(jan, 'C')
        daily_data = sales_by_jan.get(jan, [])
        daily_qtys = [qty for _, qty in daily_data]
        lt = max(int(p.get('lead_time_days') or 3), 1)
        sf = max(float(p.get('safety_factor') or 1.0), 1.0)
        manual_adj = max(0.1, float(p.get('manual_adj_factor') or 1.0))

        if abc_rank == 'A' and len(daily_qtys) >= _MIN_DAYS:
            # === Aランク: ホルト・ウィンタース + 分位点 + 気温補正 ===

            # ホルト・ウィンタース30日予測
            hw_preds = holt_winters_forecast(daily_qtys, horizon=30)

            # 分位点（過去実績ベース）
            qdata = quantile_forecast(daily_qtys)
            q25, q50, q75 = qdata['q25'], qdata['q50'], qdata['q75']
            iqr_std = qdata['iqr_std']

            # 気温補正係数
            temp_adj = get_temp_adj_factor(jan, avg_temp_recent, temp_sens_map)

            # 30日予測（HW × 気温補正 × 手動補正）
            base_forecast_30 = sum(hw_preds) * temp_adj * manual_adj

            # 受注予定・販促上乗せ
            demand_add = sum(
                demand_map.get(jan, {}).get(str(today + timedelta(days=i)), 0)
                for i in range(30)
            )
            promo_add = 0.0
            hw_daily_avg = base_forecast_30 / 30.0 if base_forecast_30 > 0 else 0.0
            for i in range(30):
                ds = str(today + timedelta(days=i))
                uplift = promo_map.get(jan, {}).get(ds, 1.0)
                if uplift > 1:
                    promo_add += hw_daily_avg * (uplift - 1.0)

            forecast_30 = base_forecast_30 + demand_add + promo_add
            next_daily  = round(forecast_30 / 30.0, 2)

            # 動的安全在庫（分位点ベース）
            dyn_ss = dynamic_safety_stock(iqr_std, lt, z_score)

            # 推奨発注点 = Q75 × LT + 動的安全在庫
            suggested_rp = int(max(0, q75 * lt + dyn_ss + 0.9999))
            algorithm = 'holt_winters+quantile'

        else:
            # === B/Cランク: 単純移動平均 ===
            q25 = q50 = q75 = iqr_std = 0.0
            dyn_ss    = 0.0
            temp_adj  = 1.0

            if daily_qtys:
                # 直近28日の移動平均
                recent = daily_qtys[-28:] if len(daily_qtys) >= 28 else daily_qtys
                avg_daily = mean(recent)
            else:
                avg_daily = 0.0

            # 受注予定反映
            demand_add = sum(
                demand_map.get(jan, {}).get(str(today + timedelta(days=i)), 0)
                for i in range(30)
            )
            forecast_30 = avg_daily * 30 * manual_adj + demand_add
            next_daily  = round(forecast_30 / 30.0, 2)

            # 固定安全在庫（従来方式）
            suggested_rp = int(max(0, next_daily * lt * sf + 0.9999))
            algorithm = 'sma'

        # 推奨発注数 = (LT + 14) × 日次予測
        suggested_oq = int(max(0, next_daily * (lt + 14) + 0.9999))

        # 在庫消化日数
        stock_qty = float(p['stock_qty'] or 0)
        cover_days = round(stock_qty / next_daily, 1) if next_daily > 0 else None

        # 52週MDプランとの比較
        md = md_map.get(jan, {})
        md_plan_qty = int(md.get('plan_qty') or 0)
        md_actual_qty = int(md.get('actual_qty') or 0)
        md_achievement = round(md_actual_qty / md_plan_qty * 100, 1) if md_plan_qty > 0 else None

        p.update({
            'abc_rank':              abc_rank,
            'algorithm':             algorithm,
            'forecast_30d':          round(forecast_30, 1),
            'next_daily_forecast':   next_daily,
            'q25_daily':             round(q25, 2),
            'q50_daily':             round(q50, 2),
            'q75_daily':             round(q75, 2),
            'iqr_std':               round(iqr_std, 2),
            'dynamic_safety_stock':  round(dyn_ss, 1),
            'temp_adj_factor':       round(temp_adj, 3),
            'demand_add_30d':        int(demand_add) if 'demand_add' in dir() else 0,
            'suggested_reorder_point': suggested_rp,
            'suggested_order_qty':   suggested_oq,
            'cover_days':            cover_days,
            'md_plan_qty':           md_plan_qty,
            'md_achievement':        md_achievement,
        })
        out.append(p)

    return out


# ──────────────────────────────────────────────────────────────────────────
# 気温感応度の一括再計算（バックグラウンド処理用）
# ──────────────────────────────────────────────────────────────────────────

def recalc_temp_sensitivity(db) -> int:
    """
    全商品の気温感応度を再計算してtemp_sensitivityテーブルに保存。
    Returns: 更新件数
    """
    # 気温データ取得
    temp_rows = db.execute("""
        SELECT obs_date, avg_temp FROM weather_data
        WHERE avg_temp IS NOT NULL
        ORDER BY obs_date
    """).fetchall()
    if len(temp_rows) < 10:
        return 0

    temp_by_date = {r['obs_date']: float(r['avg_temp']) for r in temp_rows}

    # 商品ごとの日次売上
    sales_rows = db.execute("""
        SELECT jan, sale_date::date AS dt, SUM(quantity) AS qty
        FROM sales_history
        WHERE sale_date::date >= CURRENT_DATE - INTERVAL '365 days'
        GROUP BY jan, sale_date::date
    """).fetchall()

    by_jan: dict[str, dict] = {}
    for r in sales_rows:
        by_jan.setdefault(r['jan'], {})[r['dt']] = int(r['qty'] or 0)

    updated = 0
    for jan, date_qty in by_jan.items():
        matched_dates = [d for d in date_qty if d in temp_by_date]
        if len(matched_dates) < 10:
            continue
        sales_vals = [float(date_qty[d]) for d in matched_dates]
        temp_vals  = [temp_by_date[d] for d in matched_dates]
        sens = calc_temp_sensitivity(sales_vals, temp_vals)
        base_daily = mean(sales_vals) if sales_vals else 1.0

        db.execute("""
            INSERT INTO temp_sensitivity (jan, temp_coef, r_squared, base_temp, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (jan) DO UPDATE
              SET temp_coef=EXCLUDED.temp_coef,
                  r_squared=EXCLUDED.r_squared,
                  base_temp=EXCLUDED.base_temp,
                  updated_at=NOW()
        """, [jan, sens['temp_coef'], sens['r_squared'], sens['base_temp']])
        updated += 1

    db.commit()
    return updated


# ──────────────────────────────────────────────────────────────────────────
# 52週MDプラン生成（新年度の週次計画を前年実績ベースで自動生成）
# ──────────────────────────────────────────────────────────────────────────

def generate_weekly_md_plan(db, jan: str, fiscal_year: int) -> int:
    """
    指定商品・年度の52週MDプランを前年実績から生成。
    Returns: 生成件数
    """
    # 前年の週次実績を取得
    ly_year = fiscal_year - 1
    rows = db.execute("""
        SELECT
            DATE_TRUNC('week', sale_date::date)::date AS week_start,
            EXTRACT(WEEK FROM sale_date::date)::int   AS week_no,
            SUM(quantity) AS qty
        FROM sales_history
        WHERE jan = %s
          AND sale_date::date >= %s::date
          AND sale_date::date <  %s::date
        GROUP BY week_start, week_no
        ORDER BY week_start
    """, [jan, f'{ly_year}-04-01', f'{fiscal_year}-04-01']).fetchall()

    if not rows:
        return 0

    count = 0
    for r in rows:
        db.execute("""
            INSERT INTO weekly_md_plans
              (jan, fiscal_year, week_no, week_start, plan_qty, actual_qty)
            VALUES (%s, %s, %s, %s, %s, 0)
            ON CONFLICT (jan, fiscal_year, week_no) DO NOTHING
        """, [jan, fiscal_year, int(r['week_no']), r['week_start'], int(r['qty'] or 0)])
        count += 1

    db.commit()
    return count
