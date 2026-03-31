"""問屋向け需要予測Blueprint: 予測一覧 / 52週MD / ABC分析 / 気温データ管理"""
import io, csv, math, logging
from datetime import date, timedelta
from flask import Blueprint, render_template, request, redirect, url_for, flash, Response, jsonify
from db import get_db
from auth_helpers import permission_required, login_required
from wholesale_forecast import (
    build_wholesale_forecast_rows,
    build_abc_map,
    generate_weekly_md_plan,
    recalc_temp_sensitivity,
)

logger = logging.getLogger('inventory.forecast')
bp = Blueprint('forecast', __name__)


# ── 需要予測メイン ────────────────────────────────────────────────────────

@bp.route('/reports/forecast/wholesale')
@permission_required('reports')
def forecast_wholesale():
    db = get_db()
    q = request.args.get('q', '').strip().lower()
    rows = build_wholesale_forecast_rows(db, q)

    # ABC別件数集計
    abc_counts = {'A': 0, 'B': 0, 'C': 0}
    for r in rows:
        abc_counts[r.get('abc_rank', 'C')] += 1

    # 設定値取得
    def _setting(key, default):
        row = db.execute("SELECT value FROM settings WHERE key=%s", [key]).fetchone()
        return row['value'] if row else default

    z_score = float(_setting('safety_level_z', '1.65'))

    return render_template('forecast_wholesale.html',
                           rows=rows, q=q,
                           abc_counts=abc_counts,
                           z_score=z_score)


@bp.route('/reports/forecast/wholesale/apply', methods=['POST'])
@permission_required('reports')
def forecast_wholesale_apply():
    db = get_db()
    mode = request.form.get('mode', 'reorder_point')
    q    = request.form.get('q', '')
    rows = build_wholesale_forecast_rows(db, q)
    updated = 0
    for r in rows:
        if mode == 'both':
            db.execute("UPDATE products SET reorder_point=%s, order_qty=%s WHERE id=%s",
                       [r['suggested_reorder_point'], r['suggested_order_qty'], r['product_id']])
        elif mode == 'order_qty':
            db.execute("UPDATE products SET order_qty=%s WHERE id=%s",
                       [r['suggested_order_qty'], r['product_id']])
        else:
            db.execute("UPDATE products SET reorder_point=%s WHERE id=%s",
                       [r['suggested_reorder_point'], r['product_id']])
        updated += 1
    db.commit()
    flash(f'問屋向け予測をもとに {updated} 件の設定を更新しました。', 'success')
    return redirect(url_for('forecast.forecast_wholesale', q=q))


@bp.route('/reports/forecast/wholesale/export')
@permission_required('reports')
def forecast_wholesale_export():
    db  = get_db()
    q   = request.args.get('q', '').strip()
    rows = build_wholesale_forecast_rows(db, q)
    sio  = io.StringIO()
    writer = csv.writer(sio)
    writer.writerow(['仕入先CD', '仕入先名', '商品CD', 'JAN', '商品名',
                     'ABCランク', 'アルゴリズム',
                     '現在庫', '消化日数',
                     '30日予測', '日次予測(Q50)',
                     'Q25日次', 'Q50日次', 'Q75日次',
                     'IQR標準偏差', '動的安全在庫',
                     '気温補正係数',
                     '推奨発注点', '推奨発注数',
                     '52週MD計画数', '達成率(%)'])
    for r in rows:
        writer.writerow([
            r['supplier_cd'], r['supplier_name'], r['product_cd'], r['jan'], r['product_name'],
            r['abc_rank'], r['algorithm'],
            r['stock_qty'], r.get('cover_days', ''),
            r['forecast_30d'], r['next_daily_forecast'],
            r['q25_daily'], r['q50_daily'], r['q75_daily'],
            r['iqr_std'], r['dynamic_safety_stock'],
            r['temp_adj_factor'],
            r['suggested_reorder_point'], r['suggested_order_qty'],
            r.get('md_plan_qty', ''), r.get('md_achievement', ''),
        ])
    data = sio.getvalue().encode('utf-8-sig')
    return Response(data, mimetype='text/csv; charset=utf-8',
                    headers={'Content-Disposition': f'attachment; filename=wholesale_forecast_{date.today()}.csv'})


# ── ABC分析 ───────────────────────────────────────────────────────────────

@bp.route('/reports/abc')
@permission_required('reports')
def abc_analysis():
    db = get_db()

    def _setting(key, default):
        row = db.execute("SELECT value FROM settings WHERE key=%s", [key]).fetchone()
        return row['value'] if row else default

    a_threshold = float(_setting('abc_a_threshold', '0.70'))
    b_threshold = float(_setting('abc_b_threshold', '0.90'))

    rows = db.execute("""
        WITH sales AS (
            SELECT sh.jan,
                   p.product_cd, p.product_name, p.supplier_cd, p.supplier_name,
                   SUM(sh.quantity) AS total_qty,
                   SUM(sh.quantity * COALESCE(NULLIF(p.cost_price,0), 1)) AS sales_value
            FROM sales_history sh
            JOIN products p ON p.jan = sh.jan
            WHERE sh.sale_date::date >= CURRENT_DATE - INTERVAL '365 days'
            GROUP BY sh.jan, p.product_cd, p.product_name, p.supplier_cd, p.supplier_name
        ), ranked AS (
            SELECT *,
                   SUM(sales_value) OVER () AS total_sales,
                   SUM(sales_value) OVER (ORDER BY sales_value DESC NULLS LAST, jan) AS running_sales
            FROM sales
        )
        SELECT *,
               CASE WHEN COALESCE(total_sales,0)=0 THEN 1.0
                    ELSE running_sales / NULLIF(total_sales,0) END AS running_ratio
        FROM ranked
        ORDER BY sales_value DESC
    """).fetchall()

    abc_rows = []
    for r in rows:
        ratio = float(r['running_ratio'] or 1.0)
        if ratio <= a_threshold:
            rank = 'A'
        elif ratio <= b_threshold:
            rank = 'B'
        else:
            rank = 'C'
        d = dict(r)
        d['abc_rank']       = rank
        d['running_ratio_pct'] = round(ratio * 100, 1)
        d['sales_value']    = round(float(r['sales_value'] or 0), 0)
        abc_rows.append(d)

    # 集計
    summary = {
        'A': {'count': 0, 'qty': 0, 'value': 0},
        'B': {'count': 0, 'qty': 0, 'value': 0},
        'C': {'count': 0, 'qty': 0, 'value': 0},
    }
    for r in abc_rows:
        rk = r['abc_rank']
        summary[rk]['count'] += 1
        summary[rk]['qty']   += int(r['total_qty'] or 0)
        summary[rk]['value'] += float(r['sales_value'] or 0)

    return render_template('abc_analysis.html',
                           rows=abc_rows, summary=summary,
                           a_threshold=a_threshold, b_threshold=b_threshold)


@bp.route('/reports/abc/export')
@permission_required('reports')
def abc_export():
    db = get_db()
    rows = db.execute("""
        WITH sales AS (
            SELECT sh.jan, p.product_cd, p.product_name, p.supplier_cd, p.supplier_name,
                   SUM(sh.quantity) AS total_qty,
                   SUM(sh.quantity * COALESCE(NULLIF(p.cost_price,0), 1)) AS sales_value
            FROM sales_history sh
            JOIN products p ON p.jan = sh.jan
            WHERE sh.sale_date::date >= CURRENT_DATE - INTERVAL '365 days'
            GROUP BY sh.jan, p.product_cd, p.product_name, p.supplier_cd, p.supplier_name
        ), ranked AS (
            SELECT *, SUM(sales_value) OVER () AS total,
                   SUM(sales_value) OVER (ORDER BY sales_value DESC NULLS LAST, jan) AS running
            FROM sales
        )
        SELECT *, CASE WHEN COALESCE(total,0)=0 THEN 1.0 ELSE running/NULLIF(total,0) END AS ratio
        FROM ranked ORDER BY sales_value DESC
    """).fetchall()

    def _setting(key, default):
        row = db.execute("SELECT value FROM settings WHERE key=%s", [key]).fetchone()
        return row['value'] if row else default
    a_thr = float(_setting('abc_a_threshold', '0.70'))
    b_thr = float(_setting('abc_b_threshold', '0.90'))

    sio = io.StringIO()
    w   = csv.writer(sio)
    w.writerow(['ABCランク', '仕入先CD', '仕入先名', '商品CD', 'JAN', '商品名', '販売数量', '売上金額', '累計構成比(%)'])
    for r in rows:
        ratio = float(r['ratio'] or 1.0)
        rank  = 'A' if ratio <= a_thr else ('B' if ratio <= b_thr else 'C')
        w.writerow([rank, r['supplier_cd'], r['supplier_name'], r['product_cd'], r['jan'], r['product_name'],
                    r['total_qty'], round(float(r['sales_value'] or 0), 0), round(ratio * 100, 1)])
    return Response(sio.getvalue().encode('utf-8-sig'), mimetype='text/csv; charset=utf-8',
                    headers={'Content-Disposition': f'attachment; filename=abc_analysis_{date.today()}.csv'})


# ── 52週MDプラン ─────────────────────────────────────────────────────────

@bp.route('/reports/weekly_md')
@permission_required('reports')
def weekly_md():
    db = get_db()
    today = date.today()
    # 年度（4月始まり）
    fiscal_year = today.year if today.month >= 4 else today.year - 1
    year_param  = int(request.args.get('year', fiscal_year))
    q           = request.args.get('q', '').strip().lower()

    rows = db.execute("""
        SELECT wm.*, p.product_name, p.product_cd, p.supplier_cd, p.supplier_name
        FROM weekly_md_plans wm
        JOIN products p ON p.jan = wm.jan
        WHERE wm.fiscal_year = %s
        ORDER BY p.supplier_cd, p.product_cd, wm.week_no
    """, [year_param]).fetchall()

    if q:
        rows = [r for r in rows if any(q in str(r.get(k) or '').lower()
                                       for k in ('jan', 'product_cd', 'product_name', 'supplier_cd', 'supplier_name'))]

    # 商品ごとに週次データを集約
    products_md: dict = {}
    weeks_set: set = set()
    for r in rows:
        jan = r['jan']
        wn  = int(r['week_no'])
        weeks_set.add(wn)
        if jan not in products_md:
            products_md[jan] = {
                'jan': jan, 'product_cd': r['product_cd'],
                'product_name': r['product_name'],
                'supplier_cd': r['supplier_cd'],
                'supplier_name': r['supplier_name'],
                'weeks': {}
            }
        products_md[jan]['weeks'][wn] = {
            'plan': int(r['plan_qty'] or 0),
            'actual': int(r['actual_qty'] or 0),
        }

    weeks = sorted(weeks_set)
    prod_list = sorted(products_md.values(), key=lambda x: (x['supplier_cd'], x['product_cd']))

    return render_template('weekly_md.html',
                           prod_list=prod_list, weeks=weeks,
                           fiscal_year=year_param,
                           current_week=today.isocalendar()[1],
                           q=q)


@bp.route('/reports/weekly_md/generate', methods=['POST'])
@permission_required('reports')
def weekly_md_generate():
    db = get_db()
    jan_input = request.form.get('jan', '').strip()
    fiscal_year = int(request.form.get('fiscal_year', date.today().year))

    if not jan_input:
        # 全商品生成
        prods = db.execute("SELECT jan FROM products WHERE is_active=1").fetchall()
        total = 0
        for p in prods:
            total += generate_weekly_md_plan(db, p['jan'], fiscal_year)
        flash(f'{fiscal_year}年度の52週MDプランを全商品({len(prods)}品目)生成しました（{total}週分）。', 'success')
    else:
        n = generate_weekly_md_plan(db, jan_input, fiscal_year)
        flash(f'{jan_input} の{fiscal_year}年度52週MDプランを生成しました（{n}週分）。', 'success')

    return redirect(url_for('forecast.weekly_md', year=fiscal_year))


@bp.route('/reports/weekly_md/update', methods=['POST'])
@permission_required('reports')
def weekly_md_update():
    db  = get_db()
    jan = request.form.get('jan', '').strip()
    fiscal_year = int(request.form.get('fiscal_year', date.today().year))
    week_no     = int(request.form.get('week_no', 1))
    plan_qty    = int(request.form.get('plan_qty', 0) or 0)
    actual_qty  = int(request.form.get('actual_qty', 0) or 0)

    db.execute("""
        UPDATE weekly_md_plans
        SET plan_qty=%s, actual_qty=%s, updated_at=NOW()
        WHERE jan=%s AND fiscal_year=%s AND week_no=%s
    """, [plan_qty, actual_qty, jan, fiscal_year, week_no])
    db.commit()
    return jsonify({'ok': True})


@bp.route('/reports/weekly_md/export')
@permission_required('reports')
def weekly_md_export():
    db = get_db()
    fiscal_year = int(request.args.get('year', date.today().year))
    rows = db.execute("""
        SELECT wm.*, p.product_name, p.product_cd, p.supplier_cd, p.supplier_name
        FROM weekly_md_plans wm JOIN products p ON p.jan=wm.jan
        WHERE wm.fiscal_year=%s
        ORDER BY p.supplier_cd, p.product_cd, wm.week_no
    """, [fiscal_year]).fetchall()

    sio = io.StringIO()
    w   = csv.writer(sio)
    w.writerow(['年度', '週番号', '週開始日', '仕入先CD', '仕入先名', '商品CD', 'JAN', '商品名',
                '計画数量', '実績数量', '達成率(%)'])
    for r in rows:
        plan = int(r['plan_qty'] or 0)
        actual = int(r['actual_qty'] or 0)
        ach = round(actual / plan * 100, 1) if plan > 0 else ''
        w.writerow([r['fiscal_year'], r['week_no'], r['week_start'],
                    r['supplier_cd'], r['supplier_name'], r['product_cd'], r['jan'], r['product_name'],
                    plan, actual, ach])
    return Response(sio.getvalue().encode('utf-8-sig'), mimetype='text/csv; charset=utf-8',
                    headers={'Content-Disposition': f'attachment; filename=weekly_md_{fiscal_year}.csv'})


# ── 気温データ管理 ───────────────────────────────────────────────────────

@bp.route('/reports/weather')
@permission_required('reports')
def weather_data():
    db = get_db()
    rows = db.execute("""
        SELECT * FROM weather_data
        ORDER BY obs_date DESC LIMIT 90
    """).fetchall()
    return render_template('weather_data.html', rows=rows)


@bp.route('/reports/weather/add', methods=['POST'])
@permission_required('reports')
def weather_add():
    db       = get_db()
    obs_date = request.form.get('obs_date', '').strip()
    location = request.form.get('location', '東京').strip()
    avg_temp = request.form.get('avg_temp', '')
    max_temp = request.form.get('max_temp', '')
    min_temp = request.form.get('min_temp', '')
    precip   = request.form.get('precipitation', '0')

    if not obs_date or avg_temp == '':
        flash('日付と平均気温は必須です。', 'danger')
        return redirect(url_for('forecast.weather_data'))

    db.execute("""
        INSERT INTO weather_data (obs_date, location, avg_temp, max_temp, min_temp, precipitation, source)
        VALUES (%s, %s, %s, %s, %s, %s, 'manual')
        ON CONFLICT (obs_date, location) DO UPDATE
          SET avg_temp=EXCLUDED.avg_temp, max_temp=EXCLUDED.max_temp,
              min_temp=EXCLUDED.min_temp, precipitation=EXCLUDED.precipitation
    """, [obs_date, location,
          float(avg_temp) if avg_temp else None,
          float(max_temp) if max_temp else None,
          float(min_temp) if min_temp else None,
          float(precip) if precip else 0])
    db.commit()
    flash(f'{obs_date} の気温データを登録しました。', 'success')
    return redirect(url_for('forecast.weather_data'))


@bp.route('/reports/weather/import', methods=['POST'])
@permission_required('reports')
def weather_import():
    db = get_db()
    f  = request.files.get('file')
    if not f or not f.filename:
        flash('ファイルを選択してください。', 'danger')
        return redirect(url_for('forecast.weather_data'))

    content = f.read().decode('utf-8-sig', errors='ignore')
    reader  = csv.DictReader(io.StringIO(content))
    created = 0
    errors  = []
    for i, row in enumerate(reader, 2):
        try:
            obs_date = (row.get('日付') or row.get('obs_date') or '').strip()
            avg_temp = row.get('平均気温') or row.get('avg_temp') or ''
            max_temp = row.get('最高気温') or row.get('max_temp') or ''
            min_temp = row.get('最低気温') or row.get('min_temp') or ''
            location = (row.get('地点') or row.get('location') or '東京').strip()
            if not obs_date or avg_temp == '':
                continue
            db.execute("""
                INSERT INTO weather_data (obs_date, location, avg_temp, max_temp, min_temp, source)
                VALUES (%s, %s, %s, %s, %s, 'csv')
                ON CONFLICT (obs_date, location) DO UPDATE
                  SET avg_temp=EXCLUDED.avg_temp, max_temp=EXCLUDED.max_temp, min_temp=EXCLUDED.min_temp
            """, [obs_date, location,
                  float(avg_temp) if avg_temp else None,
                  float(max_temp) if max_temp else None,
                  float(min_temp) if min_temp else None])
            created += 1
        except Exception as e:
            errors.append(f'行{i}: {e}')
    db.commit()
    flash(f'{created}件の気温データをインポートしました。' + (f' エラー: {len(errors)}件' if errors else ''), 'success' if not errors else 'warning')
    return redirect(url_for('forecast.weather_data'))


@bp.route('/reports/weather/recalc_sensitivity', methods=['POST'])
@permission_required('reports')
def weather_recalc_sensitivity():
    db = get_db()
    n  = recalc_temp_sensitivity(db)
    flash(f'気温感応度を {n} 商品分 再計算しました。', 'success')
    return redirect(url_for('forecast.weather_data'))


@bp.route('/reports/weather/template')
@permission_required('reports')
def weather_template():
    sio = io.StringIO()
    w   = csv.writer(sio)
    w.writerow(['日付', '地点', '平均気温', '最高気温', '最低気温'])
    w.writerow(['2026-04-01', '東京', '16.5', '21.0', '12.0'])
    w.writerow(['2026-04-02', '東京', '17.2', '22.5', '13.1'])
    from urllib.parse import quote
    return Response(sio.getvalue().encode('utf-8-sig'), mimetype='text/csv',
                    headers={'Content-Disposition': "attachment; filename*=UTF-8''" + quote('気温データテンプレート.csv')})
