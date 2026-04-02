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


def _save_sens_log(db, count: int, trigger: str):
    """気温感応度再計算の実行履歴をsettingsテーブルに保存"""
    from datetime import datetime as _dt
    labels = {'monthly': '月次自動', 'import': 'インポート後自動', 'manual': '手動実行'}
    val = f"{_dt.now().strftime('%Y-%m-%d %H:%M')} ／ {count}商品 ／ {labels.get(trigger, trigger)}"
    db.execute("""
        INSERT INTO settings (key, value) VALUES ('temp_sensitivity_last_run', %s)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """, [val])
    db.commit()


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
    location_summary = db.execute("""
        SELECT location,
               COUNT(*)          AS cnt,
               MIN(obs_date)     AS oldest,
               MAX(obs_date)     AS newest,
               AVG(avg_temp)     AS avg_temp_avg
        FROM weather_data
        GROUP BY location
        ORDER BY location
    """).fetchall()
    _sens_row = db.execute(
        "SELECT value FROM settings WHERE key='temp_sensitivity_last_run'"
    ).fetchone()
    sens_last_run = _sens_row['value'] if _sens_row else None
    return render_template('weather_data.html', rows=rows,
                           location_summary=location_summary,
                           sens_last_run=sens_last_run)


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
            avg_temp = row.get('平均気温(℃)') or row.get('平均気温') or row.get('avg_temp') or ''
            max_temp = row.get('最高気温(℃)') or row.get('最高気温') or row.get('max_temp') or ''
            min_temp = row.get('最低気温(℃)') or row.get('最低気温') or row.get('min_temp') or ''
            location = (row.get('地点') or row.get('location') or '東京').strip()
            precip   = row.get('降水量(mm)') or row.get('降水量') or 0
            if not obs_date or avg_temp == '':
                continue
            db.execute("""
                INSERT INTO weather_data (obs_date, location, avg_temp, max_temp, min_temp, precipitation, source)
                VALUES (%s, %s, %s, %s, %s, %s, 'csv')
                ON CONFLICT (obs_date, location) DO UPDATE
                  SET avg_temp=EXCLUDED.avg_temp, max_temp=EXCLUDED.max_temp,
                      min_temp=EXCLUDED.min_temp, precipitation=EXCLUDED.precipitation
            """, [obs_date, location,
                  float(avg_temp) if avg_temp else None,
                  float(max_temp) if max_temp else None,
                  float(min_temp) if min_temp else None,
                  float(precip) if precip else None])
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
    _save_sens_log(db, n, 'manual')
    flash(f'気温感応度を {n} 商品分 再計算しました。', 'success')
    return redirect(url_for('forecast.weather_data'))


@bp.route('/reports/weather/template')
@permission_required('reports')
def weather_template():
    import datetime
    sio = io.StringIO()
    w   = csv.writer(sio)
    w.writerow(['日付', '地点', '平均気温(℃)', '最高気温(℃)', '最低気温(℃)', '降水量(mm)'])
    today = datetime.date.today()
    for i in range(30):
        d = today - datetime.timedelta(days=29 - i)
        w.writerow([d.strftime('%Y-%m-%d'), '東京', '15.0', '20.0', '10.0', '0.0'])
    from urllib.parse import quote
    return Response(sio.getvalue().encode('utf-8-sig'), mimetype='text/csv',
                    headers={'Content-Disposition': "attachment; filename*=UTF-8''" + quote('気温データテンプレート.csv')})


@bp.route('/reports/weather/excel_template')
@permission_required('reports')
def weather_excel_template():
    """気象データ Excelテンプレートをダウンロード（days/locations パラメータ対応）"""
    import openpyxl
    from openpyxl.styles import PatternFill, Font, Alignment
    import io as _bio
    import datetime as _dt
    try:
        days = max(1, min(int(request.args.get('days', 30)), 365))
    except (ValueError, TypeError):
        days = 30
    # 地点リスト（クエリパラメータ locations=東京,大阪 で指定可、省略時は東京のみ）
    locs_param = request.args.get('locations', '東京')
    locations  = [l.strip() for l in locs_param.split(',') if l.strip()] or ['東京']

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = '気象データ'
    headers = ['日付', '地点', '平均気温(℃)', '最高気温(℃)', '最低気温(℃)', '降水量(mm)']
    hfill = PatternFill(fill_type='solid', fgColor='1D4ED8')
    hfont = Font(color='FFFFFF', bold=True)
    for col, h in enumerate(headers, 1):
        c = ws.cell(row=1, column=col, value=h)
        c.fill = hfill
        c.font = hfont
        c.alignment = Alignment(horizontal='center')
    today = _dt.date.today()
    for loc in locations:
        for i in range(days):
            d = today - _dt.timedelta(days=days - 1 - i)
            ws.append([d.strftime('%Y-%m-%d'), loc, '', '', '', ''])
    ws.column_dimensions['A'].width = 14
    ws.column_dimensions['B'].width = 10
    for col in ['C', 'D', 'E', 'F']:
        ws.column_dimensions[col].width = 14
    buf = _bio.BytesIO()
    wb.save(buf)
    buf.seek(0)
    from urllib.parse import quote
    fname = quote('気象データ_テンプレート.xlsx')
    return Response(buf.read(),
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    headers={'Content-Disposition': f"attachment; filename*=UTF-8''{fname}"})


@bp.route('/reports/weather/import_page')
@permission_required('reports')
def weather_import_page():
    """過去気象データ 一括インポート専用ページ"""
    from flask import session
    result = session.pop('weather_import_result', None)
    return render_template('weather_import.html', result=result)


@bp.route('/reports/weather/excel_import', methods=['POST'])
@permission_required('reports')
def weather_excel_import():
    """気象データをExcel/CSVから一括インポート"""
    import openpyxl
    import datetime as _dt
    db = get_db()
    f  = request.files.get('excel_file')
    if not f or not f.filename:
        flash('ファイルを選択してください。', 'danger')
        return redirect(url_for('forecast.weather_data'))

    created = 0
    errors  = []
    filename = f.filename.lower()

    if filename.endswith('.csv'):
        # CSV パス
        content = f.read().decode('utf-8-sig', errors='ignore')
        reader  = csv.DictReader(io.StringIO(content))
        for i, row in enumerate(reader, 2):
            try:
                obs_date = (row.get('日付') or row.get('obs_date') or '').strip()
                location = (row.get('地点') or row.get('location') or '東京').strip()
                avg_temp = row.get('平均気温(℃)') or row.get('平均気温') or row.get('avg_temp') or ''
                max_temp = row.get('最高気温(℃)') or row.get('最高気温') or row.get('max_temp') or ''
                min_temp = row.get('最低気温(℃)') or row.get('最低気温') or row.get('min_temp') or ''
                precip   = row.get('降水量(mm)')  or row.get('降水量')  or 0
                if not obs_date or avg_temp == '':
                    continue
                db.execute("""
                    INSERT INTO weather_data
                      (obs_date, location, avg_temp, max_temp, min_temp, precipitation, source)
                    VALUES (%s, %s, %s, %s, %s, %s, 'import')
                    ON CONFLICT (obs_date, location) DO UPDATE
                      SET avg_temp=EXCLUDED.avg_temp, max_temp=EXCLUDED.max_temp,
                          min_temp=EXCLUDED.min_temp, precipitation=EXCLUDED.precipitation
                """, [obs_date, location,
                      float(avg_temp) if avg_temp else None,
                      float(max_temp) if max_temp else None,
                      float(min_temp) if min_temp else None,
                      float(precip)   if precip   else 0])
                created += 1
            except Exception as e:
                errors.append(f'行{i}: {e}')
    else:
        # Excel パス
        try:
            wb = openpyxl.load_workbook(f, data_only=True)
            ws = wb.active
        except Exception as e:
            flash(f'Excelファイルの読み込みに失敗しました: {e}', 'danger')
            return redirect(url_for('forecast.weather_data'))

        header_row = [str(c.value).strip() if c.value is not None else '' for c in ws[1]]

        def _col(names):
            for n in names:
                for j, h in enumerate(header_row):
                    if n in h:
                        return j
            return -1

        idx_date = _col(['日付', 'obs_date'])
        idx_loc  = _col(['地点', 'location'])
        idx_avg  = _col(['平均気温', 'avg_temp'])
        idx_max  = _col(['最高気温', 'max_temp'])
        idx_min  = _col(['最低気温', 'min_temp'])
        idx_pre  = _col(['降水量',   'precipitation'])

        if idx_date < 0 or idx_avg < 0:
            flash('必須列「日付」「平均気温」が見つかりません。テンプレートを確認してください。', 'danger')
            return redirect(url_for('forecast.weather_data'))

        for i, row in enumerate(ws.iter_rows(min_row=2, values_only=True), 2):
            try:
                obs_val = row[idx_date] if idx_date < len(row) else None
                if obs_val is None:
                    continue
                if isinstance(obs_val, (_dt.date, _dt.datetime)):
                    obs_date = obs_val.strftime('%Y-%m-%d')
                else:
                    obs_date = str(obs_val).strip()
                if not obs_date:
                    continue
                location = str(row[idx_loc]).strip() if idx_loc >= 0 and idx_loc < len(row) and row[idx_loc] else '東京'
                avg_temp = row[idx_avg] if idx_avg >= 0 and idx_avg < len(row) else None
                max_temp = row[idx_max] if idx_max >= 0 and idx_max < len(row) else None
                min_temp = row[idx_min] if idx_min >= 0 and idx_min < len(row) else None
                precip   = row[idx_pre] if idx_pre >= 0 and idx_pre < len(row) else 0
                if avg_temp is None:
                    continue
                db.execute("""
                    INSERT INTO weather_data
                      (obs_date, location, avg_temp, max_temp, min_temp, precipitation, source)
                    VALUES (%s, %s, %s, %s, %s, %s, 'import')
                    ON CONFLICT (obs_date, location) DO UPDATE
                      SET avg_temp=EXCLUDED.avg_temp, max_temp=EXCLUDED.max_temp,
                          min_temp=EXCLUDED.min_temp, precipitation=EXCLUDED.precipitation
                """, [obs_date, location,
                      float(avg_temp) if avg_temp is not None else None,
                      float(max_temp) if max_temp is not None else None,
                      float(min_temp) if min_temp is not None else None,
                      float(precip)   if precip   is not None else 0])
                created += 1
            except Exception as e:
                errors.append(f'行{i}: {e}')

    db.commit()

    # インポート後に気温感応度を自動再計算
    sens_n = 0
    if created > 0:
        try:
            sens_n = recalc_temp_sensitivity(db)
            if sens_n:
                _save_sens_log(db, sens_n, 'import')
        except Exception:
            sens_n = 0

    # 地点別インポート結果を集計してセッションへ
    from flask import session
    loc_summary = {}
    for row_data in db.execute("""
        SELECT location, COUNT(*) AS cnt, MIN(obs_date) AS oldest, MAX(obs_date) AS newest
        FROM weather_data WHERE source='import'
        GROUP BY location ORDER BY location
    """).fetchall():
        loc_summary[row_data['location']] = {
            'cnt':    row_data['cnt'],
            'oldest': str(row_data['oldest']),
            'newest': str(row_data['newest']),
        }
    session['weather_import_result'] = {
        'created':     created,
        'error_count': len(errors),
        'errors':      errors[:5],
        'loc_summary': loc_summary,
    }
    redirect_to = request.form.get('redirect_to', 'import_page')
    sens_msg = f' 気温感応度 {sens_n}商品 再計算済み。' if sens_n else ''
    if redirect_to == 'weather_data':
        flash(f'{created}件インポート完了。{sens_msg}', 'success' if not errors else 'warning')
        return redirect(url_for('forecast.weather_data'))
    session['weather_import_result']['sens_n'] = sens_n
    return redirect(url_for('forecast.weather_import_page'))


@bp.route('/reports/weather/fetch_api', methods=['POST'])
@permission_required('reports')
def weather_fetch_api():
    """Open-Meteo APIから気象データを取得してDBに保存"""
    import urllib.request
    import urllib.parse
    db = get_db()
    location = (request.form.get('location') or '東京').strip()
    try:
        lat = float(request.form.get('lat') or 35.6897)
        lon = float(request.form.get('lon') or 139.6922)
    except (ValueError, TypeError):
        lat, lon = 35.6897, 139.6922
    try:
        days = int(request.form.get('days') or 30)
        days = max(1, min(days, 365))
    except (ValueError, TypeError):
        days = 30

    import json as _json
    from datetime import date as _date, timedelta as _timedelta
    end_date   = _date.today()
    start_date = end_date - _timedelta(days=days - 1)

    if days <= 92:
        # forecast API（直近92日まで）
        params = urllib.parse.urlencode({
            'latitude':      lat,
            'longitude':     lon,
            'daily':         'temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum',
            'timezone':      'Asia/Tokyo',
            'past_days':     days,
            'forecast_days': 0,
        })
        url = f'https://api.open-meteo.com/v1/forecast?{params}'
    else:
        # archive API（93日〜365日）
        params = urllib.parse.urlencode({
            'latitude':   lat,
            'longitude':  lon,
            'daily':      'temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum',
            'timezone':   'Asia/Tokyo',
            'start_date': start_date.isoformat(),
            'end_date':   end_date.isoformat(),
        })
        url = f'https://archive-api.open-meteo.com/v1/archive?{params}'

    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = _json.loads(resp.read())
    except Exception as e:
        flash(f'API取得エラー: {e}', 'danger')
        return redirect(url_for('forecast.weather_data'))

    daily = data.get('daily', {})
    dates     = daily.get('time', [])
    max_temps = daily.get('temperature_2m_max', [])
    min_temps = daily.get('temperature_2m_min', [])
    avg_temps = daily.get('temperature_2m_mean', [])
    precips   = daily.get('precipitation_sum', [])

    created = 0
    errors  = []
    for i, obs_date in enumerate(dates):
        try:
            avg_t = avg_temps[i] if i < len(avg_temps) else None
            max_t = max_temps[i] if i < len(max_temps) else None
            min_t = min_temps[i] if i < len(min_temps) else None
            prec  = precips[i]   if i < len(precips)   else None
            if avg_t is None:
                continue
            db.execute("""
                INSERT INTO weather_data (obs_date, location, avg_temp, max_temp, min_temp, precipitation, source)
                VALUES (%s, %s, %s, %s, %s, %s, 'api')
                ON CONFLICT (obs_date, location) DO UPDATE
                  SET avg_temp=EXCLUDED.avg_temp, max_temp=EXCLUDED.max_temp,
                      min_temp=EXCLUDED.min_temp, precipitation=EXCLUDED.precipitation,
                      source='api'
            """, [obs_date, location, avg_t, max_t, min_t, prec])
            created += 1
        except Exception as e:
            errors.append(f'{obs_date}: {e}')
    db.commit()
    msg = f'{location}: {created}件の気象データをAPIから取得しました。'
    if errors:
        msg += f' エラー: {len(errors)}件'
    flash(msg, 'success' if not errors else 'warning')
    return redirect(url_for('forecast.weather_data'))


@bp.route('/reports/weather/fetch_multi', methods=['POST'])
@permission_required('reports')
def weather_fetch_multi():
    """複数地点の気象データを一括取得"""
    import urllib.request
    import urllib.parse
    db = get_db()
    locations_raw = request.form.getlist('locations')
    try:
        days = int(request.form.get('days') or 30)
        days = max(1, min(days, 92))
    except (ValueError, TypeError):
        days = 30

    if not locations_raw:
        flash('地点を選択してください。', 'warning')
        return redirect(url_for('forecast.weather_data'))

    total_created = 0
    total_errors  = 0
    failed_locs   = []

    for loc_str in locations_raw:
        parts = loc_str.split('|')
        if len(parts) != 3:
            continue
        location = parts[0].strip()
        try:
            lat = float(parts[1])
            lon = float(parts[2])
        except ValueError:
            continue

        params = urllib.parse.urlencode({
            'latitude':  lat,
            'longitude': lon,
            'daily':     'temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum',
            'timezone':  'Asia/Tokyo',
            'past_days': days,
            'forecast_days': 0,
        })
        url = f'https://api.open-meteo.com/v1/forecast?{params}'
        try:
            import json as _json
            with urllib.request.urlopen(url, timeout=15) as resp:
                data = _json.loads(resp.read())
        except Exception as e:
            failed_locs.append(f'{location}({e})')
            total_errors += 1
            continue

        daily     = data.get('daily', {})
        dates     = daily.get('time', [])
        max_temps = daily.get('temperature_2m_max', [])
        min_temps = daily.get('temperature_2m_min', [])
        avg_temps = daily.get('temperature_2m_mean', [])
        precips   = daily.get('precipitation_sum', [])

        for i, obs_date in enumerate(dates):
            try:
                avg_t = avg_temps[i] if i < len(avg_temps) else None
                max_t = max_temps[i] if i < len(max_temps) else None
                min_t = min_temps[i] if i < len(min_temps) else None
                prec  = precips[i]   if i < len(precips)   else None
                if avg_t is None:
                    continue
                db.execute("""
                    INSERT INTO weather_data
                      (obs_date, location, avg_temp, max_temp, min_temp, precipitation, source)
                    VALUES (%s, %s, %s, %s, %s, %s, 'api')
                    ON CONFLICT (obs_date, location) DO UPDATE
                      SET avg_temp=EXCLUDED.avg_temp, max_temp=EXCLUDED.max_temp,
                          min_temp=EXCLUDED.min_temp, precipitation=EXCLUDED.precipitation,
                          source='api'
                """, [obs_date, location, avg_t, max_t, min_t, prec])
                total_created += 1
            except Exception:
                total_errors += 1

    db.commit()
    loc_count = len(locations_raw) - len(failed_locs)
    msg = f'{loc_count}地点・{total_created}件の気象データを取得しました。'
    if failed_locs:
        msg += f' 取得失敗: {", ".join(failed_locs[:3])}{"他" if len(failed_locs)>3 else ""}'
    flash(msg, 'success' if not failed_locs else 'warning')
    return redirect(url_for('forecast.weather_data'))
