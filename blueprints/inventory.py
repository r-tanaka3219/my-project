"""在庫管理 Blueprint"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, Response, session
from datetime import date, timedelta
import csv, io, logging
from db import get_db
from auth_helpers import login_required, permission_required
from helpers import _to_int, _build_picking_plan, _build_replenishment_rows, _excel_bytes_from_rows

logger = logging.getLogger('inventory.inventory')
bp = Blueprint('inventory', __name__)


@bp.route('/inventory')
@permission_required('inventory')
def inventory():
    today = date.today()
    db = get_db()
    q = request.args.get('q','').strip()
    sql = """
        SELECT s.*, p.reorder_point, p.expiry_alert_days, p.product_cd, p.location_code AS product_location,
               COALESCE(NULLIF(s.location_code,''), NULLIF(p.location_code,''), '') as display_location,
               COALESCE(t.total_qty, 0) AS total_qty
        FROM stocks s
        LEFT JOIN products p ON s.jan=p.jan
        LEFT JOIN (SELECT jan, SUM(quantity) AS total_qty FROM stocks GROUP BY jan) t ON t.jan=s.jan
        WHERE s.quantity>0
    """
    params = []
    if q:
        sql += " AND (s.jan ILIKE %s OR p.product_cd ILIKE %s OR p.product_name ILIKE %s OR p.supplier_cd ILIKE %s OR p.supplier_name ILIKE %s)"
        params += [f'%{q}%'] * 5
    sql += " ORDER BY CAST(NULLIF(regexp_replace(p.supplier_cd,'[^0-9]','','g'),'') AS BIGINT) NULLS LAST, CAST(NULLIF(regexp_replace(p.product_cd,'[^0-9]','','g'),'') AS BIGINT) NULLS LAST, s.expiry_date ASC"
    stocks = db.execute(sql, params).fetchall()
    alert_date = str(today + timedelta(days=30))
    return render_template('inventory.html', stocks=stocks, today=today, alert_date=alert_date, q=q)


@bp.route('/inventory/<int:stock_id>/edit', methods=['POST'])
@permission_required('inventory')
def inventory_edit(stock_id):
    db = get_db()
    s = db.execute("SELECT * FROM stocks WHERE id=%s", [stock_id]).fetchone()
    if not s:
        flash('在庫データが見つかりません', 'danger')
        return redirect(url_for('inventory.inventory'))
    qty = request.form.get('quantity','').strip()
    expiry = request.form.get('expiry_date','').strip()
    lot_no = request.form.get('lot_no','').strip()
    location_code = request.form.get('location_code','').strip()
    if not qty or int(qty) < 0:
        flash('数量が不正です', 'danger')
        return redirect(url_for('inventory.inventory'))
    qty = int(qty)
    # 賞味期限フォーマット整形
    if expiry:
        expiry = expiry.replace('/', '-')
        parts = expiry.split('-')
        if len(parts) == 3:
            expiry = f'{parts[0]}-{int(parts[1]):02d}-{int(parts[2]):02d}'
    before = s['quantity']
    db.execute("""
        UPDATE stocks SET quantity=%s, expiry_date=%s, lot_no=%s, location_code=%s WHERE id=%s
    """, [qty, expiry, lot_no, location_code, stock_id])
    db.execute("""
        INSERT INTO stock_movements
        (jan,product_name,move_type,quantity,before_qty,after_qty,note,source_file,move_date)
        VALUES (%s,%s,'adjust',%s,%s,%s,'在庫直接編集','manual',%s)
    """, [s['jan'], s['product_name'], abs(qty - before), before, qty, str(date.today())])
    db.commit()
    flash(f"{s['product_name']} の在庫を更新しました（数量:{before}→{qty}）", 'success')
    return redirect(url_for('inventory.inventory'))


@bp.route('/inventory/dispose/<int:stock_id>', methods=['POST'])
@login_required
def inventory_dispose(stock_id):
    db = get_db()
    s = db.execute("SELECT * FROM stocks WHERE id=%s FOR UPDATE", [stock_id]).fetchone()
    if not s:
        flash('在庫が見つかりません', 'error')
        return redirect(url_for('inventory.inventory'))
    reason_type = request.form.get('reason_type', '').strip()
    reason_note = request.form.get('reason_note', '').strip()
    qty = int(request.form.get('quantity', s['quantity']))
    if qty <= 0 or qty > s['quantity']:
        flash('数量が不正です', 'error')
        return redirect(url_for('inventory.inventory'))
    today = str(date.today())
    # 原価・ロス金額取得
    prod = db.execute("SELECT cost_price FROM products WHERE jan=%s", [s['jan']]).fetchone()
    cost_price = float(prod['cost_price'] or 0) if prod else 0
    loss_amount = round(cost_price * qty, 2)
    # 廃棄テーブルに追加
    db.execute("""
        INSERT INTO disposed_stocks
        (jan,product_name,supplier_cd,supplier_name,product_cd,quantity,expiry_date,lot_no,
         reason_type,reason_note,disposed_at,cost_price,loss_amount)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, [s['jan'],s['product_name'],s.get('supplier_cd',''),s.get('supplier_name',''),
           s.get('product_cd',''),qty,s['expiry_date'],s.get('lot_no',''),
           reason_type,reason_note,today,cost_price,loss_amount])
    # 在庫から減算
    if qty >= s['quantity']:
        db.execute("DELETE FROM stocks WHERE id=%s", [stock_id])
    else:
        db.execute("UPDATE stocks SET quantity=quantity-%s WHERE id=%s", [qty, stock_id])
    # 移動履歴
    before = db.execute("SELECT COALESCE(SUM(quantity),0) AS _sum FROM stocks WHERE jan=%s",[s['jan']]).fetchone()['_sum']
    db.execute("""
        INSERT INTO stock_movements
        (jan,product_name,move_type,quantity,before_qty,after_qty,note,source_file,move_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, [s['jan'],s['product_name'],'dispose',qty,before+qty,before,
           ('退避: ' + reason_type + ' ' + reason_note).strip(),'manual',today])
    db.commit()
    flash(f'「{s["product_name"]}」{qty}個を退避しました', 'success')
    return redirect(url_for('inventory.inventory'))

@bp.route('/inventory/disposed/<int:did>/restore', methods=['POST'])
@login_required
def disposed_restore(did):
    db = get_db()
    d = db.execute("SELECT * FROM disposed_stocks WHERE id=%s", [did]).fetchone()
    if not d:
        flash('退避在庫が見つかりません', 'error')
        return redirect(url_for('inventory.inventory_disposed'))
    # 商品マスタ確認
    product = db.execute("SELECT * FROM products WHERE jan=%s AND is_active=1", [d['jan']]).fetchone()
    if not product:
        flash(f'JAN {d["jan"]} の商品マスタが見つかりません', 'error')
        return redirect(url_for('inventory.inventory_disposed'))
    before = db.execute("SELECT COALESCE(SUM(quantity),0) AS _sum FROM stocks WHERE jan=%s",[d['jan']]).fetchone()['_sum']
    # 在庫に戻す
    db.execute("""
        INSERT INTO stocks
        (product_id,jan,product_name,supplier_cd,supplier_name,
         product_cd,unit_qty,quantity,expiry_date,lot_no)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, [product['id'],d['jan'],d['product_name'],
           product['supplier_cd'],product['supplier_name'],
           product['product_cd'],product['unit_qty'],
           d['quantity'],d['expiry_date'],d['lot_no']])
    db.execute("""
        INSERT INTO stock_movements
        (jan,product_name,move_type,quantity,before_qty,after_qty,note,source_file,move_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, [d['jan'],d['product_name'],'receipt',d['quantity'],before,before+d['quantity'],
           '退避在庫を復元','manual',str(date.today())])
    # 退避テーブルから削除
    db.execute("DELETE FROM disposed_stocks WHERE id=%s", [did])
    db.commit()
    flash(f'「{d["product_name"]}」{d["quantity"]}個を在庫に戻しました', 'success')
    return redirect(url_for('inventory.inventory_disposed'))


@bp.route('/inventory/disposed')
@login_required
def inventory_disposed():
    db = get_db()
    q = request.args.get('q','').strip()
    rows = db.execute("SELECT * FROM disposed_stocks ORDER BY disposed_at DESC, id DESC").fetchall()
    if q:
        rows = [r for r in rows if q.lower() in (r['jan'] or '').lower()
                or q.lower() in (r['product_cd'] or '').lower()
                or q.lower() in (r['product_name'] or '').lower()
                or q.lower() in (r['supplier_cd'] or '').lower()
                or q.lower() in (r['supplier_name'] or '').lower()]
    return render_template('inventory_disposed.html', rows=rows, q=q)

@bp.route('/inventory/disposed/export')
@login_required
def inventory_disposed_export():
    import openpyxl, io
    db = get_db()
    rows = db.execute("SELECT * FROM disposed_stocks ORDER BY disposed_at DESC, id DESC").fetchall()
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = '廃棄退避在庫'
    headers = ['退避日','仕入先CD','仕入先名','商品CD','JANコード','商品名','数量','原価(円)','ロス金額(円)','賞味期限','ロット番号','退避理由','備考']
    ws.append(headers)
    for r in rows:
        ws.append([r['disposed_at'],r['supplier_cd'],r['supplier_name'],r['product_cd'],
                   r['jan'],r['product_name'],r['quantity'],
                   float(r['cost_price'] or 0),float(r['loss_amount'] or 0),
                   r['expiry_date'],r['lot_no'],r['reason_type'],r['reason_note']])
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = 18
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return Response(buf.getvalue(), mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    headers={'Content-Disposition': f'attachment; filename=disposed_stocks_{date.today()}.xlsx'})


@bp.route('/inventory/transfers')
@permission_required('inventory')
def inventory_transfers():
    db = get_db()
    rows = db.execute("""
        SELECT t.*, p.product_cd, p.supplier_cd, p.supplier_name, p.product_name, p.jan
        FROM stock_transfers t
        LEFT JOIN products p ON t.jan=p.jan
        ORDER BY t.created_at DESC, t.id DESC
        LIMIT 300
    """).fetchall()
    return render_template('inventory_transfers.html', rows=rows)

@bp.route('/inventory/transfer/<int:stock_id>', methods=['POST'])
@permission_required('inventory')
def inventory_transfer(stock_id):
    db = get_db()
    qty = _to_int(request.form.get('quantity'))
    to_location = (request.form.get('to_location_code') or '').strip()
    note = (request.form.get('note') or '').strip()
    if qty <= 0 or not to_location:
        flash('移動数量と移動先ロケーションは必須です。', 'danger')
        return redirect(url_for('inventory.inventory'))
    stock = db.execute("SELECT * FROM stocks WHERE id=%s FOR UPDATE", [stock_id]).fetchone()
    if not stock or int(stock['quantity'] or 0) <= 0:
        flash('移動元在庫が見つかりません。', 'danger')
        return redirect(url_for('inventory.inventory'))
    if qty > int(stock['quantity'] or 0):
        flash('移動数量が在庫数を超えています。', 'danger')
        return redirect(url_for('inventory.inventory'))
    from_location = (stock.get('location_code') or '').strip()
    if from_location == to_location:
        flash('同じロケーションには移動できません。', 'warning')
        return redirect(url_for('inventory.inventory'))
    before_from = int(stock['quantity'] or 0)
    if qty == before_from:
        db.execute("UPDATE stocks SET location_code=%s WHERE id=%s", [to_location, stock_id])
        to_stock_id = stock_id
    else:
        db.execute("UPDATE stocks SET quantity=quantity-%s WHERE id=%s", [qty, stock_id])
        target = db.execute("SELECT * FROM stocks WHERE jan=%s AND COALESCE(expiry_date,'')=%s AND COALESCE(lot_no,'')=%s AND COALESCE(location_code,'')=%s", [stock['jan'], stock.get('expiry_date') or '', stock.get('lot_no') or '', to_location]).fetchone()
        if target:
            db.execute("UPDATE stocks SET quantity=quantity+%s WHERE id=%s", [qty, target['id']])
            to_stock_id = target['id']
        else:
            db.execute("""
                INSERT INTO stocks (product_id,jan,product_name,supplier_cd,supplier_name,product_cd,unit_qty,quantity,expiry_date,lot_no,location_code)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, [stock['product_id'], stock['jan'], stock['product_name'], stock['supplier_cd'], stock['supplier_name'], stock['product_cd'], stock['unit_qty'], qty, stock.get('expiry_date') or '', stock.get('lot_no') or '', to_location])
            to_stock_id = db.execute("SELECT CURRVAL(pg_get_serial_sequence('stocks','id')) AS id").fetchone()['id']
    db.execute("INSERT INTO stock_movements (jan,product_name,move_type,quantity,before_qty,after_qty,note,source_file,move_date,expiry_date) VALUES (%s,%s,'transfer_out',%s,%s,%s,%s,%s,%s,%s)", [stock['jan'], stock['product_name'], qty, before_from, before_from-qty, f'{from_location} -> {to_location} {note}'.strip(), 'transfer', str(date.today()), stock.get('expiry_date') or ''])
    db.execute("INSERT INTO stock_movements (jan,product_name,move_type,quantity,before_qty,after_qty,note,source_file,move_date,expiry_date) VALUES (%s,%s,'transfer_in',%s,%s,%s,%s,%s,%s,%s)", [stock['jan'], stock['product_name'], qty, 0, qty, f'{from_location} -> {to_location} {note}'.strip(), 'transfer', str(date.today()), stock.get('expiry_date') or ''])
    db.execute("INSERT INTO stock_transfers (jan,from_stock_id,to_stock_id,from_location,to_location,quantity,lot_no,expiry_date,note,transfer_date,created_by) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", [stock['jan'], stock_id, to_stock_id, from_location, to_location, qty, stock.get('lot_no') or '', stock.get('expiry_date') or '', note, str(date.today()), session.get('user','')])
    db.commit()
    flash(f'ロケーション移動を登録しました。 {from_location or "未設定"} → {to_location} / {qty}個', 'success')
    return redirect(url_for('inventory.inventory'))


@bp.route('/inventory/picking')
@permission_required('inventory')
def inventory_picking():
    db = get_db()
    q = request.args.get('q','').strip().lower()
    days = _to_int(request.args.get('days', '7'), 7)
    plan = _build_picking_plan(db, days, q)
    return render_template('picking.html', rows=plan, q=q, days=days)

@bp.route('/inventory/picking/export_csv')
@permission_required('inventory')
def inventory_picking_export_csv():
    db = get_db()
    q = request.args.get('q','').strip().lower()
    days = _to_int(request.args.get('days', '7'), 7)
    rows = _build_picking_plan(db, days, q)
    sio = io.StringIO()
    writer = csv.writer(sio)
    writer.writerow(['仕入先CD','仕入先名','商品CD','JAN','商品名','ロケーション','賞味期限','ロット','在庫','必要数','ピック数','残必要数'])
    for r in rows:
        writer.writerow([r['supplier_cd'], r['supplier_name'], r['product_cd'], r['jan'], r['product_name'], r['location_code'], r['expiry_date'], r['lot_no'], r['quantity'], r['need_qty'], r['pick_qty'], r['remaining_after']])
    data = sio.getvalue().encode('utf-8-sig')
    return Response(data, mimetype='text/csv; charset=utf-8', headers={'Content-Disposition': f'attachment; filename=picking_plan_{date.today()}.csv'})

@bp.route('/inventory/picking/export_excel')
@permission_required('inventory')
def inventory_picking_export_excel():
    db = get_db()
    q = request.args.get('q','').strip().lower()
    days = _to_int(request.args.get('days', '7'), 7)
    rows = _build_picking_plan(db, days, q)
    excel = _excel_bytes_from_rows('Picking', ['仕入先CD','仕入先名','商品CD','JAN','商品名','ロケーション','賞味期限','ロット','在庫','必要数','ピック数','残必要数'], [[r['supplier_cd'], r['supplier_name'], r['product_cd'], r['jan'], r['product_name'], r['location_code'], r['expiry_date'], r['lot_no'], r['quantity'], r['need_qty'], r['pick_qty'], r['remaining_after']] for r in rows])
    return Response(excel, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers={'Content-Disposition': f'attachment; filename=picking_plan_{date.today()}.xlsx'})

@bp.route('/inventory/picking/print')
@permission_required('inventory')
def inventory_picking_print():
    db = get_db()
    q = request.args.get('q','').strip().lower()
    days = _to_int(request.args.get('days', '7'), 7)
    rows = _build_picking_plan(db, days, q)
    return render_template('picking_print.html', rows=rows, q=q, days=days, today=str(date.today()))


@bp.route('/inventory/replenishment')
@permission_required('inventory')
def inventory_replenishment():
    db = get_db()
    q = request.args.get('q','').strip().lower()
    rows = _build_replenishment_rows(db, q)
    tasks = db.execute("""
        SELECT *
        FROM replenishment_history
        WHERE NULLIF(task_date, '')::date >= CURRENT_DATE - INTERVAL '14 days'
        ORDER BY
            CASE status WHEN 'planned' THEN 0 ELSE 1 END,
            NULLIF(task_date, '')::date DESC,
            id DESC
    """).fetchall()
    return render_template('replenishment.html', rows=rows, q=q, tasks=tasks)

@bp.route('/inventory/replenishment/create', methods=['POST'])
@permission_required('inventory')
def inventory_replenishment_create():
    db = get_db()
    jan = (request.form.get('jan') or '').strip()
    qty = _to_int(request.form.get('qty'))
    shelf_location = (request.form.get('shelf_location') or '').strip()
    from_location = (request.form.get('from_location') or '').strip()
    note = (request.form.get('note') or '').strip()
    product = db.execute("SELECT product_name FROM products WHERE jan=%s", [jan]).fetchone()
    if not product or qty <= 0:
        flash('補充指示の内容が不正です。', 'danger')
        return redirect(url_for('inventory.inventory_replenishment', q=jan))
    db.execute("INSERT INTO replenishment_history (jan, product_name, shelf_location, from_location, planned_qty, task_date, status, note, created_by) VALUES (%s,%s,%s,%s,%s,%s,'planned',%s,%s)", [jan, product['product_name'], shelf_location, from_location, qty, str(date.today()), note, session.get('user','')])
    db.commit()
    flash(f"{product['product_name']} の補充指示を登録しました。", 'success')
    return redirect(url_for('inventory.inventory_replenishment', q=jan))

@bp.route('/inventory/replenishment/complete', methods=['POST'])
@permission_required('inventory')
def inventory_replenishment_complete():
    db = get_db()
    task_id = _to_int(request.form.get('task_id'))
    completed_qty = _to_int(request.form.get('completed_qty'))
    note = (request.form.get('note') or '').strip()
    task = db.execute("SELECT * FROM replenishment_history WHERE id=%s", [task_id]).fetchone()
    if not task or completed_qty < 0:
        flash('補充完了登録に失敗しました。', 'danger')
        return redirect(url_for('inventory.inventory_replenishment'))
    status = 'done' if completed_qty >= int(task.get('planned_qty') or 0) else 'partial'
    db.execute("UPDATE replenishment_history SET completed_qty=%s, completed_at=%s, status=%s, note=COALESCE(note,'') || %s, completed_by=%s WHERE id=%s", [completed_qty, str(date.today()), status, (' / ' + note) if note else '', session.get('user',''), task_id])
    db.commit()
    flash('補充実績を登録しました。', 'success')
    return redirect(url_for('inventory.inventory_replenishment', q=task['jan']))

@bp.route('/inventory/replenishment/export_csv')
@permission_required('inventory')
def inventory_replenishment_export_csv():
    db = get_db()
    q = request.args.get('q','').strip().lower()
    rows = _build_replenishment_rows(db, q)
    sio = io.StringIO()
    writer = csv.writer(sio)
    writer.writerow(['仕入先CD','仕入先名','商品CD','JAN','商品名','棚ロケーション','棚在庫','補充点','棚目標','引当可能在庫','推奨補充数','最古期限','状態'])
    for r in rows:
        writer.writerow([r['supplier_cd'], r['supplier_name'], r['product_cd'], r['jan'], r['product_name'], r['shelf_location'], r['shelf_qty'], r['shelf_trigger'], r['shelf_target'], r['reserve_qty'], r['suggested_replenish_qty'], r['reserve_oldest_expiry'] or '', r['status']])
    data = sio.getvalue().encode('utf-8-sig')
    return Response(data, mimetype='text/csv; charset=utf-8', headers={'Content-Disposition': f'attachment; filename=replenishment_{date.today()}.csv'})

@bp.route('/inventory/replenishment/export_excel')
@permission_required('inventory')
def inventory_replenishment_export_excel():
    db = get_db()
    q = request.args.get('q','').strip().lower()
    rows = _build_replenishment_rows(db, q)
    excel = _excel_bytes_from_rows('Replenishment', ['仕入先CD','仕入先名','商品CD','JAN','商品名','棚ロケーション','棚在庫','補充点','棚目標','引当可能在庫','推奨補充数','最古期限','状態'], [[r['supplier_cd'], r['supplier_name'], r['product_cd'], r['jan'], r['product_name'], r['shelf_location'], r['shelf_qty'], r['shelf_trigger'], r['shelf_target'], r['reserve_qty'], r['suggested_replenish_qty'], r['reserve_oldest_expiry'] or '', r['status']] for r in rows])
    return Response(excel, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers={'Content-Disposition': f'attachment; filename=replenishment_{date.today()}.xlsx'})
