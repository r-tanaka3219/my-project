"""発注管理 Blueprint"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from datetime import date, timedelta
import logging
from db import get_db
from auth_helpers import login_required, admin_required, permission_required
from helpers import _to_int, _safe_date, _record_receipt
from auto_check import run_order_check, get_pending_orders

logger = logging.getLogger('inventory.orders')
bp = Blueprint('orders', __name__)

@bp.route('/orders')
@permission_required('orders')
def orders():
    db = get_db()
    q = request.args.get('q','').strip()
    sql = """
        SELECT p.*, COALESCE(s.stock_qty, 0) AS stock_qty
        FROM products p
        LEFT JOIN (SELECT jan, SUM(quantity) AS stock_qty FROM stocks GROUP BY jan) s ON s.jan=p.jan
        WHERE p.is_active=1
    """
    params = []
    if q:
        sql += " AND (p.jan ILIKE %s OR p.product_cd ILIKE %s OR p.product_name ILIKE %s OR p.supplier_cd ILIKE %s OR p.supplier_name ILIKE %s)"
        params += [f'%{q}%'] * 5
    sql += " ORDER BY CAST(NULLIF(regexp_replace(p.supplier_cd,'[^0-9]','','g'),'') AS BIGINT) NULLS LAST, CAST(NULLIF(regexp_replace(p.product_cd,'[^0-9]','','g'),'') AS BIGINT) NULLS LAST"
    rows = db.execute(sql, params).fetchall()
    low_stock     = [r for r in rows if r['reorder_point'] > 0 and r['stock_qty'] <= r['reorder_point'] and not r['ordered_at']]
    ordered_list  = [r for r in rows if r['ordered_at']]
    all_products  = [r for r in rows if not r['ordered_at']]
    pending = get_pending_orders(db)
    # 発注済み商品の実際の発注数をorder_historyから取得
    today_str = str(date.today())
    actual_order_qty = {}
    if ordered_list:
        jans = [r['jan'] for r in ordered_list]
        placeholders = ','.join(['%s'] * len(jans))
        hist_rows = db.execute(f"""
            SELECT jan, SUM(order_qty) as total_qty
            FROM order_history
            WHERE jan IN ({placeholders})
            AND order_date = %s
            GROUP BY jan
        """, jans + [today_str]).fetchall()
        for h in hist_rows:
            actual_order_qty[h['jan']] = h['total_qty']
    return render_template('orders.html', low_stock=low_stock, ordered_list=ordered_list,
                           all_products=all_products, pending=pending, today=date.today(), q=q,
                           actual_order_qty=actual_order_qty)

@bp.route('/orders/send', methods=['POST'])
@login_required
def order_send():
    from mail_service import queue_order, flush_order_mail
    import math
    db = get_db()
    today = str(date.today())
    sent = 0
    hist_ids = []
    for jan in request.form.getlist('jan'):
        p = db.execute("SELECT * FROM products WHERE jan=%s AND is_active=1",[jan]).fetchone()
        if not p: continue
        # ケース単位（unit_qty）に切り上げ
        unit_qty = int(p['unit_qty'] or 1)
        order_qty = int(p['order_qty'] or unit_qty)
        if unit_qty > 1 and order_qty % unit_qty != 0:
            order_qty = math.ceil(order_qty / unit_qty) * unit_qty
        db.execute("""
            INSERT INTO order_history
            (supplier_cd,supplier_name,supplier_email,jan,product_cd,product_name,
             order_qty,trigger_type,order_date,expected_receipt_date,mail_sent,mail_result)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [p['supplier_cd'],p['supplier_name'],p['supplier_email'],
              p['jan'],p['product_cd'],p['product_name'],order_qty,'manual',today,str(date.today() + timedelta(days=int(p.get('lead_time_days') or 3))),0,''])
        db.commit()
        queue_order(dict(p), order_qty, 'manual')
        hist = db.execute("SELECT id FROM order_history WHERE jan=%s AND order_date=%s ORDER BY id DESC LIMIT 1",
                          [jan,today]).fetchone()
        if hist:
            hist_ids.append(hist['id'])
        sent += 1
    # 全件まとめて1通送信
    ok, msg = flush_order_mail()
    for hid in hist_ids:
        db.execute("UPDATE order_history SET mail_sent=%s,mail_result=%s WHERE id=%s",
                   [1 if ok else 0, msg, hid])
    # order_pending から削除・発注済みフラグをセット
    for jan in request.form.getlist('jan'):
        db.execute("DELETE FROM order_pending WHERE jan=%s", [jan])
        db.execute("UPDATE products SET ordered_at=%s WHERE jan=%s", [today, jan])
    db.commit()
    flash(f'{sent}件の発注をまとめてメール送信しました。', 'success')
    return redirect(url_for('orders.orders'))

@bp.route('/orders/auto_check', methods=['POST'])
@login_required
def orders_auto_check():
    results = run_order_check()
    flash(f'自動チェック完了: {len(results)}件処理。', 'success' if results else 'success')
    return redirect(url_for('orders.orders'))


# ─── 発注履歴 ────────────────────────────────────────────────────
@bp.route('/order_history')
@permission_required('order_history')
def order_history():
    db = get_db()
    q = request.args.get('q','').strip()
    page = max(1, int(request.args.get('page', 1)))
    per_page = 100
    all_rows = db.execute("SELECT * FROM order_history ORDER BY created_at DESC").fetchall()
    if q:
        all_rows = [r for r in all_rows if q.lower() in (r['jan'] or '').lower()
                    or q.lower() in (r['product_cd'] or '').lower()
                    or q.lower() in (r['product_name'] or '').lower()
                    or q.lower() in (r['supplier_cd'] or '').lower()
                    or q.lower() in (r['supplier_name'] or '').lower()]
    total = len(all_rows)
    rows = all_rows[(page-1)*per_page:page*per_page]
    pages = (total + per_page - 1) // per_page
    return render_template('order_history.html', orders=rows, q=q, page=page, pages=pages, total=total)




# ─── 混載ペンディング管理 ──────────────────────────────────────
@bp.route('/orders/pending_force', methods=['POST'])
def pending_force():
    """指定したペンディング発注を強制送信"""
    db = get_db()
    pending_ids = request.form.getlist('pending_id')
    today = str(date.today())
    sent = 0
    from auto_check import _do_order
    for pid in pending_ids:
        item = db.execute("SELECT * FROM order_pending WHERE id=%s",[int(pid)]).fetchone()
        if not item or item['status'] != 'pending':
            continue
        product = db.execute("SELECT * FROM products WHERE jan=%s",[item['jan']]).fetchone()
        if not product:
            continue
        ok, msg = _do_order(db, product, item['order_qty'], 'forced', today)
        db.execute("UPDATE order_pending SET status='sent' WHERE id=%s",[int(pid)])
        db.commit()
        sent += 1
    flash(f'{sent}件の保留発注を強制送信しました。', 'success' if sent else 'warning')
    return redirect(url_for('orders.orders'))

@bp.route('/orders/pending_force_single', methods=['POST'])
def pending_force_single():
    """個別ペンディング発注を強制送信"""
    db = get_db()
    pending_id = request.form.get('pending_id')
    if not pending_id:
        flash('発注IDが指定されていません', 'error')
        return redirect(url_for('orders.orders'))
    today = str(date.today())
    from auto_check import _do_order
    item = db.execute("SELECT * FROM order_pending WHERE id=%s", [int(pending_id)]).fetchone()
    if not item or item['status'] != 'pending':
        flash('発注データが見つかりません', 'error')
        return redirect(url_for('orders.orders'))
    product = db.execute("SELECT * FROM products WHERE jan=%s", [item['jan']]).fetchone()
    if not product:
        flash('商品が見つかりません', 'error')
        return redirect(url_for('orders.orders'))
    ok, msg = _do_order(db, product, item['order_qty'], 'forced', today)
    db.execute("UPDATE order_pending SET status='sent' WHERE id=%s", [int(pending_id)])
    db.commit()
    if ok:
        flash(f'{item["product_name"]} を個別強制発注しました', 'success')
    else:
        flash(f'発注エラー: {msg}', 'danger')
    return redirect(url_for('orders.orders'))

@bp.route('/orders/pending_cancel_single', methods=['POST'])
def pending_cancel_single():
    """個別ペンディング発注をキャンセル"""
    db = get_db()
    pending_id = request.form.get('pending_id')
    if not pending_id:
        flash('発注IDが指定されていません', 'error')
        return redirect(url_for('orders.orders'))
    db.execute("UPDATE order_pending SET status='cancelled' WHERE id=%s", [int(pending_id)])
    db.commit()
    flash('1件のペンディング発注をキャンセルしました', 'warning')
    return redirect(url_for('orders.orders'))



@bp.route('/orders/pending_force_group_edit')
@login_required
def pending_force_group_edit():
    db = get_db()
    group_name = request.args.get('mixed_group', '').strip()
    if not group_name:
        flash('グループ名が指定されていません', 'error')
        return redirect(url_for('orders.orders'))

    sql = """
        SELECT op.*, p.unit_qty, p.mixed_lot_cases, p.mixed_lot_mode, p.order_qty AS product_order_qty,
               p.product_name AS master_product_name
        FROM order_pending op
        JOIN products p ON op.jan = p.jan
        WHERE op.mixed_group=%s AND op.status='pending'
        ORDER BY op.order_cases DESC, op.pending_since ASC
    """
    items = db.execute(sql, [group_name]).fetchall()
    if not items:
        flash('対象の保留発注が見つかりません', 'error')
        return redirect(url_for('orders.orders'))

    lot_cases = int(items[0]['mixed_lot_cases'] or 5)
    total_cases = sum(int(it['order_cases'] or 0) for it in items)
    shortage = max(0, lot_cases - total_cases)

    stocks_map = {}
    for it in items:
        st = db.execute("SELECT COALESCE(SUM(quantity),0) AS s FROM stocks WHERE jan=%s", [it['jan']]).fetchone()['s']
        stocks_map[it['jan']] = int(st or 0)

    items_sorted = sorted(items, key=lambda x: stocks_map.get(x['jan'], 0))
    n = len(items_sorted)
    extra_per = shortage // n if n > 0 else 0
    remainder = shortage % n if n > 0 else 0
    auto_extra = {}
    for idx, it in enumerate(items_sorted):
        auto_extra[it['jan']] = extra_per + (1 if idx < remainder else 0)

    view_items = []
    for it in items:
        unit_qty = int(it['unit_qty'] or 1)
        base_cases = int(it['order_cases'] or 0)
        base_order_qty = int(it['order_qty'] or it.get('product_order_qty') or unit_qty)
        extra_cases = int(auto_extra.get(it['jan'], 0))
        final_order_qty = (base_cases + extra_cases) * base_order_qty
        view_items.append({
            'id': it['id'],
            'jan': it['jan'],
            'product_cd': it['product_cd'],
            'product_name': it['product_name'] or it.get('master_product_name') or '',
            'supplier_name': it['supplier_name'],
            'pending_since': it['pending_since'],
            'force_send_date': it['force_send_date'],
            'stock_qty': stocks_map.get(it['jan'], 0),
            'unit_qty': unit_qty,
            'base_cases': base_cases,
            'base_order_qty': base_order_qty,
            'auto_extra_cases': extra_cases,
            'suggested_total_cases': base_cases + extra_cases,
            'suggested_order_qty': final_order_qty,
        })

    return render_template(
        'pending_force_group_edit.html',
        group_name=group_name,
        items=view_items,
        lot_cases=lot_cases,
        total_cases=total_cases,
        shortage=shortage,
        suggested_total_cases=sum(v['suggested_total_cases'] for v in view_items),
        today=date.today(),
    )


@bp.route('/orders/pending_force_group_manual', methods=['POST'])
@login_required
def pending_force_group_manual():
    from auto_check import _do_order
    from mail_service import flush_order_mail

    db = get_db()
    group_name = request.form.get('mixed_group', '').strip()
    if not group_name:
        flash('グループ名が指定されていません', 'error')
        return redirect(url_for('orders.orders'))

    sql = """
        SELECT op.*, p.unit_qty, p.mixed_lot_cases, p.mixed_lot_mode, p.order_qty AS product_order_qty
        FROM order_pending op
        JOIN products p ON op.jan = p.jan
        WHERE op.mixed_group=%s AND op.status='pending'
        ORDER BY op.order_cases DESC, op.pending_since ASC
    """
    items = db.execute(sql, [group_name]).fetchall()
    if not items:
        flash('対象の保留発注が見つかりません', 'error')
        return redirect(url_for('orders.orders'))

    lot_cases = int(items[0]['mixed_lot_cases'] or 5)
    item_map = {str(it['id']): it for it in items}
    final_total_cases = 0
    manual_lines = []

    for pid, it in item_map.items():
        base_cases = int(it['order_cases'] or 0)
        base_order_qty = int(it['order_qty'] or it.get('product_order_qty') or 1)
        raw_extra = (request.form.get(f'extra_cases_{pid}', '') or '0').strip()
        try:
            extra_cases = int(raw_extra)
        except ValueError:
            extra_cases = 0
        if extra_cases < 0:
            extra_cases = 0
        total_cases = base_cases + extra_cases
        final_qty = total_cases * base_order_qty
        final_total_cases += total_cases
        manual_lines.append({
            'item': it,
            'extra_cases': extra_cases,
            'total_cases': total_cases,
            'final_qty': final_qty,
        })

    if final_total_cases < lot_cases:
        flash(f'手動調整後の合計ロットが不足しています。合計 {final_total_cases} ケース / 必要 {lot_cases} ケースです。', 'error')
        return redirect(url_for('pending_force_group_edit', mixed_group=group_name))

    today = str(date.today())
    sent = 0
    for line in manual_lines:
        it = line['item']
        product = db.execute('SELECT * FROM products WHERE jan=%s', [it['jan']]).fetchone()
        if not product:
            continue
        ok, msg = _do_order(db, product, line['final_qty'], 'forced_manual', today)
        db.execute("UPDATE order_pending SET status='sent' WHERE id=%s", [it['id']])
        db.commit()
        sent += 1

    ok, msg = flush_order_mail()
    for line in manual_lines:
        it = line['item']
        db.execute("""
            UPDATE order_history SET mail_sent=%s, mail_result=%s
            WHERE jan=%s AND order_date=%s AND trigger_type='forced_manual'
        """, [1 if ok else 0, msg, it['jan'], today])
    db.commit()

    if ok:
        flash(f'グループ{group_name}: 手動調整後の {sent} 件を強制発注しました（合計 {final_total_cases} ケース）。', 'success')
    else:
        flash(f'グループ{group_name}: 発注登録済みですがメール送信失敗: {msg}', 'warning')
    return redirect(url_for('orders.orders'))


@bp.route('/orders/pending_force_group', methods=['POST'])
def pending_force_group():
    from auto_check import _do_order
    db = get_db()
    group_name = request.form.get('mixed_group', '').strip()
    if not group_name:
        flash('グループ名が指定されていません', 'error')
        return redirect(url_for('orders.orders'))
    today = str(date.today())
    sql = "SELECT op.*, p.unit_qty, p.mixed_lot_cases, p.order_qty FROM order_pending op JOIN products p ON op.jan=p.jan WHERE op.mixed_group=%s AND op.status='pending' ORDER BY op.order_cases DESC"
    items = db.execute(sql, [group_name]).fetchall()
    if not items:
        flash('対象の保留発注が見つかりません', 'error')
        return redirect(url_for('orders.orders'))
    lot_cases = int(items[0]['mixed_lot_cases'] or 5)
    total_cases = sum(it['order_cases'] for it in items)
    n = len(items)
    shortage = max(0, lot_cases - total_cases)
    # 在庫数が少ない順にソート（在庫が少ない方に余りを多く配分）
    stocks_map = {}
    for it in items:
        st = db.execute(
            "SELECT COALESCE(SUM(quantity),0) AS s FROM stocks WHERE jan=%s", [it['jan']]
        ).fetchone()['s']
        stocks_map[it['jan']] = int(st)
    items_sorted = sorted(items, key=lambda x: stocks_map.get(x['jan'], 0))
    extra_per = shortage // n if n > 0 else 0
    remainder = shortage % n if n > 0 else 0
    adjusted = {}
    for idx, it in enumerate(items_sorted):
        extra = extra_per + (1 if idx < remainder else 0)
        adjusted[it['jan']] = (it['order_cases'] + extra) * int(it['order_qty'] or 1)
    sent = 0
    for it in items:
        product = db.execute("SELECT * FROM products WHERE jan=%s", [it['jan']]).fetchone()
        if not product:
            continue
        order_qty = adjusted.get(it['jan'], it['order_qty'])
        ok, msg = _do_order(db, product, order_qty, 'forced', today)
        db.execute("UPDATE order_pending SET status='sent' WHERE id=%s", [it['id']])
        db.commit()
        sent += 1
    # メール送信
    from mail_service import flush_order_mail
    ok, msg = flush_order_mail()
    for it in items:
        db.execute("""
            UPDATE order_history SET mail_sent=%s, mail_result=%s
            WHERE jan=%s AND order_date=%s AND trigger_type='forced'
        """, [1 if ok else 0, msg, it['jan'], today])
    db.commit()
    if ok:
        flash(f'グループ{group_name}: {sent}件を混載強制発注しました（合計{max(total_cases, lot_cases)}ケース）メール送信済み', 'success')
    else:
        flash(f'グループ{group_name}: 発注登録済みですがメール送信失敗: {msg}', 'warning')
    return redirect(url_for('orders.orders'))

@bp.route('/orders/pending_cancel', methods=['POST'])
def pending_cancel():
    """ペンディング発注をキャンセル"""
    db = get_db()
    pending_ids = request.form.getlist('pending_id')
    for pid in pending_ids:
        db.execute("UPDATE order_pending SET status='cancelled' WHERE id=%s",[int(pid)])
    db.commit()
    flash(f'{len(pending_ids)}件の保留発注をキャンセルしました。', 'warning')
    return redirect(url_for('orders.orders'))


# ─── フォルダパス候補API ─────────────────────────────────────────
@bp.route('/api/folder_candidates')
@admin_required
def folder_candidates():
    """登録済みCSV設定のフォルダパス一覧を返す（入力補助用）"""
    db = get_db()
    rows = db.execute(
        "SELECT DISTINCT folder_path FROM csv_import_settings WHERE folder_path!='' ORDER BY folder_path"
    ).fetchall()
    paths = [r['folder_path'] for r in rows]
    return jsonify({'paths': paths})

@bp.route('/orders/backorders')
@permission_required('orders')
def orders_backorders():
    db = get_db()
    q = request.args.get('q','').strip()
    sql = """
        SELECT oh.*, p.unit_qty, p.location_code,
               COALESCE(orx.received_qty, 0) AS received_qty,
               (oh.order_qty - COALESCE(orx.received_qty, 0)) AS outstanding_qty,
               (CURRENT_DATE - oh.order_date::date) AS order_age,
               COALESCE(orx.last_receipt_date, '') AS last_receipt_date
        FROM order_history oh
        LEFT JOIN products p ON oh.jan=p.jan
        LEFT JOIN (
            SELECT order_history_id,
                   SUM(received_qty) AS received_qty,
                   MAX(receipt_date) AS last_receipt_date
            FROM order_receipts
            GROUP BY order_history_id
        ) orx ON orx.order_history_id = oh.id
        WHERE NULLIF(oh.order_date, '')::date >= CURRENT_DATE - INTERVAL '180 days'
          AND oh.closed_at IS NULL
    """
    params = []
    if q:
        sql += " AND (oh.jan ILIKE %s OR oh.product_cd ILIKE %s OR oh.product_name ILIKE %s OR oh.supplier_cd ILIKE %s OR oh.supplier_name ILIKE %s)"
        params += [f'%{q}%'] * 5
    sql += " ORDER BY NULLIF(oh.order_date, '')::date DESC, oh.id DESC"
    rows = db.execute(sql, params).fetchall()
    today = date.today()
    enriched = []
    for r in rows:
        r = dict(r)
        eta = _safe_date(r.get('expected_receipt_date')) if r.get('expected_receipt_date') else None
        if (r.get('outstanding_qty') or 0) <= 0:
            r['delay_level'] = '完了'
            r['delay_days'] = 0
        elif eta and eta < today:
            r['delay_level'] = '遅延'
            r['delay_days'] = (today - eta).days
        elif eta and (eta - today).days <= 2:
            r['delay_level'] = '入荷接近'
            r['delay_days'] = (eta - today).days
        elif (r.get('order_age') or 0) >= 7:
            r['delay_level'] = '長期未着'
            r['delay_days'] = int(r['order_age'] or 0)
        else:
            r['delay_level'] = '正常'
            r['delay_days'] = 0
        enriched.append(r)
    rows = enriched
    summary = {
        'open_count': sum(1 for r in rows if (r['outstanding_qty'] or 0) > 0),
        'open_qty': sum(r['outstanding_qty'] or 0 for r in rows if (r['outstanding_qty'] or 0) > 0),
        'overdue_count': sum(1 for r in rows if r['delay_level'] == '遅延'),
        'warning_count': sum(1 for r in rows if r['delay_level'] in ('遅延','入荷接近','長期未着')),
    }
    return render_template('backorders.html', rows=rows, summary=summary, q=q, today=str(today))

@bp.route('/orders/backorders/receive', methods=['POST'])
@permission_required('orders')
def orders_backorders_receive():
    db = get_db()
    order_id = _to_int(request.form.get('order_history_id'))
    qty = _to_int(request.form.get('receive_qty'))
    expiry = (request.form.get('expiry_date') or '').strip()
    lot_no = (request.form.get('lot_no') or '').strip()
    location_code = (request.form.get('location_code') or '').strip()
    note = (request.form.get('note') or '').strip()
    if order_id <= 0 or qty <= 0:
        flash('入荷数が不正です。', 'danger')
        return redirect(url_for('orders.orders_backorders'))
    order = db.execute("SELECT * FROM order_history WHERE id=%s", [order_id]).fetchone()
    product = db.execute("SELECT * FROM products WHERE jan=%s", [order['jan']]).fetchone() if order else None
    if not order or not product:
        flash('発注情報が見つかりません。', 'danger')
        return redirect(url_for('orders.orders_backorders'))
    received = db.execute("SELECT COALESCE(SUM(received_qty),0) AS qty FROM order_receipts WHERE order_history_id=%s", [order_id]).fetchone()['qty']
    outstanding = max(int(order['order_qty'] or 0) - int(received or 0), 0)
    if qty > outstanding:
        flash(f'残数量 {outstanding} を超えて受領できません。', 'danger')
        return redirect(url_for('orders.orders_backorders'))
    if not expiry:
        flash('部分入荷でも賞味期限は必須です。', 'danger')
        return redirect(url_for('orders.orders_backorders'))
    _record_receipt(db, product, qty, expiry, lot_no, location_code or product.get('location_code',''), 'backorder', f'backorder:{order_id} {note}')
    db.execute("INSERT INTO order_receipts (order_history_id, jan, received_qty, receipt_date, note) VALUES (%s,%s,%s,%s,%s)", [order_id, order['jan'], qty, str(date.today()), note])
    remaining = outstanding - qty
    if remaining <= 0:
        # 同一JANで他に未完了の発注残がなければ ordered_at をクリア
        other_open = db.execute("""
            SELECT COUNT(*) AS cnt FROM order_history
            WHERE jan=%s AND id<>%s AND closed_at IS NULL
              AND (order_qty - COALESCE((SELECT SUM(received_qty) FROM order_receipts r WHERE r.order_history_id=order_history.id),0)) > 0
        """, [order['jan'], order_id]).fetchone()['cnt']
        if other_open == 0:
            db.execute("UPDATE products SET ordered_at='' WHERE jan=%s", [order['jan']])
    db.commit()
    flash(f'{order["product_name"]} を {qty} 個受領登録しました。残 {remaining} 個。', 'success')
    return redirect(url_for('orders.orders_backorders'))

@bp.route('/orders/backorders/close', methods=['POST'])
@permission_required('orders')
def orders_backorders_close():
    db = get_db()
    order_id = _to_int(request.form.get('order_history_id'))
    order = db.execute("SELECT * FROM order_history WHERE id=%s", [order_id]).fetchone()
    if not order:
        flash('発注情報が見つかりません。', 'danger')
        return redirect(url_for('orders.orders_backorders'))
    received = db.execute("SELECT COALESCE(SUM(received_qty),0) AS qty FROM order_receipts WHERE order_history_id=%s", [order_id]).fetchone()['qty']
    outstanding = max(int(order['order_qty'] or 0) - int(received or 0), 0)
    # order_history に closed_at をセット（クローズフラグ）
    db.execute("UPDATE order_history SET closed_at=%s WHERE id=%s", [str(date.today()), order_id])
    # 同一JANで他に未完了の発注残がなければ ordered_at をクリア
    other_open = db.execute("""
        SELECT COUNT(*) AS cnt FROM order_history
        WHERE jan=%s AND id<>%s AND closed_at IS NULL
          AND (order_qty - COALESCE((SELECT SUM(received_qty) FROM order_receipts r WHERE r.order_history_id=order_history.id),0)) > 0
    """, [order['jan'], order_id]).fetchone()['cnt']
    if other_open == 0:
        db.execute("UPDATE products SET ordered_at='' WHERE jan=%s", [order['jan']])
    db.commit()
    flash(f'{order["product_name"]} の未納残 {outstanding} 個をクローズしました。', 'warning')
    return redirect(url_for('orders.orders_backorders'))
