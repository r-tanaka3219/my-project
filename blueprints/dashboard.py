"""ダッシュボード Blueprint"""
from flask import Blueprint, render_template, request
from datetime import date
import logging
from db import get_db
from auth_helpers import permission_required

logger = logging.getLogger('inventory.dashboard')
bp = Blueprint('dashboard', __name__)


@bp.route('/')
@permission_required('dashboard')
def dashboard():
    today = date.today()
    db = get_db()

    total_products = db.execute(
        "SELECT COUNT(*) AS _cnt FROM products WHERE is_active=1"
    ).fetchone()['_cnt']

    # 発注点以下の商品（件数 + 一覧）
    low_stock_rows = db.execute("""
        SELECT p.jan, p.product_cd, p.product_name, p.unit_qty,
               p.supplier_cd, p.supplier_name,
               p.reorder_point, p.order_qty,
               COALESCE(SUM(s.quantity),0) AS total_qty
        FROM products p
        LEFT JOIN stocks s ON s.jan = p.jan
        WHERE p.is_active = 1 AND (p.ordered_at IS NULL OR p.ordered_at = '')
          AND p.reorder_point > 0
        GROUP BY p.id, p.jan, p.product_cd, p.product_name, p.unit_qty,
                 p.supplier_cd, p.supplier_name, p.reorder_point, p.order_qty
        HAVING COALESCE(SUM(s.quantity),0) <= p.reorder_point
        ORDER BY CAST(NULLIF(regexp_replace(p.supplier_cd,'[^0-9]','','g'),'') AS BIGINT) ASC NULLS LAST,
                 CAST(NULLIF(regexp_replace(p.product_cd,'[^0-9]','','g'),'') AS BIGINT) ASC NULLS LAST
    """).fetchall()
    low_stock = len(low_stock_rows)

    # 期限アラート件数
    expiry_alert = db.execute("""
        SELECT COUNT(DISTINCT s.jan) AS _cnt
        FROM stocks s JOIN products p ON s.jan = p.jan
        WHERE s.quantity > 0 AND s.expiry_date != ''
          AND s.expiry_date <= CAST(CURRENT_DATE + p.expiry_alert_days * INTERVAL '1 day' AS TEXT)
    """).fetchone()['_cnt']

    dash_page = max(1, int(request.args.get('dash_page', 1)))
    dash_per = 10
    all_orders = db.execute(
        "SELECT * FROM order_history WHERE mail_sent=1 ORDER BY created_at DESC"
    ).fetchall()
    recent_orders = all_orders[(dash_page-1)*dash_per:dash_page*dash_per]
    dash_pages = (len(all_orders) + dash_per - 1) // dash_per
    recent_alerts = db.execute(
        "SELECT * FROM alert_logs ORDER BY created_at DESC LIMIT 6"
    ).fetchall()

    return render_template('dashboard.html',
        total_products=total_products,
        low_stock=low_stock,
        low_stock_rows=low_stock_rows,
        expiry_alert=expiry_alert,
        recent_orders=recent_orders,
        dash_page=dash_page,
        dash_pages=dash_pages,
        recent_alerts=recent_alerts,
        today=today)
