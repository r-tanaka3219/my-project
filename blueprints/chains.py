"""チェーン・店舗管理 Blueprint"""
from flask import Blueprint, render_template, request, redirect, url_for, flash
import logging
from db import get_db
from auth_helpers import permission_required, admin_required

logger = logging.getLogger('inventory.chains')
bp = Blueprint('chains', __name__)


@bp.route('/chains')
@permission_required('chains')
def chains():
    db = get_db()
    chains = db.execute("SELECT * FROM chain_masters ORDER BY chain_cd").fetchall()
    stores = db.execute("SELECT * FROM store_masters ORDER BY store_cd").fetchall()
    supplier_settings = db.execute(
        "SELECT * FROM supplier_cd_settings ORDER BY chain_cd NULLS LAST, store_cd NULLS LAST, supplier_cd"
    ).fetchall()
    product_settings = db.execute(
        "SELECT * FROM product_cd_settings ORDER BY chain_cd NULLS LAST, store_cd NULLS LAST, product_cd NULLS LAST, jan NULLS LAST"
    ).fetchall()
    chain_list = [c['chain_cd'] for c in chains]
    store_list = [s['store_cd'] for s in stores]
    return render_template('chains.html', chains=chains, stores=stores,
                           supplier_settings=supplier_settings,
                           product_settings=product_settings,
                           chain_list=chain_list, store_list=store_list)

@bp.route('/chains/chain/add', methods=['POST'])
@admin_required
def chain_add():
    db = get_db()
    chain_cd       = request.form.get('chain_cd', '').strip()
    chain_name     = request.form.get('chain_name', '').strip()
    exclude_deduct = 1 if request.form.get('exclude_deduct') == '1' else 0
    if not chain_cd:
        flash('チェーンCDを入力してください', 'error')
        return redirect('/chains')
    db.execute(
        "INSERT INTO chain_masters (chain_cd,chain_name,exclude_deduct) VALUES (%s,%s,%s) ON CONFLICT (chain_cd) DO UPDATE SET chain_name=%s, exclude_deduct=%s",
        [chain_cd, chain_name, exclude_deduct, chain_name, exclude_deduct]
    )
    db.commit()
    flash(f'チェーンCD {chain_cd} を追加しました', 'success')
    return redirect('/chains')

@bp.route('/chains/chain/<int:chain_id>/update', methods=['POST'])
@admin_required
def chain_update(chain_id):
    db = get_db()
    chain_name     = request.form.get('chain_name', '').strip()
    exclude_deduct = 1 if request.form.get('exclude_deduct') == '1' else 0
    db.execute(
        "UPDATE chain_masters SET chain_name=%s, exclude_deduct=%s WHERE id=%s",
        [chain_name, exclude_deduct, chain_id]
    )
    db.commit()
    flash('更新しました', 'success')
    return redirect('/chains')

@bp.route('/chains/store/add', methods=['POST'])
@admin_required
def store_add():
    db = get_db()
    store_cd       = request.form.get('store_cd', '').strip()
    store_name     = request.form.get('store_name', '').strip()
    chain_cd       = request.form.get('chain_cd', '').strip()
    client_name    = request.form.get('client_name', '').strip()
    exclude_deduct = 1 if request.form.get('exclude_deduct') == '1' else 0
    if not store_cd:
        flash('店舗CDを入力してください', 'error')
        return redirect('/chains#store')
    db.execute(
        "INSERT INTO store_masters (store_cd,store_name,chain_cd,client_name,exclude_deduct) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (store_cd) DO UPDATE SET store_name=%s, chain_cd=%s, client_name=%s, exclude_deduct=%s",
        [store_cd, store_name, chain_cd, client_name, exclude_deduct,
         store_name, chain_cd, client_name, exclude_deduct]
    )
    db.commit()
    flash(f'店舗CD {store_cd} を追加しました', 'success')
    return redirect('/chains')

@bp.route('/chains/store/<int:store_id>/update', methods=['POST'])
@admin_required
def store_update(store_id):
    db = get_db()
    store_name     = request.form.get('store_name', '').strip()
    client_name    = request.form.get('client_name', '').strip()
    exclude_deduct = 1 if request.form.get('exclude_deduct') == '1' else 0
    db.execute(
        "UPDATE store_masters SET store_name=%s, client_name=%s, exclude_deduct=%s WHERE id=%s",
        [store_name, client_name, exclude_deduct, store_id]
    )
    db.commit()
    flash('更新しました', 'success')
    return redirect('/chains')


# ── 仕入先CD設定 ─────────────────────────────────────────────────────

@bp.route('/chains/supplier-setting/add', methods=['POST'])
@admin_required
def supplier_setting_add():
    db = get_db()
    supplier_cd    = request.form.get('supplier_cd', '').strip()
    chain_cd       = request.form.get('chain_cd', '').strip() or None
    store_cd       = request.form.get('store_cd', '').strip() or None
    exclude_deduct = 1 if request.form.get('exclude_deduct') == '1' else 0
    notes          = request.form.get('notes', '').strip()
    if not supplier_cd:
        flash('仕入先CDを入力してください', 'error')
        return redirect('/chains#supplier')
    db.execute(
        """INSERT INTO supplier_cd_settings (chain_cd, store_cd, supplier_cd, exclude_deduct, notes)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (chain_cd, store_cd, supplier_cd)
           DO UPDATE SET exclude_deduct=%s, notes=%s""",
        [chain_cd, store_cd, supplier_cd, exclude_deduct, notes,
         exclude_deduct, notes]
    )
    db.commit()
    flash(f'仕入先CD {supplier_cd} の設定を追加しました', 'success')
    return redirect('/chains#supplier')

@bp.route('/chains/supplier-setting/<int:setting_id>/update', methods=['POST'])
@admin_required
def supplier_setting_update(setting_id):
    db = get_db()
    exclude_deduct = 1 if request.form.get('exclude_deduct') == '1' else 0
    notes          = request.form.get('notes', '').strip()
    db.execute(
        "UPDATE supplier_cd_settings SET exclude_deduct=%s, notes=%s WHERE id=%s",
        [exclude_deduct, notes, setting_id]
    )
    db.commit()
    flash('仕入先CD設定を更新しました', 'success')
    return redirect('/chains#supplier')

@bp.route('/chains/supplier-setting/<int:setting_id>/delete', methods=['POST'])
@admin_required
def supplier_setting_delete(setting_id):
    db = get_db()
    db.execute("DELETE FROM supplier_cd_settings WHERE id=%s", [setting_id])
    db.commit()
    flash('削除しました', 'success')
    return redirect('/chains#supplier')


# ── 商品CD設定 ───────────────────────────────────────────────────────

@bp.route('/chains/product-setting/add', methods=['POST'])
@admin_required
def product_setting_add():
    db = get_db()
    product_cd     = request.form.get('product_cd', '').strip() or None
    jan            = request.form.get('jan', '').strip() or None
    chain_cd       = request.form.get('chain_cd', '').strip() or None
    store_cd       = request.form.get('store_cd', '').strip() or None
    exclude_deduct = 1 if request.form.get('exclude_deduct') == '1' else 0
    notes          = request.form.get('notes', '').strip()
    if not product_cd and not jan:
        flash('商品CDまたはJANコードを入力してください', 'error')
        return redirect('/chains#product')
    db.execute(
        """INSERT INTO product_cd_settings (chain_cd, store_cd, product_cd, jan, exclude_deduct, notes)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        [chain_cd, store_cd, product_cd, jan, exclude_deduct, notes]
    )
    db.commit()
    flash(f'商品CD {product_cd or jan} の設定を追加しました', 'success')
    return redirect('/chains#product')

@bp.route('/chains/product-setting/<int:setting_id>/update', methods=['POST'])
@admin_required
def product_setting_update(setting_id):
    db = get_db()
    exclude_deduct = 1 if request.form.get('exclude_deduct') == '1' else 0
    notes          = request.form.get('notes', '').strip()
    db.execute(
        "UPDATE product_cd_settings SET exclude_deduct=%s, notes=%s WHERE id=%s",
        [exclude_deduct, notes, setting_id]
    )
    db.commit()
    flash('商品CD設定を更新しました', 'success')
    return redirect('/chains#product')

@bp.route('/chains/product-setting/<int:setting_id>/delete', methods=['POST'])
@admin_required
def product_setting_delete(setting_id):
    db = get_db()
    db.execute("DELETE FROM product_cd_settings WHERE id=%s", [setting_id])
    db.commit()
    flash('削除しました', 'success')
    return redirect('/chains#product')
