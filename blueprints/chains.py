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
    return render_template('chains.html', chains=chains, stores=stores)

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
