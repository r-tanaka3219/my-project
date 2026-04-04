"""認証 Blueprint"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, session
import hashlib, logging
from db import get_db
from auth_helpers import _hash, _check_hash, _rate_limit, has_permission, PAGE_PERMISSIONS

logger = logging.getLogger('inventory.auth')
bp = Blueprint('auth', __name__)


@bp.route('/login', methods=['GET','POST'])
@_rate_limit('10 per minute')
def login():
    if session.get('user'):
        if has_permission('dashboard'):
            return redirect(url_for('dashboard.dashboard'))
        for perm, _ in PAGE_PERMISSIONS:
            if perm != 'dashboard' and has_permission(perm):
                return redirect(url_for(perm))
    if request.method == 'POST':
        username = request.form.get('username','').strip()
        password = request.form.get('password','')
        db = get_db()
        user = db.execute(
            "SELECT * FROM users WHERE username=%s AND is_active=1", [username]
        ).fetchone()
        if user and _check_hash(user['password'], password):
            # 旧 SHA-256 ハッシュの場合は PBKDF2 形式へ自動アップグレード
            if not user['password'].startswith('pbkdf2:') and not user['password'].startswith('scrypt:'):
                db.execute("UPDATE users SET password=%s WHERE username=%s",
                           [_hash(password), user['username']])
                db.commit()
            session.clear()
            session['user'] = user['username']
            session['role'] = user['role']
            session['permissions'] = user['permissions'] or ''
            # 初期パスワード強制変更チェック
            if user.get('must_change_password'):
                flash('セキュリティのため、パスワードを変更してください。', 'warning')
                return redirect(url_for('auth.change_password_required'))
            flash(f'ようこそ、{user["username"]} さん。', 'success')
            # 権限に応じた最初のページへリダイレクト
            if has_permission('dashboard'):
                return redirect(url_for('dashboard.dashboard'))
            # ダッシュボード権限がない場合は最初に許可されたページへ
            for perm, _ in PAGE_PERMISSIONS:
                if perm != 'dashboard' and has_permission(perm):
                    return redirect(url_for(perm))
            return redirect(url_for('auth.login'))
        flash('IDまたはパスワードが違います。', 'danger')
    return render_template('login.html')

@bp.route('/logout')
def logout():
    session.clear()
    flash('ログアウトしました。', 'success')
    return redirect(url_for('auth.login'))


@bp.route('/change_password_required', methods=['GET', 'POST'])
def change_password_required():
    if not session.get('user'):
        return redirect(url_for('auth.login'))
    if request.method == 'POST':
        pw1 = request.form.get('password', '').strip()
        pw2 = request.form.get('password2', '').strip()
        # デフォルトパスワードへの変更を禁止
        _default_hashes = {
            '240be518fabd2724ddb6f04eeb1da5967448d7e831c08c8fa822809f74c720a9',
            'e606e38b0d8c19b24cf0ee3808183162ea7cd63ff7912dbb22b5e803286b4446',
        }
        if not pw1 or len(pw1) < 8:
            flash('パスワードは8文字以上で入力してください。', 'danger')
        elif pw1 != pw2:
            flash('パスワードが一致しません。', 'danger')
        elif hashlib.sha256(pw1.encode()).hexdigest() in _default_hashes:
            flash('初期パスワードは使用できません。別のパスワードを設定してください。', 'danger')
        else:
            db = get_db()
            db.execute(
                "UPDATE users SET password=%s, must_change_password=0 WHERE username=%s",
                [_hash(pw1), session['user']]
            )
            db.commit()
            flash('パスワードを変更しました。', 'success')
            if has_permission('dashboard'):
                return redirect(url_for('dashboard.dashboard'))
            for perm, _ in PAGE_PERMISSIONS:
                if perm != 'dashboard' and has_permission(perm):
                    return redirect(url_for(perm))
            return redirect(url_for('auth.login'))
    return render_template('change_password_required.html')
