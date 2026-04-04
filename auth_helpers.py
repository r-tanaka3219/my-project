"""認証・認可ヘルパー（デコレータ、権限チェック）"""
import hashlib, hmac
from functools import wraps
from flask import session, redirect, url_for, flash
from extensions import limiter
from werkzeug.security import generate_password_hash, check_password_hash as _wp_check

# ─── パスワードハッシュ ───────────────────────────────────────────
def _hash(pw: str) -> str:
    """新しいパスワードハッシュを生成（PBKDF2-SHA256 + ランダムソルト）"""
    return generate_password_hash(pw)


def _check_hash(stored: str, pw: str) -> bool:
    """保存済みハッシュとパスワードを照合。旧 SHA-256 形式との後方互換あり。"""
    if stored.startswith('pbkdf2:') or stored.startswith('scrypt:'):
        return _wp_check(stored, pw)
    # 旧形式（ソルトなし SHA-256）
    return hmac.compare_digest(stored, hashlib.sha256(pw.encode()).hexdigest())


# ─── ページ権限リスト ─────────────────────────────────────────────
PAGE_PERMISSIONS = [
    ('dashboard',     'ホーム'),
    ('inventory',     '在庫一覧'),
    ('receipt',       '入庫'),
    ('orders',        '発注'),
    ('order_history', '発注履歴'),
    ('stocktake',     '棚卸'),
    ('reports',       'レポート'),
    ('forecast',      'AI予測'),
    ('products',      '商品管理'),
    ('csv',           'CSV取込'),
    ('chains',        'チェーン管理'),
    ('recipients',    'メール宛先'),
    ('users',         'ユーザー管理'),
    ('settings',      '設定'),
]

# 権限名 → Blueprintエンドポイント名のマッピング
PERMISSION_ENDPOINTS = {
    'dashboard':     'dashboard.dashboard',
    'inventory':     'inventory.inventory',
    'receipt':       'receipt.receipt',
    'orders':        'orders.orders',
    'order_history': 'orders.order_history',
    'stocktake':     'stocktake.stocktake',
    'reports':       'reports.reports',
    'forecast':      'forecast.forecast_wholesale',
    'products':      'products.products',
    'csv':           'csv_import.csv_settings',
    'chains':        'chains.chains',
    'recipients':    'recipients.recipients',
    'users':         'admin.users',
    'settings':      'admin.settings',
}


# ─── 権限チェック関数 ─────────────────────────────────────────────
def current_user():
    return session.get('user')


def has_permission(perm):
    if session.get('role') == 'admin':
        return True
    return perm in session.get('permissions', '').split(',')


# ─── 認証デコレータ ───────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user'):
            flash('ログインしてください。', 'warning')
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user'):
            flash('ログインしてください。', 'warning')
            return redirect(url_for('auth.login'))
        if session.get('role') != 'admin':
            flash('管理者権限が必要です。', 'danger')
            return redirect(url_for('dashboard.dashboard'))
        return f(*args, **kwargs)
    return decorated


def permission_required(perm):
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not session.get('user'):
                return redirect(url_for('auth.login'))
            if has_permission(perm):
                return f(*args, **kwargs)
            flash('このページへのアクセス権限がありません', 'danger')
            if perm == 'dashboard' or not has_permission('dashboard'):
                for p, _ in PAGE_PERMISSIONS:
                    if p != 'dashboard' and has_permission(p):
                        endpoint = PERMISSION_ENDPOINTS.get(p, p)
                        return redirect(url_for(endpoint))
                return redirect(url_for('auth.login'))
            return redirect(url_for('dashboard.dashboard'))
        return decorated
    return decorator


# ─── レート制限デコレータ ─────────────────────────────────────────
def _rate_limit(limit_string):
    def decorator(f):
        if limiter:
            return limiter.limit(limit_string)(f)
        return f
    return decorator


# ─── Jinja2 コンテキストプロセッサ ───────────────────────────────
def inject_permissions():
    return dict(has_permission=has_permission, PAGE_PERMISSIONS=PAGE_PERMISSIONS)
