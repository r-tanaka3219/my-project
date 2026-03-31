"""Flask 拡張機能の初期化（アプリファクトリパターン用）"""

try:
    from flask_wtf.csrf import CSRFProtect
    csrf = CSRFProtect()
except ImportError:
    csrf = None

try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
    limiter = Limiter(get_remote_address, default_limits=[], storage_uri='memory://')
except ImportError:
    limiter = None


def init_extensions(app):
    if csrf:
        csrf.init_app(app)
    if limiter:
        limiter.init_app(app)
