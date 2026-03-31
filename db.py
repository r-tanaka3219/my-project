"""リクエストスコープの DB 接続管理"""
from flask import g


def get_db():
    if 'db' not in g:
        from database import get_db as _get_db
        g.db = _get_db()
    return g.db


def close_db(error=None):
    db = g.pop('db', None)
    if db is not None:
        try:
            db.close()
        except Exception:
            pass
