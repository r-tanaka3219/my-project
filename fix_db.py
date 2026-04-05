"""
本番DB カラム追加スクリプト（手動実行用）
実行方法: python fix_db.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

try:
    from database import migrate_db, _grant_privileges
    print("権限付与中...")
    _grant_privileges()
    print("マイグレーション実行中...")
    migrate_db()
    print("\n[OK] 完了しました。サービスを再起動してください。")
except Exception as e:
    print(f"\n[ERROR] {e}")
    sys.exit(1)
