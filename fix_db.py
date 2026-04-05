"""
本番DB カラム追加スクリプト
実行方法: python fix_db.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

import psycopg2

conn = psycopg2.connect(
    host=os.getenv('PG_HOST', 'localhost'),
    port=int(os.getenv('PG_PORT', 5432)),
    dbname=os.getenv('PG_DBNAME', 'inventory'),
    user=os.getenv('PG_USER', 'inventory_user'),
    password=os.getenv('PG_PASSWORD', ''),
)
conn.autocommit = True
cur = conn.cursor()

sqls = [
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS lock_order_qty INTEGER DEFAULT 0",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS manual_adj_factor REAL DEFAULT 1.0",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS mixed_group TEXT DEFAULT ''",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS mixed_lot_mode TEXT DEFAULT 'gte'",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS mixed_lot_cases INTEGER DEFAULT 3",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS mixed_force_days INTEGER DEFAULT 3",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS location_code TEXT DEFAULT ''",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS shelf_face_qty INTEGER DEFAULT 0",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS shelf_replenish_point INTEGER DEFAULT 0",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS season_start_mmdd TEXT",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS season_end_mmdd TEXT",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS ordered_at TEXT DEFAULT ''",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS cost_price REAL DEFAULT 0",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS sell_price REAL DEFAULT 0",
    "ALTER TABLE order_pending ADD COLUMN IF NOT EXISTS mixed_lot_mode TEXT DEFAULT 'gte'",
    "ALTER TABLE order_pending ADD COLUMN IF NOT EXISTS mixed_lot_cases INTEGER DEFAULT 3",
    "ALTER TABLE order_pending ADD COLUMN IF NOT EXISTS mixed_force_days INTEGER DEFAULT 3",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS permissions TEXT DEFAULT ''",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS must_change_password INTEGER DEFAULT 0",
    "ALTER TABLE stocks ADD COLUMN IF NOT EXISTS location_code TEXT DEFAULT ''",
    "ALTER TABLE stock_movements ADD COLUMN IF NOT EXISTS expiry_date TEXT DEFAULT ''",
    "ALTER TABLE order_history ADD COLUMN IF NOT EXISTS expected_receipt_date TEXT DEFAULT ''",
    "ALTER TABLE mail_recipients ADD COLUMN IF NOT EXISTS supplier_cd TEXT DEFAULT ''",
    "ALTER TABLE import_logs ADD COLUMN IF NOT EXISTS trigger_type TEXT DEFAULT 'auto'",
]

ok = 0
for sql in sqls:
    try:
        cur.execute(sql)
        print(f"  OK: {sql[:60]}...")
        ok += 1
    except Exception as e:
        print(f"  SKIP: {e}")

conn.close()
print(f"\n完了: {ok}/{len(sqls)} 件処理")
print("サービスを再起動してください")
