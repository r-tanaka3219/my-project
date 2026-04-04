"""テストデータクリーンアップ"""
import sys, os, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv('.env')
from database import get_db

TARGET_JAN = '4571239218338'
TARGET_ORDER_ID = 1948

db = get_db()

cur_stock = db.execute('SELECT COALESCE(SUM(quantity),0) AS s FROM stocks WHERE jan=%s', [TARGET_JAN]).fetchone()['s']
cur_recv  = db.execute('SELECT COALESCE(SUM(received_qty),0) AS r FROM order_receipts WHERE order_history_id=%s', [TARGET_ORDER_ID]).fetchone()['r']
print(f'クリーンアップ前: 在庫={cur_stock}, 受領済={cur_recv}')

# テストで追加された order_receipts 削除
n1 = db.execute("DELETE FROM order_receipts WHERE order_history_id=%s AND note='入庫登録より自動連動'", [TARGET_ORDER_ID]).rowcount
print(f'order_receipts 削除: {n1} 件')

# テストで追加された stock_movements 削除
n2 = db.execute("DELETE FROM stock_movements WHERE jan=%s AND source_file='manual' AND note LIKE 'receipt_hash:%%'", [TARGET_JAN]).rowcount
print(f'stock_movements 削除: {n2} 件')

# テストで追加された stocks 削除（最後に追加された qty=5, qty=7 の行）
added = db.execute(
    "SELECT id, quantity FROM stocks WHERE jan=%s AND lot_no='' AND location_code='' ORDER BY id DESC LIMIT 2",
    [TARGET_JAN]
).fetchall()
for s in added:
    print(f'  stocks.id={s["id"]} qty={s["quantity"]} -> 削除')
    db.execute('DELETE FROM stocks WHERE id=%s', [s['id']])

db.commit()

after_stock = db.execute('SELECT COALESCE(SUM(quantity),0) AS s FROM stocks WHERE jan=%s', [TARGET_JAN]).fetchone()['s']
after_recv  = db.execute('SELECT COALESCE(SUM(received_qty),0) AS r FROM order_receipts WHERE order_history_id=%s', [TARGET_ORDER_ID]).fetchone()['r']
print(f'クリーンアップ後: 在庫={after_stock}, 受領済={after_recv}')
print('完了')
db.close()
