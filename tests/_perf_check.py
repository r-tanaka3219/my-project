"""パフォーマンス修正確認"""
import sys, os, io, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv('.env')
from database import get_db

db = get_db()

# 修正後クエリ確認
start = time.time()
rows = db.execute("""
    SELECT p.*, COALESCE(s.stock_qty, 0) AS stock_qty
    FROM products p
    LEFT JOIN (SELECT jan, SUM(quantity) AS stock_qty FROM stocks GROUP BY jan) s
           ON s.jan = p.jan
    WHERE p.is_active=1
    ORDER BY CAST(NULLIF(regexp_replace(p.supplier_cd,'[^0-9]','','g'),'') AS BIGINT) NULLS LAST,
             CAST(NULLIF(regexp_replace(p.product_cd,'[^0-9]','','g'),'') AS BIGINT) NULLS LAST
""").fetchall()
elapsed = time.time() - start
print(f'商品件数: {len(rows)} 件')
print(f'クエリ時間: {elapsed*1000:.1f} ms（1クエリで完結）')

# q フィルタ確認
rows_q = db.execute("""
    SELECT p.*, COALESCE(s.stock_qty, 0) AS stock_qty
    FROM products p
    LEFT JOIN (SELECT jan, SUM(quantity) AS stock_qty FROM stocks GROUP BY jan) s
           ON s.jan = p.jan
    WHERE p.is_active=1
      AND (p.jan ILIKE %s OR p.product_cd ILIKE %s OR p.product_name ILIKE %s
           OR p.supplier_cd ILIKE %s OR p.supplier_name ILIKE %s)
    ORDER BY p.supplier_cd, p.product_cd
""", ['%梅%']*5).fetchall()
print(f'絞り込み(梅): {len(rows_q)} 件')

# インデックス確認
idx = db.execute("""
    SELECT indexname FROM pg_indexes
    WHERE tablename IN ('order_pending','alert_logs','products')
    AND indexname LIKE 'ix_%'
    ORDER BY tablename, indexname
""").fetchall()
print()
print('適用済みインデックス:')
for r in idx:
    print(f'  {r["indexname"]}')

db.close()
