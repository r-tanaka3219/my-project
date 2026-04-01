"""
Database layer - PostgreSQL (psycopg2)
SQLite から PostgreSQL へ移行版
"""
import os
import logging
import psycopg2
import psycopg2.extras
from datetime import date, timedelta
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger('inventory.database')

# database.py が直接 import された場合でも .env を確実に読み込む
load_dotenv(Path(__file__).parent / '.env', override=False)

def get_dsn(long_timeout=False):
    opts = '-c lock_timeout=0 -c statement_timeout=0 -c idle_in_transaction_session_timeout=0'
    return {
        'host':     os.getenv('PG_HOST',     'localhost'),
        'port':     int(os.getenv('PG_PORT', '5432')),
        'dbname':   os.getenv('PG_DBNAME',   'inventory'),
        'user':     os.getenv('PG_USER',     'inventory_user'),
        'password': os.getenv('PG_PASSWORD', ''),
        'connect_timeout': 10,
        'options':  opts,
    }

# ── psycopg2 を sqlite3 互換に見せるラッパー ──────────────────────

class _CursorWrapper:
    """? プレースホルダー→%s 変換、fetchone/all を RealDictRow で返す"""
    def __init__(self, conn):
        self._cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    @staticmethod
    def _q(sql):
        return sql.replace('?', '%s')

    def execute(self, sql, params=None):
        self._cur.execute(self._q(sql), params)
        return self

    def executemany(self, sql, seq):
        self._cur.executemany(self._q(sql), seq)
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def __iter__(self):
        return iter(self._cur)

    @property
    def lastrowid(self):
        self._cur.execute("SELECT lastval()")
        return self._cur.fetchone()['lastval']

    @property
    def rowcount(self):
        return self._cur.rowcount


class DBConn:
    """sqlite3.Connection 互換ラッパー"""
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        w = _CursorWrapper(self._conn)
        w.execute(sql, params)
        return w

    def executemany(self, sql, seq):
        w = _CursorWrapper(self._conn)
        w.executemany(sql, seq)
        return w

    def executescript(self, script):
        """DDL など複数文をセミコロン区切りで実行"""
        cur = self._conn.cursor()
        for stmt in script.split(';'):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()


def get_db() -> DBConn:
    conn = psycopg2.connect(**get_dsn())
    conn.autocommit = False
    return DBConn(conn)


# _migrate: removed (columns already applied)


# ── DDL (PostgreSQL 方言) ────────────────────────────────────────
_DDL = """
CREATE TABLE IF NOT EXISTS products (
    id                  SERIAL PRIMARY KEY,
    supplier_cd         TEXT NOT NULL,
    supplier_name       TEXT NOT NULL,
    supplier_email      TEXT DEFAULT '',
    jan                 TEXT NOT NULL UNIQUE,
    product_cd          TEXT NOT NULL,
    product_name        TEXT NOT NULL,
    unit_qty            INTEGER DEFAULT 1,
    order_unit          INTEGER DEFAULT 1,
    order_qty           INTEGER DEFAULT 1,
    reorder_point       INTEGER DEFAULT 0,
    reorder_auto        INTEGER DEFAULT 1,
    lot_size            INTEGER DEFAULT 0,
    shelf_life_days     INTEGER DEFAULT 365,
    expiry_alert_days   INTEGER DEFAULT 30,
    safety_factor       REAL    DEFAULT 1.3,
    lead_time_days      INTEGER DEFAULT 3,
    is_active           INTEGER DEFAULT 1,
    mixed_group         TEXT    DEFAULT '',
    mixed_lot_mode      TEXT    DEFAULT 'gte',
    mixed_lot_cases     INTEGER DEFAULT 3,
    mixed_force_days    INTEGER DEFAULT 3,
    created_at          TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS stocks (
    id            SERIAL PRIMARY KEY,
    product_id    INTEGER REFERENCES products(id),
    jan           TEXT NOT NULL,
    product_name  TEXT NOT NULL,
    supplier_cd   TEXT NOT NULL,
    supplier_name TEXT NOT NULL,
    product_cd    TEXT NOT NULL,
    unit_qty      INTEGER DEFAULT 1,
    quantity      INTEGER DEFAULT 0,
    expiry_date   TEXT DEFAULT '',
    lot_no        TEXT DEFAULT '',
    created_at    TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS order_pending (
    id              SERIAL PRIMARY KEY,
    supplier_cd     TEXT NOT NULL,
    supplier_name   TEXT NOT NULL,
    supplier_email  TEXT DEFAULT '',
    mixed_group     TEXT DEFAULT '',
    mixed_lot_mode  TEXT DEFAULT 'gte',
    mixed_lot_cases INTEGER DEFAULT 3,
    jan             TEXT NOT NULL,
    product_cd      TEXT NOT NULL,
    product_name    TEXT NOT NULL,
    order_qty       INTEGER DEFAULT 0,
    order_cases     INTEGER DEFAULT 0,
    trigger_type    TEXT DEFAULT 'reorder',
    pending_since   TEXT NOT NULL,
    force_send_date TEXT NOT NULL,
    status          TEXT DEFAULT 'pending',
    created_at      TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS sales_history (
    id           SERIAL PRIMARY KEY,
    jan          TEXT NOT NULL,
    product_name TEXT NOT NULL,
    quantity     INTEGER DEFAULT 0,
    sale_date    TEXT NOT NULL,
    source_file  TEXT DEFAULT '',
    row_hash     TEXT DEFAULT '',
    created_at   TIMESTAMP DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_sales_row_hash ON sales_history(row_hash) WHERE row_hash IS NOT NULL AND row_hash <> '';


-- チェーン・店舗除外管理
CREATE TABLE IF NOT EXISTS chain_masters (
    id             SERIAL PRIMARY KEY,
    chain_cd       TEXT NOT NULL UNIQUE,
    chain_name     TEXT DEFAULT '',
    exclude_deduct INTEGER DEFAULT 0,
    created_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS store_masters (
    id             SERIAL PRIMARY KEY,
    store_cd       TEXT NOT NULL UNIQUE,
    store_name     TEXT DEFAULT '',
    chain_cd       TEXT DEFAULT '',
    client_name    TEXT DEFAULT '',
    exclude_deduct INTEGER DEFAULT 0,
    created_at     TIMESTAMP DEFAULT NOW()
);

-- sales_history にチェーン情報カラム追加（ALTER TABLE は migrate_db で実行）

CREATE TABLE IF NOT EXISTS stock_movements (
    id           SERIAL PRIMARY KEY,
    jan          TEXT NOT NULL,
    product_name TEXT NOT NULL,
    move_type    TEXT NOT NULL,
    quantity     INTEGER DEFAULT 0,
    before_qty   INTEGER DEFAULT 0,
    after_qty    INTEGER DEFAULT 0,
    note         TEXT DEFAULT '',
    source_file  TEXT DEFAULT '',
    move_date    TEXT NOT NULL,
    created_at   TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS order_history (
    id              SERIAL PRIMARY KEY,
    supplier_cd     TEXT NOT NULL,
    supplier_name   TEXT NOT NULL,
    supplier_email  TEXT DEFAULT '',
    jan             TEXT NOT NULL,
    product_cd      TEXT NOT NULL,
    product_name    TEXT NOT NULL,
    order_qty       INTEGER DEFAULT 0,
    trigger_type    TEXT DEFAULT 'manual',
    order_date      TEXT NOT NULL,
    mail_sent       INTEGER DEFAULT 0,
    mail_result     TEXT DEFAULT '',
    created_at      TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS alert_logs (
    id           SERIAL PRIMARY KEY,
    alert_type   TEXT NOT NULL,
    jan          TEXT DEFAULT '',
    product_name TEXT DEFAULT '',
    message      TEXT DEFAULT '',
    mail_sent    INTEGER DEFAULT 0,
    created_at   TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS csv_import_settings (
    id               SERIAL PRIMARY KEY,
    name             TEXT NOT NULL,
    import_type      TEXT DEFAULT 'sales',
    folder_path      TEXT DEFAULT '',
    net_user         TEXT DEFAULT '',
    net_pass         TEXT DEFAULT '',
    filename_pattern TEXT DEFAULT '*{yyyymmdd}.csv',
    encoding         TEXT DEFAULT 'utf-8-sig',
    col_jan          TEXT DEFAULT 'JANコード',
    col_qty          TEXT DEFAULT '数量',
    col_date         TEXT DEFAULT '納品日',
    col_slip_no      TEXT DEFAULT '伝票番号',
    col_chain_cd     TEXT DEFAULT 'チェーンCD',
    col_row_no       TEXT DEFAULT '行番号',
    col_expiry       TEXT DEFAULT '賞味期限',
    col_cases        TEXT DEFAULT 'ケース',
    col_pieces       TEXT DEFAULT 'ピース',
    col_supplier_cd  TEXT DEFAULT '仕入先CD',
    col_filter_cd    TEXT DEFAULT '担当CD',
    filter_cd_values TEXT DEFAULT '',
    run_times        TEXT DEFAULT '06:00',
    run_hour         INTEGER DEFAULT 6,
    run_minute       INTEGER DEFAULT 0,
    is_active        INTEGER DEFAULT 1,
    last_run_at      TEXT DEFAULT '',
    last_result      TEXT DEFAULT '',
    month_end_enabled  INTEGER DEFAULT 0,
    month_end_folder   TEXT    DEFAULT '',
    month_end_pattern  TEXT    DEFAULT '{yyyymm}_売上実績.csv',
    month_end_date_col TEXT    DEFAULT '納品日',
    created_at       TIMESTAMP DEFAULT NOW()
);
    -- 月末月次CSV設定（ALTER TABLE は migrate_db で実行）

CREATE TABLE IF NOT EXISTS import_logs (
    id          SERIAL PRIMARY KEY,
    setting_id  INTEGER REFERENCES csv_import_settings(id),
    filename    TEXT NOT NULL,
    rows_ok     INTEGER DEFAULT 0,
    rows_err    INTEGER DEFAULT 0,
    status      TEXT DEFAULT '',
    detail      TEXT DEFAULT '',
    imported_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS mail_recipients (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT NOT NULL UNIQUE,
    send_type  TEXT DEFAULT 'both',
    is_active  INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS users (
    id         SERIAL PRIMARY KEY,
    username   TEXT NOT NULL UNIQUE,
    password   TEXT NOT NULL,
    role       TEXT NOT NULL DEFAULT 'user',
    is_active  INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS inventory_count (
    id           SERIAL PRIMARY KEY,
    count_date   TEXT NOT NULL,
    jan          TEXT NOT NULL,
    product_name TEXT NOT NULL,
    system_qty   INTEGER DEFAULT 0,
    actual_qty   INTEGER DEFAULT 0,
    diff_qty     INTEGER DEFAULT 0,
    adjusted     INTEGER DEFAULT 0,
    note         TEXT DEFAULT '',
    created_at   TIMESTAMP DEFAULT NOW(),
    UNIQUE(count_date, jan)
)
"""


_MIGRATIONS = [
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS permissions TEXT DEFAULT ''",
    "ALTER TABLE sales_history ADD COLUMN IF NOT EXISTS chain_cd    TEXT DEFAULT ''",
    "ALTER TABLE sales_history ADD COLUMN IF NOT EXISTS client_name TEXT DEFAULT ''",
    "ALTER TABLE sales_history ADD COLUMN IF NOT EXISTS store_cd    TEXT DEFAULT ''",
    "ALTER TABLE sales_history ADD COLUMN IF NOT EXISTS store_name  TEXT DEFAULT ''",
    "ALTER TABLE csv_import_settings ADD COLUMN IF NOT EXISTS month_end_enabled  INTEGER DEFAULT 0",
    "ALTER TABLE csv_import_settings ADD COLUMN IF NOT EXISTS month_end_folder   TEXT    DEFAULT ''",
    "ALTER TABLE csv_import_settings ADD COLUMN IF NOT EXISTS month_end_pattern  TEXT    DEFAULT '{yyyymm}_売上実績.csv'",
    "ALTER TABLE csv_import_settings ADD COLUMN IF NOT EXISTS month_end_date_col TEXT    DEFAULT '納品日'",
    "ALTER TABLE csv_import_settings ADD COLUMN IF NOT EXISTS col_client_name    TEXT    DEFAULT '社名'",
    "ALTER TABLE csv_import_settings ADD COLUMN IF NOT EXISTS col_store_cd       TEXT    DEFAULT '得意先CD'",
    "ALTER TABLE csv_import_settings ADD COLUMN IF NOT EXISTS col_store_name     TEXT    DEFAULT '店舗名'",
    "ALTER TABLE import_logs ADD COLUMN IF NOT EXISTS trigger_type TEXT DEFAULT 'auto'",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS ordered_at TEXT DEFAULT ''",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS cost_price NUMERIC(10,2) DEFAULT 0",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS sell_price NUMERIC(10,2) DEFAULT 0",
    "ALTER TABLE disposed_stocks ADD COLUMN IF NOT EXISTS cost_price NUMERIC(10,2) DEFAULT 0",
    "ALTER TABLE disposed_stocks ADD COLUMN IF NOT EXISTS loss_amount NUMERIC(10,2) DEFAULT 0",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS cost_price REAL DEFAULT 0",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS sell_price REAL DEFAULT 0",
    "ALTER TABLE disposed_stocks ADD COLUMN IF NOT EXISTS product_cd TEXT DEFAULT ''",
    "ALTER TABLE settings ADD COLUMN IF NOT EXISTS order_history_months INTEGER DEFAULT 12",
    "ALTER TABLE settings ADD COLUMN IF NOT EXISTS disposed_months INTEGER DEFAULT 12",
    "ALTER TABLE disposed_stocks ADD COLUMN IF NOT EXISTS cost_price REAL DEFAULT 0",
    "ALTER TABLE disposed_stocks ADD COLUMN IF NOT EXISTS loss_amount REAL DEFAULT 0",
    "CREATE TABLE IF NOT EXISTS disposed_stocks (id SERIAL PRIMARY KEY, jan TEXT NOT NULL, product_name TEXT NOT NULL, supplier_cd TEXT DEFAULT '', supplier_name TEXT DEFAULT '', quantity INTEGER DEFAULT 0, expiry_date TEXT DEFAULT '', lot_no TEXT DEFAULT '', reason_type TEXT DEFAULT '', reason_note TEXT DEFAULT '', disposed_at TEXT NOT NULL, cost_price REAL DEFAULT 0, loss_amount REAL DEFAULT 0, created_at TIMESTAMP DEFAULT NOW())",
    "CREATE TABLE IF NOT EXISTS settings (id SERIAL PRIMARY KEY, key TEXT NOT NULL UNIQUE, value TEXT DEFAULT '', created_at TIMESTAMP DEFAULT NOW())",
    "CREATE TABLE IF NOT EXISTS mail_templates (id SERIAL PRIMARY KEY, mail_type TEXT NOT NULL UNIQUE, subject TEXT NOT NULL, body_header TEXT DEFAULT '', body_item TEXT NOT NULL, body_footer TEXT DEFAULT '', created_at TIMESTAMP DEFAULT NOW())",
    "CREATE TABLE IF NOT EXISTS scheduler_run_log (id SERIAL PRIMARY KEY, job_key TEXT NOT NULL, run_date TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW(), UNIQUE(job_key, run_date))",

    "ALTER TABLE products ADD COLUMN IF NOT EXISTS location_code TEXT DEFAULT ''",
    "ALTER TABLE stocks ADD COLUMN IF NOT EXISTS location_code TEXT DEFAULT ''",
    "ALTER TABLE stock_movements ADD COLUMN IF NOT EXISTS expiry_date TEXT DEFAULT ''",
    "ALTER TABLE inventory_count ADD COLUMN IF NOT EXISTS expiry_detail TEXT DEFAULT ''",
    "ALTER TABLE inventory_count ADD COLUMN IF NOT EXISTS diff_reason_category TEXT DEFAULT ''",
    "ALTER TABLE inventory_count ADD COLUMN IF NOT EXISTS diff_reason_detail TEXT DEFAULT ''",
    "CREATE TABLE IF NOT EXISTS order_receipts (id SERIAL PRIMARY KEY, order_history_id INTEGER REFERENCES order_history(id), jan TEXT NOT NULL, received_qty INTEGER DEFAULT 0, receipt_date TEXT NOT NULL, note TEXT DEFAULT '', created_at TIMESTAMP DEFAULT NOW())",
    "ALTER TABLE order_history ADD COLUMN IF NOT EXISTS expected_receipt_date TEXT DEFAULT ''",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS shelf_face_qty INTEGER DEFAULT 0",
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS shelf_replenish_point INTEGER DEFAULT 0",
    "CREATE TABLE IF NOT EXISTS promotion_plans (id SERIAL PRIMARY KEY, jan TEXT NOT NULL, promo_date DATE NOT NULL, promo_name TEXT DEFAULT '', uplift_factor REAL DEFAULT 1.0, created_at TIMESTAMP DEFAULT NOW(), UNIQUE(jan, promo_date, promo_name))",
    "CREATE TABLE IF NOT EXISTS stock_transfers (id SERIAL PRIMARY KEY, jan TEXT NOT NULL, from_stock_id INTEGER, to_stock_id INTEGER, from_location TEXT DEFAULT '', to_location TEXT DEFAULT '', quantity INTEGER DEFAULT 0, lot_no TEXT DEFAULT '', expiry_date TEXT DEFAULT '', note TEXT DEFAULT '', transfer_date TEXT NOT NULL, created_by TEXT DEFAULT '', created_at TIMESTAMP DEFAULT NOW())",
    "CREATE TABLE IF NOT EXISTS demand_plans (id SERIAL PRIMARY KEY, jan TEXT NOT NULL, demand_date DATE NOT NULL, demand_qty INTEGER DEFAULT 0, demand_type TEXT DEFAULT 'order', customer_name TEXT DEFAULT '', note TEXT DEFAULT '', created_by TEXT DEFAULT '', created_at TIMESTAMP DEFAULT NOW())",
    "CREATE INDEX IF NOT EXISTS ix_demand_plans_jan_date ON demand_plans(jan, demand_date)",
    # P3: 手動調整係数
    "ALTER TABLE products ADD COLUMN IF NOT EXISTS manual_adj_factor REAL DEFAULT 1.0",
    # P2: 分位点発注点モードをsettingsで管理（値はsettingsテーブルに格納）
    "INSERT INTO settings(key,value) SELECT 'forecast_reorder_mode','sf' WHERE NOT EXISTS (SELECT 1 FROM settings WHERE key='forecast_reorder_mode')",
    "INSERT INTO settings(key,value) SELECT 'forecast_ai_mode','1' WHERE NOT EXISTS (SELECT 1 FROM settings WHERE key='forecast_ai_mode')",  # 1=AIモードON（デフォルト）
    "INSERT INTO settings(key,value) SELECT 'forecast_manual_adj','1' WHERE NOT EXISTS (SELECT 1 FROM settings WHERE key='forecast_manual_adj')",
    "CREATE TABLE IF NOT EXISTS replenishment_history (id SERIAL PRIMARY KEY, jan TEXT NOT NULL, product_name TEXT NOT NULL, shelf_location TEXT DEFAULT '', from_location TEXT DEFAULT '', planned_qty INTEGER DEFAULT 0, completed_qty INTEGER DEFAULT 0, task_date TEXT NOT NULL, completed_at TEXT DEFAULT '', status TEXT DEFAULT 'planned', note TEXT DEFAULT '', created_by TEXT DEFAULT '', completed_by TEXT DEFAULT '', created_at TIMESTAMP DEFAULT NOW())",
    "CREATE INDEX IF NOT EXISTS ix_replenishment_history_jan_date ON replenishment_history(jan, task_date)",
    # 初期パスワード強制変更フラグ
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS must_change_password INTEGER DEFAULT 0",
    # デフォルトパスワードのままのユーザーにフラグを立てる（admin123 / user123）
    "UPDATE users SET must_change_password=1 WHERE password IN ('240be518fabd2724ddb6f04eeb1da5967448d7e831c08c8fa822809f74c720a9','e606e38b0d8c19b24cf0ee3808183162ea7cd63ff7912dbb22b5e803286b4446') AND must_change_password=0",

    # ── 問屋向け拡張 ──────────────────────────────────────────────────────
    # 気温データ（気象庁API or 手動入力）
    """CREATE TABLE IF NOT EXISTS weather_data (
        id           SERIAL PRIMARY KEY,
        obs_date     DATE NOT NULL,
        location     TEXT DEFAULT '東京',
        avg_temp     REAL,
        max_temp     REAL,
        min_temp     REAL,
        precipitation REAL DEFAULT 0,
        source       TEXT DEFAULT 'manual',
        created_at   TIMESTAMP DEFAULT NOW(),
        UNIQUE(obs_date, location)
    )""",
    "CREATE INDEX IF NOT EXISTS ix_weather_data_date ON weather_data(obs_date)",

    # 52週MDプラン（週次販売計画）
    """CREATE TABLE IF NOT EXISTS weekly_md_plans (
        id            SERIAL PRIMARY KEY,
        jan           TEXT NOT NULL,
        fiscal_year   INTEGER NOT NULL,
        week_no       INTEGER NOT NULL,
        week_start    DATE NOT NULL,
        plan_qty      INTEGER DEFAULT 0,
        plan_amount   NUMERIC(14,2) DEFAULT 0,
        actual_qty    INTEGER DEFAULT 0,
        actual_amount NUMERIC(14,2) DEFAULT 0,
        note          TEXT DEFAULT '',
        created_by    TEXT DEFAULT '',
        created_at    TIMESTAMP DEFAULT NOW(),
        updated_at    TIMESTAMP DEFAULT NOW(),
        UNIQUE(jan, fiscal_year, week_no)
    )""",
    "CREATE INDEX IF NOT EXISTS ix_weekly_md_jan_year ON weekly_md_plans(jan, fiscal_year)",

    # 得意先マスタ（問屋→小売店）
    """CREATE TABLE IF NOT EXISTS customer_masters (
        id             SERIAL PRIMARY KEY,
        customer_cd    TEXT NOT NULL UNIQUE,
        customer_name  TEXT NOT NULL,
        customer_type  TEXT DEFAULT 'retailer',
        region         TEXT DEFAULT '',
        contact_email  TEXT DEFAULT '',
        is_active      INTEGER DEFAULT 1,
        note           TEXT DEFAULT '',
        created_at     TIMESTAMP DEFAULT NOW()
    )""",

    # 気温感応度（商品×気温の相関係数をキャッシュ）
    """CREATE TABLE IF NOT EXISTS temp_sensitivity (
        id              SERIAL PRIMARY KEY,
        jan             TEXT NOT NULL UNIQUE,
        temp_coef       REAL DEFAULT 0,
        r_squared       REAL DEFAULT 0,
        base_temp       REAL DEFAULT 20.0,
        updated_at      TIMESTAMP DEFAULT NOW()
    )""",

    # 予測結果キャッシュ（計算コスト削減）
    """CREATE TABLE IF NOT EXISTS forecast_cache (
        id              SERIAL PRIMARY KEY,
        jan             TEXT NOT NULL,
        calc_date       DATE NOT NULL,
        abc_rank        TEXT DEFAULT 'C',
        q25_daily       REAL DEFAULT 0,
        q50_daily       REAL DEFAULT 0,
        q75_daily       REAL DEFAULT 0,
        dynamic_ss      REAL DEFAULT 0,
        suggested_rp    INTEGER DEFAULT 0,
        suggested_oq    INTEGER DEFAULT 0,
        temp_adj_factor REAL DEFAULT 1.0,
        algorithm       TEXT DEFAULT 'sma',
        updated_at      TIMESTAMP DEFAULT NOW(),
        UNIQUE(jan, calc_date)
    )""",
    "CREATE INDEX IF NOT EXISTS ix_forecast_cache_jan_date ON forecast_cache(jan, calc_date)",

    # settingsに問屋向け設定追加
    "INSERT INTO settings(key,value) SELECT 'wholesale_mode','1' WHERE NOT EXISTS (SELECT 1 FROM settings WHERE key='wholesale_mode')",
    "INSERT INTO settings(key,value) SELECT 'safety_level_z','1.65' WHERE NOT EXISTS (SELECT 1 FROM settings WHERE key='safety_level_z')",
    "INSERT INTO settings(key,value) SELECT 'abc_a_threshold','0.70' WHERE NOT EXISTS (SELECT 1 FROM settings WHERE key='abc_a_threshold')",
    "INSERT INTO settings(key,value) SELECT 'abc_b_threshold','0.90' WHERE NOT EXISTS (SELECT 1 FROM settings WHERE key='abc_b_threshold')",
    "INSERT INTO settings(key,value) SELECT 'weather_location','東京' WHERE NOT EXISTS (SELECT 1 FROM settings WHERE key='weather_location')",

    # 仕入先CD単位の設定（チェーン・店舗別）
    """CREATE TABLE IF NOT EXISTS supplier_cd_settings (
        id             SERIAL PRIMARY KEY,
        chain_cd       TEXT,
        store_cd       TEXT,
        supplier_cd    TEXT NOT NULL,
        exclude_deduct INTEGER DEFAULT 0,
        notes          TEXT DEFAULT '',
        created_at     TIMESTAMP DEFAULT NOW(),
        UNIQUE(chain_cd, store_cd, supplier_cd)
    )""",
    "CREATE INDEX IF NOT EXISTS ix_supplier_cd_settings_chain ON supplier_cd_settings(chain_cd)",
    "CREATE INDEX IF NOT EXISTS ix_supplier_cd_settings_store ON supplier_cd_settings(store_cd)",

    # 商品CD単位の設定（チェーン・店舗別）
    """CREATE TABLE IF NOT EXISTS product_cd_settings (
        id             SERIAL PRIMARY KEY,
        chain_cd       TEXT,
        store_cd       TEXT,
        product_cd     TEXT,
        jan            TEXT,
        exclude_deduct INTEGER DEFAULT 0,
        notes          TEXT DEFAULT '',
        created_at     TIMESTAMP DEFAULT NOW()
    )""",
    "CREATE INDEX IF NOT EXISTS ix_product_cd_settings_chain ON product_cd_settings(chain_cd)",
    "CREATE INDEX IF NOT EXISTS ix_product_cd_settings_store ON product_cd_settings(store_cd)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_product_cd_settings ON product_cd_settings(chain_cd, store_cd, COALESCE(product_cd,''), COALESCE(jan,''))",
    # mail_recipients に仕入先CD列追加
    "ALTER TABLE mail_recipients ADD COLUMN IF NOT EXISTS supplier_cd TEXT DEFAULT ''",
]

def migrate_db():
    conn = psycopg2.connect(**get_dsn(long_timeout=True))
    conn.autocommit = True
    cur = conn.cursor()
    for sql in _MIGRATIONS:
        try:
            cur.execute(sql)
        except Exception:
            pass
    conn.close()

def init_db():
    conn = psycopg2.connect(**get_dsn(long_timeout=True))
    conn.autocommit = False
    db = DBConn(conn)
    try:
        db.executescript(_DDL)
        db.commit()
        cur = db.execute("SELECT COUNT(*) AS cnt FROM products")
        if cur.fetchone()['cnt'] == 0:
            _insert_samples(db)
            db.commit()
        logger.info("OK: Database ready.")
    finally:
        db.close()
    migrate_db()


def _insert_samples(db):
    today = date.today()
    db.executemany("""
        INSERT INTO products
        (supplier_cd,supplier_name,supplier_email,jan,product_cd,product_name,
         unit_qty,order_unit,order_qty,reorder_point,lot_size,shelf_life_days,
         expiry_alert_days,safety_factor,lead_time_days)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, [
        ('SUP001','山田食品','order@example.com','4901234567890','P001','りんごジュース 1L',  12,1,24,48,24,30,7,1.3,3),
        ('SUP001','山田食品','order@example.com','4901234567891','P002','オレンジジュース 1L',12,1,24,36,24,30,7,1.3,3),
        ('SUP002','佐藤飲料','order@example.com','4909876543210','P003','緑茶 500ml',         24,2,48,96,48,60,14,1.5,5),
        ('SUP002','佐藤飲料','order@example.com','4909876543211','P004','麦茶 500ml',         24,1,48,72,48,90,14,1.5,5),
    ])
    products = db.execute("SELECT id, jan FROM products").fetchall()
    pid = {r['jan']: r['id'] for r in products}
    db.executemany("""
        INSERT INTO stocks
        (product_id,jan,product_name,supplier_cd,supplier_name,product_cd,unit_qty,quantity,expiry_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, [
        (pid['4901234567890'],'4901234567890','りんごジュース 1L','SUP001','山田食品','P001',12,60, str(today+timedelta(days=180))),
        (pid['4901234567890'],'4901234567890','りんごジュース 1L','SUP001','山田食品','P001',12,12, str(today+timedelta(days=6))),
        (pid['4901234567891'],'4901234567891','オレンジジュース 1L','SUP001','山田食品','P002',12,24, str(today+timedelta(days=120))),
        (pid['4909876543210'],'4909876543210','緑茶 500ml','SUP002','佐藤飲料','P003',24,120,str(today+timedelta(days=240))),
        (pid['4909876543211'],'4909876543211','麦茶 500ml','SUP002','佐藤飲料','P004',24,48, str(today+timedelta(days=300))),
    ])
    import random, calendar
    random.seed(42)
    last_year = today.year - 1
    pnames = {'4901234567890':'りんごジュース 1L','4901234567891':'オレンジジュース 1L',
              '4909876543210':'緑茶 500ml','4909876543211':'麦茶 500ml'}
    bases  = {'4901234567890':30,'4901234567891':20,'4909876543210':60,'4909876543211':40}
    seasons = [0.8,0.8,0.9,1.0,1.2,1.5,1.8,1.8,1.3,1.0,0.9,1.1]
    sales = []
    for jan in pnames:
        for month in range(1, 13):
            qty = int(bases[jan] * seasons[month-1] * random.uniform(0.8, 1.2))
            days_in_month = calendar.monthrange(last_year, month)[1]
            sale_date = f"{last_year}-{month:02d}-{random.randint(1,days_in_month):02d}"
            sales.append((jan, pnames[jan], qty, sale_date, 'sample'))
    db.executemany(
        "INSERT INTO sales_history (jan,product_name,quantity,sale_date,source_file) VALUES (%s,%s,%s,%s,%s)",
        sales)
    # 初期ユーザー（admin/admin123、user/user123）
    admin_exists = db.execute("SELECT COUNT(*) AS c FROM users").fetchone()['c']
    if not admin_exists:
        db.executemany("INSERT INTO users (username,password,role) VALUES (%s,%s,%s)", [
            ('admin', '240be518fabd2724ddb6f04eeb1da5967448d7e831c08c8fa822809f74c720a9', 'admin'),
            ('user',  'e606e38b0d8c19b24cf0ee3808183162ea7cd63ff7912dbb22b5e803286b4446',  'user'),
        ])
    db.executemany("INSERT INTO mail_recipients (name,email,send_type) VALUES (%s,%s,%s)", [
        ('管理者','manager@example.com','both'),
        ('倉庫担当','warehouse@example.com','both'),
    ])
    db.commit()
    logger.info("OK: Sample data created.")