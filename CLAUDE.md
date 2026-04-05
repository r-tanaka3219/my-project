# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the Application

```bash
# Windows (recommended)
start.bat         # Auto-discovers Python, installs deps, starts server

# Direct
python app.py
```

The app starts on `http://localhost:5000` (configurable via `PORT` in `.env`). It uses **Waitress** as the production server if available, otherwise Flask's dev server. On startup it runs DB connection warmup and pre-computes the forecast cache in a background thread.

## Dependencies

```bash
pip install -r requirements.txt
```

Key packages: Flask 3.x, psycopg2-binary, APScheduler, Flask-WTF, Flask-Limiter, numpy, scipy, openpyxl, waitress.

## Tests

```bash
python tests/test_system.py      # Integration tests (DB, auth, CSRF, rate limiting, CSV)
python tests/_perf_check.py      # Performance benchmarks
```

No linting tools are configured.

## Environment Configuration

Copy `.env.example` to `.env`. Required variables:

```env
SECRET_KEY=...
PORT=5000
PG_HOST=localhost
PG_PORT=5432
PG_DBNAME=inventory
PG_USER=inventory_user
PG_PASSWORD=...
```

Optional: `MAIL_SERVER`, `MAIL_PORT`, `MAIL_USERNAME`, `MAIL_PASSWORD`, `MAIL_FROM`, `WEATHER_LAT`, `WEATHER_LON`, `DAILY_MAIL_HOUR`, `MONTH_END_IMPORT_HOUR`.

## Database Initialization

```bash
python create_db.py     # Creates PostgreSQL user, database, and all 33+ tables
python _run_migrate.py  # Applies schema migrations
```

## Architecture Overview

### Entry Point: `app.py` (~4800 lines)

Monolithic main file that handles:
- Flask app factory and configuration
- All blueprint registration
- Forecast result caching (24-hour TTL, background refresh)
- `sales_daily_agg` warmup (pre-aggregated table used for performance-critical queries)
- APScheduler task registration (daily mail, month-end imports, expiry alerts)
- Request-scoped settings cache (`g._settings_cache`)

### Blueprints (`blueprints/`)

| Blueprint | URL Prefix | Purpose |
|-----------|-----------|---------|
| `auth.py` | `/` | Login, logout, password change |
| `dashboard.py` | `/` | Home/KPI dashboard |
| `inventory.py` | `/inventory` | Stock view, adjustments, transfers |
| `orders.py` | `/orders` | Order creation, history, mixed-load (混載) |
| `products.py` | `/products` | Product master CRUD, Excel import/export |
| `chains.py` | `/chains` | Chain/store master management |
| `forecast.py` | `/reports` | AI demand forecast, ABC analysis, 52-week MD plan, weather data |

Heavy business logic also lives directly in `app.py` (CSV import, reports, settings, admin, etc.).

### Database Layer (`database.py`, `db.py`)

PostgreSQL accessed through a **SQLite3-compatible wrapper**:
- All SQL uses `?` placeholders — the wrapper auto-converts them to `%s`
- `ThreadedConnectionPool` manages connection reuse
- Request-scoped access via `get_db()` → `g.db`; connection auto-returns to pool on teardown

```python
# Standard pattern in every route
db = get_db()
rows = db.execute("SELECT * FROM products WHERE jan=?", [jan]).fetchall()
```

### Authentication & Authorization (`auth_helpers.py`)

- **Passwords**: werkzeug PBKDF2-SHA256 / Scrypt; auto-upgrades legacy SHA-256 hashes on login
- **Decorators**: `@login_required`, `@admin_required`, `@permission_required('perm_name')`
- **Roles**: `admin` (full access) or `user` with comma-separated permission list stored in session
- **14 permissions**: `dashboard`, `inventory`, `receipt`, `orders`, `order_history`, `stocktake`, `reports`, `forecast`, `products`, `csv`, `chains`, `recipients`, `users`, `settings`

### Forecast & Performance Architecture

**`sales_daily_agg` table** — pre-aggregated daily sales per JAN, indexed on `(jan, sale_dt)`. All AI prediction and ABC analysis queries use this table instead of raw `sales_history` to avoid expensive sequential scans.

**Forecast cache** — computed result stored in `_fc_store` dict with 24-hour TTL (`_FC_TTL = 86400`). Background thread refreshes it without blocking requests. Sync with `_fc_event.wait(60)` on first cold load.

### Scheduled Tasks (`auto_check.py`, APScheduler)

- Daily order emails (default 08:00)
- Month-end CSV import (default 05:00 on 1st)
- Expiry date alerts
- Low-stock order point checks

CSV files are read from Windows network share paths configured per import setting. The system supports UTF-8, Shift-JIS, and CP932 encodings.

### Key Supporting Files

| File | Purpose |
|------|---------|
| `helpers.py` | Date normalization, JAN validation, CSV utilities |
| `mail_service.py` | Email queue and order notification templates |
| `extensions.py` | Flask-WTF CSRF and Flask-Limiter initialization |
| `wholesale_forecast.py` | Holt-Winters exponential smoothing, ABC/XYZ classification, temperature correlation |

### AI Forecast Algorithm (in `wholesale_forecast.py` + `blueprints/forecast.py`)

- **Method**: Holt-Winters exponential smoothing (α=0.3, β=0.1) for A-rank products; moving average for B/C
- **Safety stock**: Dynamic — `Z × σ_IQR × √L` where `σ_IQR = (Q75−Q25)/1.349`
- **Temperature correction**: Linear regression of daily temp vs. sales, applied as multiplier
- **ABC classification**: Past 365-day revenue from `sales_daily_agg`; XYZ by weekly CV
