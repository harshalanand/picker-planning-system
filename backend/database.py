import sqlite3, os
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "picker_planning.db")

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def get_db():
    return get_connection()

def init_db():
    conn = get_connection()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS plans (
        token       TEXT PRIMARY KEY,
        plan_date   TEXT NOT NULL,
        run_number  INTEGER NOT NULL DEFAULT 1,
        created_at  TEXT,
        notes       TEXT DEFAULT '',
        config_json TEXT,
        demand_json TEXT,
        total_dos   INTEGER DEFAULT 0,
        total_qty   INTEGER DEFAULT 0,
        pickers_used INTEGER DEFAULT 0,
        avg_util    REAL DEFAULT 0,
        skipped_dos INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS plan_details (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        token        TEXT NOT NULL,
        plan_date    TEXT,
        run_number   INTEGER,
        priority     INTEGER,
        do_no        TEXT,
        sto_no       TEXT DEFAULT '',
        st_cd        TEXT DEFAULT '',
        st_nm        TEXT DEFAULT '',
        floor        INTEGER,
        sec          TEXT DEFAULT '',
        do_qty       INTEGER,
        picker_no    INTEGER,
        machine_no   TEXT,
        scanner_name TEXT DEFAULT '',
        grp          TEXT DEFAULT 'G1',
        bgt_machine  INTEGER DEFAULT 3000,
        start_time   TEXT,
        end_time     TEXT,
        duration_min REAL,
        pcs_per_min  REAL,
        cap_used     INTEGER,
        util_pct     REAL,
        remaining    INTEGER,
        over_wh      INTEGER DEFAULT 0,
        avail_min    REAL DEFAULT 0,
        status       TEXT DEFAULT 'Planned',
        cancel_reason TEXT DEFAULT '',
        cancelled_at  TEXT DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS picker_day_state (
        plan_date  TEXT NOT NULL,
        machine_no TEXT NOT NULL,
        floor      INTEGER NOT NULL,
        cap_used   INTEGER DEFAULT 0,
        avail_min  REAL DEFAULT 0,
        last_token TEXT,
        PRIMARY KEY (plan_date, machine_no, floor)
    );
    CREATE TABLE IF NOT EXISTS actual_times (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        token        TEXT NOT NULL,
        do_no        TEXT NOT NULL,
        plan_date    TEXT DEFAULT '',
        actual_date  TEXT DEFAULT '',
        actual_start TEXT DEFAULT '',
        actual_end   TEXT DEFAULT '',
        actual_qty   INTEGER DEFAULT 0,
        notes        TEXT DEFAULT '',
        entered_at   TEXT,
        UNIQUE(token, do_no)
    );
    CREATE INDEX IF NOT EXISTS idx_pd_token    ON plan_details(token);
    CREATE INDEX IF NOT EXISTS idx_pd_date     ON plan_details(plan_date);
    CREATE INDEX IF NOT EXISTS idx_pd_status   ON plan_details(status);
    CREATE INDEX IF NOT EXISTS idx_act_token   ON actual_times(token);
    CREATE INDEX IF NOT EXISTS idx_pds_date    ON picker_day_state(plan_date);
    """)
    conn.commit()
    conn.close()

def migrate_db():
    conn = get_connection()
    existing = {r[1] for r in conn.execute("PRAGMA table_info(plan_details)").fetchall()}
    migrations = [
        ("status",        "ALTER TABLE plan_details ADD COLUMN status TEXT DEFAULT 'Planned'"),
        ("cancel_reason", "ALTER TABLE plan_details ADD COLUMN cancel_reason TEXT DEFAULT ''"),
        ("cancelled_at",  "ALTER TABLE plan_details ADD COLUMN cancelled_at TEXT DEFAULT ''"),
    ]
    for col, sql in migrations:
        if col not in existing:
            conn.execute(sql)
    # actual_times
    at_existing = {r[1] for r in conn.execute("PRAGMA table_info(actual_times)").fetchall()}
    at_migrations = [
        ("plan_date",   "ALTER TABLE actual_times ADD COLUMN plan_date TEXT DEFAULT ''"),
        ("actual_date", "ALTER TABLE actual_times ADD COLUMN actual_date TEXT DEFAULT ''"),
        ("actual_qty",  "ALTER TABLE actual_times ADD COLUMN actual_qty INTEGER DEFAULT 0"),
        ("notes",       "ALTER TABLE actual_times ADD COLUMN notes TEXT DEFAULT ''"),
    ]
    for col, sql in at_migrations:
        if col not in at_existing:
            conn.execute(sql)
    conn.commit()
    conn.close()
