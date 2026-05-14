import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "opta_tracker.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS predictions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    scraped_at      TEXT NOT NULL,
    match_date      TEXT NOT NULL,          -- YYYY-MM-DD
    match_time_utc  TEXT,
    comp            TEXT NOT NULL,          -- LL, EPL, MLS, etc.
    home            TEXT NOT NULL,          -- Opta abbreviation
    away            TEXT NOT NULL,
    prob_home       REAL NOT NULL,          -- Opta %
    prob_draw       REAL NOT NULL,
    prob_away       REAL NOT NULL,
    sofascore_id    INTEGER,
    apifootball_id  INTEGER,
    UNIQUE(match_date, home, away)
);

CREATE TABLE IF NOT EXISTS odds (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    prediction_id   INTEGER NOT NULL REFERENCES predictions(id),
    fetched_at      TEXT NOT NULL,
    odds_home       REAL,
    odds_draw       REAL,
    odds_away       REAL,
    impl_home       REAL,                   -- prob implícita normalizada
    impl_draw       REAL,
    impl_away       REAL,
    delta_home      REAL,                   -- Opta% - impl%
    delta_draw      REAL,
    delta_away      REAL
);

CREATE TABLE IF NOT EXISTS results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    prediction_id   INTEGER NOT NULL REFERENCES predictions(id) UNIQUE,
    home_score      INTEGER,
    away_score      INTEGER,
    outcome         TEXT,                   -- H / D / A
    updated_at      TEXT NOT NULL
);
"""

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        # Migrate: add apifootball_id to existing DBs
        try:
            conn.execute("ALTER TABLE predictions ADD COLUMN apifootball_id INTEGER")
            conn.commit()
        except Exception:
            pass  # column already exists
    print(f"DB ready: {DB_PATH}")

if __name__ == "__main__":
    init_db()
