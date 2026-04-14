import os
import shutil
import duckdb
from pathlib import Path

# Database through MotherDuck (commented out for backwards compatibility during local development)
# con = duckdb.connect('md:mdb_timestock', config={"motherduck_token": MOTHERDUCK_TOKEN})

DB_NAME = "rdb_timestock_3"
REPO_DB_PATH = Path("backend") / DB_NAME

# For local development, we will use a file-based DuckDB. In production (Railway), we will use the mounted volume.
if os.environ.get("RAILWAY") == "1":
    DB_PATH = Path("/data") / DB_NAME
else:
    DB_PATH = Path("backend") / DB_NAME

# Ensure directory exists
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Copy starter DB if it doesn't exist yet
if not DB_PATH.exists():
    if REPO_DB_PATH.exists():
        shutil.copy(REPO_DB_PATH, DB_PATH)
        print(f"Copied starter DB to {DB_PATH}")
    else:
        print(f"No starter DB found at {REPO_DB_PATH}. A new DB will be created.")

# Connect to DuckDB
con = duckdb.connect(str(DB_PATH))
def execute(query, *args):
    global con
    try:
        return con.execute(query, *args)
    except Exception:
        # reconnect if broken
        con = duckdb.connect(str(DB_PATH))
        return con.execute(query, *args)
print(f"Connected to DB at {DB_PATH}")