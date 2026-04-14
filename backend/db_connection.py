import os
import sys
import shutil
import duckdb
from pathlib import Path

# Database through MotherDuck (commented out for backwards compatibility during local development)
# con = duckdb.connect('md:mdb_timestock', config={"motherduck_token": MOTHERDUCK_TOKEN})

DB_NAME = "timestock_database"
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

#For Electron packaging, we need to adjust the DB path handling to work with PyInstaller's resource bundling.
#The following code handles this:

# DB_NAME = "timestock_database"
# REPO_DB_PATH = Path("backend") / DB_NAME

# # Helper for PyInstaller paths
# def resource_path(relative_path):
#     if hasattr(sys, "_MEIPASS"):
#         return Path(sys._MEIPASS) / relative_path
#     return Path(__file__).resolve().parent.parent / relative_path

# # Detect if running as packaged exe
# def is_packaged():
#     return hasattr(sys, "_MEIPASS")

# print("DATABASE MODULE LOADED")
# print("is_packaged:", is_packaged())
# print("RAILWAY:", os.environ.get("RAILWAY"))

# # For local development, we will use a file-based DuckDB. In production (Railway), we will use the mounted volume.
# if is_packaged():
#     print("Using PACKAGED AppData database")
#     appdata_dir = Path(os.environ["APPDATA"]) / "timestock_desktop"
#     appdata_dir.mkdir(parents=True, exist_ok=True)

#     DB_PATH = appdata_dir / DB_NAME

#     # Copy starter DB from bundled resources on first run
#     bundled_db = resource_path(f"backend/{DB_NAME}")

#     if not DB_PATH.exists():
#         if bundled_db.exists():
#             shutil.copyfile(bundled_db, DB_PATH)
#             print(f"Copied starter DB to {DB_PATH}")
#         else:
#             print(f"No bundled starter DB found at {bundled_db}. A new DB will be created.")

# elif os.environ.get("RAILWAY") == "1":
#     print("Using RAILWAY database")
#     DB_PATH = Path("/data") / DB_NAME

# else:
#     print("Using LOCAL DEV database")
#     DB_PATH = Path("backend") / DB_NAME

# # Ensure directory exists
# DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# # Copy starter DB if it doesn't exist yet
# if not DB_PATH.exists() and not is_packaged():
#     if REPO_DB_PATH.exists():
#         shutil.copy(REPO_DB_PATH, DB_PATH)
#         print(f"Copied starter DB to {DB_PATH}")
#     else:
#         print(f"No starter DB found at {REPO_DB_PATH}. A new DB will be created.")

# # Connect to DuckDB
# con = duckdb.connect(str(DB_PATH))

# def execute(query, *args):
#     global con
#     try:
#         return con.execute(query, *args)
#     except Exception:
#         # reconnect if broken
#         con = duckdb.connect(str(DB_PATH))
#         return con.execute(query, *args)

# print(f"Connected to DB at {DB_PATH}")