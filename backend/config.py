import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

IS_PRODUCTION = os.environ.get("REPL_DEPLOYMENT") == "1" or os.environ.get("REPLIT_DEPLOYMENT") == "1"

if IS_PRODUCTION:
    DATA_DIR = os.path.join(os.path.expanduser("~"), "noc_data")
    UPLOAD_DIR = os.path.join(os.path.expanduser("~"), "noc_uploads")
else:
    DATA_DIR = os.path.join(BASE_DIR, ".data")
    UPLOAD_DIR = os.path.join(BASE_DIR, ".uploads")

DB_PATH = os.path.join(DATA_DIR, "noc_analytics.duckdb")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
CHUNK_DIR = os.path.join(BASE_DIR, "temp_chunks")
EXPORT_DIR = os.path.join(BASE_DIR, "exports")

for d in [DATA_DIR, BACKUP_DIR, UPLOAD_DIR, CHUNK_DIR, EXPORT_DIR]:
    os.makedirs(d, exist_ok=True)

DUCKDB_MEMORY_LIMIT = "512MB"
DUCKDB_THREADS = 2
CHUNK_SIZE_MB = 5
MAX_BACKUP_COUNT = 3
SINGLE_UPLOAD_LIMIT_MB = 10
