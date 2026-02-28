import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "noc_analytics.duckdb")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
CHUNK_DIR = os.path.join(BASE_DIR, "temp_chunks")
EXPORT_DIR = os.path.join(BASE_DIR, "exports")

for d in [DATA_DIR, BACKUP_DIR, UPLOAD_DIR, CHUNK_DIR, EXPORT_DIR]:
    os.makedirs(d, exist_ok=True)

DUCKDB_MEMORY_LIMIT = "512MB"
DUCKDB_THREADS = 2
CHUNK_SIZE_MB = 5
MAX_BACKUP_COUNT = 3
SINGLE_UPLOAD_LIMIT_MB = 10
