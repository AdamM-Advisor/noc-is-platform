import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

IS_PRODUCTION = os.environ.get("REPL_DEPLOYMENT") == "1" or os.environ.get("REPLIT_DEPLOYMENT") == "1"
APP_ENV = os.environ.get("APP_ENV", "production" if IS_PRODUCTION else "development")

if IS_PRODUCTION:
    DATA_DIR = os.path.join(os.path.expanduser("~"), "noc_data")
    UPLOAD_DIR = os.path.join(os.path.expanduser("~"), "noc_uploads")
else:
    DATA_DIR = os.path.join(BASE_DIR, ".data")
    UPLOAD_DIR = os.path.join(BASE_DIR, ".uploads")

DATA_DIR = os.environ.get("NOCIS_DATA_DIR", DATA_DIR)
UPLOAD_DIR = os.environ.get("NOCIS_UPLOAD_DIR", UPLOAD_DIR)

DB_PATH = os.environ.get("NOCIS_DB_PATH", os.path.join(DATA_DIR, "noc_analytics.duckdb"))
RAW_DIR = os.environ.get("NOCIS_RAW_DIR", os.path.join(DATA_DIR, "raw"))
BACKUP_DIR = os.environ.get("NOCIS_BACKUP_DIR", os.path.join(DATA_DIR, "backups"))
CHUNK_DIR = os.environ.get("NOCIS_CHUNK_DIR", os.path.join(BASE_DIR, "temp_chunks"))
EXPORT_DIR = os.environ.get("NOCIS_EXPORT_DIR", os.path.join(BASE_DIR, "exports"))

for d in [DATA_DIR, RAW_DIR, BACKUP_DIR, UPLOAD_DIR, CHUNK_DIR, EXPORT_DIR]:
    os.makedirs(d, exist_ok=True)

DUCKDB_MEMORY_LIMIT = "512MB"
DUCKDB_THREADS = 2
CHUNK_SIZE_MB = 5
MAX_BACKUP_COUNT = 3
SINGLE_UPLOAD_LIMIT_MB = 10
UPLOAD_PIPELINE_MODE = os.environ.get("NOCIS_UPLOAD_PIPELINE_MODE", "parquet").strip().lower()

CORS_ALLOW_ORIGINS = [
    origin.strip()
    for origin in os.environ.get("CORS_ALLOW_ORIGINS", "").split(",")
    if origin.strip()
]

COOKIE_SECURE = os.environ.get("COOKIE_SECURE", "1" if APP_ENV == "production" else "0").lower() in ("1", "true", "yes")
COOKIE_SAMESITE = os.environ.get("COOKIE_SAMESITE", "none" if APP_ENV == "production" else "lax").lower()
SESSION_COOKIE_DOMAIN = os.environ.get("SESSION_COOKIE_DOMAIN", "").strip() or None
LOCAL_AUTH_SHOW_2FA_CODE = (
    os.environ.get("LOCAL_AUTH_SHOW_2FA_CODE", "1" if APP_ENV == "development" else "0").lower()
    in ("1", "true", "yes")
)
