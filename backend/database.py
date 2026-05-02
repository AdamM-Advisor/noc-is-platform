import time
import threading
import duckdb
import os
from contextlib import contextmanager
from backend.config import DB_PATH, DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS

_write_lock = threading.Lock()
_CONNECT_RETRIES = int(os.environ.get("NOCIS_DB_CONNECT_RETRIES", "40"))
_CONNECT_BASE_SLEEP_SEC = float(os.environ.get("NOCIS_DB_CONNECT_BASE_SLEEP_SEC", "0.25"))
_LOCK_ERROR_MARKERS = (
    "used by another process",
    "conflicting lock",
    "database is locked",
    "could not set lock",
    "access is denied",
)


def _configure_connection(conn):
    conn.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    conn.execute(f"SET threads={DUCKDB_THREADS}")
    return conn


def _is_transient_lock_error(exc):
    message = str(exc).lower()
    return any(marker in message for marker in _LOCK_ERROR_MARKERS)


def _connect_database(read_only=False):
    last_exc = None
    for attempt in range(_CONNECT_RETRIES):
        try:
            return duckdb.connect(DB_PATH, read_only=read_only)
        except Exception as exc:
            last_exc = exc
            if not _is_transient_lock_error(exc) or attempt >= _CONNECT_RETRIES - 1:
                raise
            delay = min(5.0, _CONNECT_BASE_SLEEP_SEC * (attempt + 1))
            time.sleep(delay)
    raise last_exc


@contextmanager
def get_connection():
    conn = _connect_database(read_only=False)
    try:
        _configure_connection(conn)
        yield conn
    finally:
        conn.close()


@contextmanager
def get_write_connection():
    _write_lock.acquire()
    try:
        conn = _connect_database(read_only=False)
        try:
            _configure_connection(conn)
            yield conn
        finally:
            conn.close()
    finally:
        _write_lock.release()


def init_database():
    with get_write_connection() as conn:
        conn.execute("SELECT 1")
