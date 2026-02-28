import threading
import duckdb
from contextlib import contextmanager
from backend.config import DB_PATH, DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS

_write_lock = threading.Lock()


def _configure_connection(conn):
    conn.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    conn.execute(f"SET threads={DUCKDB_THREADS}")
    return conn


@contextmanager
def get_connection():
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        _configure_connection(conn)
        yield conn
    finally:
        conn.close()


@contextmanager
def get_write_connection():
    _write_lock.acquire()
    conn = duckdb.connect(DB_PATH, read_only=False)
    try:
        _configure_connection(conn)
        yield conn
    finally:
        conn.close()
        _write_lock.release()


def init_database():
    with get_write_connection() as conn:
        conn.execute("SELECT 1")
