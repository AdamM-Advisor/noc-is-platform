import time
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
    max_retries = 5
    for attempt in range(max_retries):
        try:
            conn = duckdb.connect(DB_PATH, read_only=False)
            break
        except duckdb.ConnectionException:
            if attempt < max_retries - 1:
                time.sleep(0.3 * (attempt + 1))
            else:
                raise
    try:
        _configure_connection(conn)
        yield conn
    finally:
        conn.close()


@contextmanager
def get_write_connection():
    _write_lock.acquire()
    try:
        conn = duckdb.connect(DB_PATH, read_only=False)
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
