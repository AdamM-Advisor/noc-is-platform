import os
import shutil
from datetime import datetime
from backend.config import DB_PATH, BACKUP_DIR, MAX_BACKUP_COUNT


def create_backup():
    if not os.path.exists(DB_PATH):
        return {
            "backup_path": None,
            "size_mb": 0,
            "timestamp": datetime.now().isoformat(),
            "total_backups": 0,
            "message": "Database file does not exist yet",
        }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"noc_analytics_{timestamp}.duckdb"
    backup_path = os.path.join(BACKUP_DIR, backup_filename)

    shutil.copy2(DB_PATH, backup_path)

    _cleanup_old_backups()

    size_mb = round(os.path.getsize(backup_path) / (1024 * 1024), 2)
    total_backups = len(_get_backup_files())

    return {
        "backup_path": backup_path,
        "size_mb": size_mb,
        "timestamp": timestamp,
        "total_backups": total_backups,
    }


def list_backups():
    backup_files = _get_backup_files()
    backups = []
    for f in backup_files:
        path = os.path.join(BACKUP_DIR, f)
        size_mb = round(os.path.getsize(path) / (1024 * 1024), 2)
        mod_time = os.path.getmtime(path)
        date = datetime.fromtimestamp(mod_time).isoformat()
        backups.append({"name": f, "size_mb": size_mb, "date": date})

    backups.sort(key=lambda x: x["date"], reverse=True)
    return backups


def restore_backup(backup_filename):
    backup_path = os.path.join(BACKUP_DIR, backup_filename)
    if not os.path.exists(backup_path):
        raise FileNotFoundError(f"Backup not found: {backup_filename}")

    shutil.copy2(backup_path, DB_PATH)

    from backend.database import init_database
    init_database()

    return {
        "restored_from": backup_filename,
        "timestamp": datetime.now().isoformat(),
    }


def _get_backup_files():
    if not os.path.exists(BACKUP_DIR):
        return []
    return sorted([
        f for f in os.listdir(BACKUP_DIR) if f.endswith(".duckdb")
    ])


def _cleanup_old_backups():
    backups = _get_backup_files()
    while len(backups) > MAX_BACKUP_COUNT:
        oldest = backups.pop(0)
        os.remove(os.path.join(BACKUP_DIR, oldest))
