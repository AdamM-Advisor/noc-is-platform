import os
import shutil
from backend.config import UPLOAD_DIR, CHUNK_DIR


def save_upload(filename, content):
    safe_name = os.path.basename(filename)
    filepath = os.path.join(UPLOAD_DIR, safe_name)

    counter = 1
    base, ext = os.path.splitext(safe_name)
    while os.path.exists(filepath):
        safe_name = f"{base}_{counter}{ext}"
        filepath = os.path.join(UPLOAD_DIR, safe_name)
        counter += 1

    with open(filepath, "wb") as f:
        f.write(content)

    size_mb = round(len(content) / (1024 * 1024), 2)
    return {
        "filename": safe_name,
        "size": size_mb,
        "path": filepath,
    }


def save_chunk(upload_id, chunk_index, total_chunks, data):
    chunk_dir = os.path.join(CHUNK_DIR, upload_id)
    os.makedirs(chunk_dir, exist_ok=True)

    chunk_path = os.path.join(chunk_dir, f"chunk_{chunk_index}")
    with open(chunk_path, "wb") as f:
        f.write(data)

    import json
    meta_path = os.path.join(chunk_dir, "_meta.json")
    with open(meta_path, "w") as f:
        json.dump({"total_chunks": total_chunks}, f)

    received = len([
        name for name in os.listdir(chunk_dir) if name.startswith("chunk_")
    ])

    return {
        "chunk_index": chunk_index,
        "received_chunks": received,
        "total_chunks": total_chunks,
    }


def assemble_chunks(upload_id, filename, total_chunks):
    chunk_dir = os.path.join(CHUNK_DIR, upload_id)

    if not os.path.exists(chunk_dir):
        raise FileNotFoundError(f"No chunks found for upload {upload_id}")

    safe_name = os.path.basename(filename)
    filepath = os.path.join(UPLOAD_DIR, safe_name)

    counter = 1
    base, ext = os.path.splitext(safe_name)
    while os.path.exists(filepath):
        safe_name = f"{base}_{counter}{ext}"
        filepath = os.path.join(UPLOAD_DIR, safe_name)
        counter += 1

    with open(filepath, "wb") as f:
        for i in range(total_chunks):
            chunk_path = os.path.join(chunk_dir, f"chunk_{i}")
            if not os.path.exists(chunk_path):
                raise FileNotFoundError(f"Missing chunk {i}")
            with open(chunk_path, "rb") as chunk_file:
                f.write(chunk_file.read())

    shutil.rmtree(chunk_dir, ignore_errors=True)

    size_mb = round(os.path.getsize(filepath) / (1024 * 1024), 2)
    return {
        "filename": safe_name,
        "size": size_mb,
        "path": filepath,
    }


def get_chunk_status(upload_id):
    chunk_dir = os.path.join(CHUNK_DIR, upload_id)

    if not os.path.exists(chunk_dir):
        return {
            "received_chunks": 0,
            "total_chunks": 0,
            "is_complete": False,
        }

    import json
    total_chunks = 0
    meta_path = os.path.join(chunk_dir, "_meta.json")
    if os.path.exists(meta_path):
        with open(meta_path, "r") as f:
            meta = json.load(f)
            total_chunks = meta.get("total_chunks", 0)

    chunks = [name for name in os.listdir(chunk_dir) if name.startswith("chunk_")]
    received = len(chunks)

    return {
        "received_chunks": received,
        "total_chunks": total_chunks,
        "is_complete": received >= total_chunks and total_chunks > 0,
    }
