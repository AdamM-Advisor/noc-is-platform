import socket
import os
import sys
import threading
import time

PORT = int(os.environ.get("PORT", "5000"))

HTTP_200 = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Type: text/plain\r\n"
    b"Content-Length: 2\r\n"
    b"Connection: close\r\n"
    b"\r\n"
    b"ok"
)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(("0.0.0.0", PORT))
sock.listen(128)
sock.setblocking(True)
os.set_inheritable(sock.fileno(), True)

print(f"[start.py] Socket listening on :{PORT}", flush=True)

_stop_health = False


def _health_accept():
    sock.settimeout(0.5)
    while not _stop_health:
        try:
            conn, addr = sock.accept()
            try:
                conn.settimeout(3)
                conn.recv(4096)
                conn.sendall(HTTP_200)
            except Exception:
                pass
            finally:
                conn.close()
        except socket.timeout:
            continue
        except OSError:
            break


health_thread = threading.Thread(target=_health_accept, daemon=True)
health_thread.start()
print("[start.py] Health responder active", flush=True)

import importlib
uvicorn = importlib.import_module("uvicorn")
from backend.main import app as application

_stop_health = True
health_thread.join(timeout=2)

print("[start.py] Starting uvicorn on pre-bound socket", flush=True)

config = uvicorn.Config(
    app=application,
    host="0.0.0.0",
    port=PORT,
    fd=sock.fileno(),
)
server = uvicorn.Server(config)
server.run()
