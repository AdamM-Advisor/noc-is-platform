import socket
import os
import sys
import signal
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
os.set_inheritable(sock.fileno(), True)

sys.stdout.write(f"[start.py] Socket bound to :{PORT}\n")
sys.stdout.flush()

child_pid = os.fork()

if child_pid == 0:
    signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
    signal.signal(signal.SIGINT, lambda *_: os._exit(0))
    while True:
        try:
            conn, _ = sock.accept()
            try:
                conn.settimeout(3)
                conn.recv(4096)
                conn.sendall(HTTP_200)
            except Exception:
                pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception:
            break
    os._exit(0)

sys.stdout.write(f"[start.py] Health child pid={child_pid}\n")
sys.stdout.flush()

import importlib
uvicorn = importlib.import_module("uvicorn")
from backend.main import app as application

sys.stdout.write("[start.py] Imports done, stopping health child\n")
sys.stdout.flush()

os.kill(child_pid, signal.SIGTERM)
try:
    os.waitpid(child_pid, 0)
except ChildProcessError:
    pass

time.sleep(0.2)

config = uvicorn.Config(
    app=application,
    host="0.0.0.0",
    port=PORT,
    fd=sock.fileno(),
)
server = uvicorn.Server(config)

def _fwd(signum, frame):
    server.should_exit = True

signal.signal(signal.SIGTERM, _fwd)
signal.signal(signal.SIGINT, _fwd)

server.run()
