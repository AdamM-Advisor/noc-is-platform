import logging
import threading
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_full_app = None
_ready = False
_boot_started = False
_lock = threading.Lock()


async def _respond(status, body_bytes, receive, send):
    while True:
        msg = await receive()
        if msg.get("type") == "http.request":
            if not msg.get("more_body", False):
                break
        else:
            break
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": [
            [b"content-type", b"text/plain; charset=utf-8"],
            [b"content-length", str(len(body_bytes)).encode()],
        ],
    })
    await send({
        "type": "http.response.body",
        "body": body_bytes,
    })


def _maybe_start_boot():
    global _boot_started
    with _lock:
        if not _boot_started:
            _boot_started = True
            t = threading.Thread(target=_boot, daemon=True)
            t.start()


async def app(scope, receive, send):
    if scope["type"] == "lifespan":
        msg = await receive()
        if msg["type"] == "lifespan.startup":
            await send({"type": "lifespan.startup.complete"})
            threading.Timer(2.0, _maybe_start_boot).start()
            msg = await receive()
            if msg["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
        return

    if scope["type"] != "http":
        return

    path = scope.get("path", "/")

    if _ready and _full_app is not None:
        await _full_app(scope, receive, send)
        return

    if path == "/" or path == "/healthz":
        await _respond(200, b"ok", receive, send)
    else:
        await _respond(503, b"starting", receive, send)


def _boot():
    global _full_app, _ready
    try:
        from backend._app_factory import create_full_app
        _full_app = create_full_app()
        _ready = True
        logger.info("Full application ready")
    except Exception:
        logger.error("Boot failed", exc_info=True)
        _ready = True
