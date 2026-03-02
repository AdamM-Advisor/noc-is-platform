import logging
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_full_app = None
_ready = False
_boot_started = False
_lock = threading.Lock()


def _send_text(status, body):
    body_bytes = body.encode("utf-8") if isinstance(body, str) else body

    async def responder(scope, receive, send):
        await send({
            "type": "http.response.start",
            "status": status,
            "headers": [
                [b"content-type", b"text/plain"],
                [b"content-length", str(len(body_bytes)).encode()],
            ],
        })
        await send({
            "type": "http.response.body",
            "body": body_bytes,
        })

    return responder


async def app(scope, receive, send):
    global _boot_started

    if scope["type"] == "lifespan":
        msg = await receive()
        if msg["type"] == "lifespan.startup":
            with _lock:
                if not _boot_started:
                    _boot_started = True
                    t = threading.Thread(target=_boot, daemon=True)
                    t.start()
            await send({"type": "lifespan.startup.complete"})
            msg = await receive()
            if msg["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
        return

    if scope["type"] != "http":
        return

    global _full_app, _ready
    path = scope.get("path", "/")

    if path == "/" or path == "/healthz":
        if _ready and _full_app is not None:
            await _full_app(scope, receive, send)
        else:
            await _send_text(200, "ok")(scope, receive, send)
        return

    if _ready and _full_app is not None:
        await _full_app(scope, receive, send)
    else:
        await _send_text(503, "starting")(scope, receive, send)


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
