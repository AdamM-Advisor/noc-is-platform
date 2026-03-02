import time
import logging
import threading
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FRONTEND_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"
_app_ready = False
_full_app = None


async def health_app(scope, receive, send):
    if scope["type"] == "lifespan":
        message = await receive()
        if message["type"] == "lifespan.startup":
            t = threading.Thread(target=_boot_full_app, daemon=True)
            t.start()
            await send({"type": "lifespan.startup.complete"})
            message = await receive()
            if message["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
        return

    if scope["type"] != "http":
        return

    global _app_ready, _full_app
    if _app_ready and _full_app is not None:
        await _full_app(scope, receive, send)
        return

    path = scope.get("path", "/")
    if path == "/" or path == "/healthz":
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [[b"content-type", b"text/plain"]],
        })
        await send({
            "type": "http.response.body",
            "body": b"ok",
        })
    else:
        await send({
            "type": "http.response.start",
            "status": 503,
            "headers": [[b"content-type", b"text/plain"]],
        })
        await send({
            "type": "http.response.body",
            "body": b"starting",
        })


def _boot_full_app():
    global _app_ready, _full_app
    try:
        from backend.app import create_app
        _full_app = create_app()
        _app_ready = True
        logger.info("Full application ready")
    except Exception as e:
        logger.error(f"Failed to boot full app: {e}")


app = health_app
