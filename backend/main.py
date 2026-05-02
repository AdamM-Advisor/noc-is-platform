import logging
import threading
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FRONTEND_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"
_full_app = None
_lock = threading.Lock()
_boot_started = False

OK_BODY = b"ok"
OK_HEADERS = [
    [b"content-type", b"text/plain; charset=utf-8"],
    [b"content-length", b"2"],
]
STARTING_BODY = b"starting"
STARTING_HEADERS = [
    [b"content-type", b"text/plain; charset=utf-8"],
    [b"content-length", b"8"],
]


async def app(scope, receive, send):
    global _boot_started

    if scope["type"] == "lifespan":
        msg = await receive()
        if msg["type"] == "lifespan.startup":
            with _lock:
                if not _boot_started:
                    _boot_started = True
                    threading.Thread(target=_boot, daemon=True).start()
            await send({"type": "lifespan.startup.complete"})
        while True:
            msg = await receive()
            if msg.get("type") == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return
        return

    if scope["type"] != "http":
        return

    path = scope.get("path", "/")

    if _full_app is not None:
        await _full_app(scope, receive, send)
        return

    if path == "/" or path == "/healthz":
        body = await receive()
        await send({"type": "http.response.start", "status": 200, "headers": OK_HEADERS})
        await send({"type": "http.response.body", "body": OK_BODY})
    else:
        body = await receive()
        await send({"type": "http.response.start", "status": 503, "headers": STARTING_HEADERS})
        await send({"type": "http.response.body", "body": STARTING_BODY})


def _boot():
    global _full_app
    try:
        import time
        from fastapi import FastAPI, Request
        from fastapi.middleware.cors import CORSMiddleware
        from fastapi.responses import PlainTextResponse, JSONResponse, FileResponse
        from fastapi.staticfiles import StaticFiles
        from contextlib import asynccontextmanager
        from backend.config import CORS_ALLOW_ORIGINS

        AUTH_EXEMPT_PATHS = {
            "/", "/healthz", "/api/auth/login", "/api/auth/verify-2fa",
            "/api/auth/me", "/api/auth/logout", "/api/health",
        }

        @asynccontextmanager
        async def noop_lifespan(a):
            yield

        full = FastAPI(title="NOC-IS Analytics Platform", version="1.0.0", lifespan=noop_lifespan)
        full.state.start_time = time.time()

        full.add_middleware(
            CORSMiddleware,
            allow_origins=CORS_ALLOW_ORIGINS or [
                "http://localhost:5000",
                "http://127.0.0.1:5000",
                "http://localhost:5173",
                "http://127.0.0.1:5173",
            ],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        @full.middleware("http")
        async def auth_mw(request: Request, call_next):
            p = request.url.path
            if request.method == "OPTIONS":
                return await call_next(request)
            if p.startswith("/api/") and p not in AUTH_EXEMPT_PATHS:
                from backend.services.auth_service import validate_session
                token = request.cookies.get("nocis_session")
                if not token or not validate_session(token):
                    return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
            return await call_next(request)

        @full.get("/")
        async def root():
            index = FRONTEND_DIST / "index.html"
            if index.is_file():
                return FileResponse(str(index))
            return PlainTextResponse("ok")

        @full.get("/healthz")
        async def healthz():
            return PlainTextResponse("ok")

        from backend.routers import health, upload, admin
        from backend.routers import schema, threshold
        from backend.routers import imports, orphans, data
        from backend.routers import hierarchy, site, sla_target, data_quality, external
        from backend.routers import profiler, gangguan, predictive
        from backend.routers import dashboard, report_card
        from backend.routers import saved_views, comparison
        from backend.routers import reports, ndc, auth, ops

        full.include_router(auth.router, prefix="/api")
        full.include_router(health.router, prefix="/api")
        full.include_router(upload.router, prefix="/api")
        full.include_router(admin.router, prefix="/api")
        full.include_router(schema.router, prefix="/api")
        full.include_router(threshold.router, prefix="/api")
        full.include_router(imports.router, prefix="/api")
        full.include_router(orphans.router, prefix="/api")
        full.include_router(data.router, prefix="/api")
        full.include_router(hierarchy.router, prefix="/api")
        full.include_router(site.router, prefix="/api")
        full.include_router(sla_target.router, prefix="/api")
        full.include_router(data_quality.router, prefix="/api")
        full.include_router(external.router, prefix="/api")
        full.include_router(profiler.router, prefix="/api")
        full.include_router(gangguan.router, prefix="/api")
        full.include_router(predictive.router, prefix="/api")
        full.include_router(dashboard.router, prefix="/api")
        full.include_router(report_card.router, prefix="/api")
        full.include_router(saved_views.router, prefix="/api")
        full.include_router(comparison.router, prefix="/api")
        full.include_router(reports.router, prefix="/api")
        full.include_router(ndc.router, prefix="/api")
        full.include_router(ops.router, prefix="/api")

        if FRONTEND_DIST.is_dir() and (FRONTEND_DIST / "assets").is_dir():
            full.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST / "assets")), name="static-assets")

            @full.get("/{full_path:path}")
            async def serve_spa(full_path: str):
                fp = FRONTEND_DIST / full_path
                if full_path and fp.is_file():
                    return FileResponse(str(fp))
                return FileResponse(str(FRONTEND_DIST / "index.html"))

        logger.info("Routers registered")

        from backend.database import init_database, get_write_connection
        from backend.services.schema_service import initialize_schema, get_schema_status, _migrate_saved_views
        from backend.services.backup_service import create_backup, list_backups, restore_backup
        from backend.services.calendar_service import seed_calendar_if_empty
        from backend.config import BACKUP_DIR
        import os

        init_database()

        backups = list_backups()
        status = get_schema_status()

        if not status["initialized"] and backups:
            latest = backups[0]["name"]
            logger.info(f"Database empty, restoring from backup: {latest}")
            restore_backup(latest)
            status = get_schema_status()
            logger.info(f"Restore complete, schema initialized: {status['initialized']}")

        with get_write_connection() as wconn:
            _migrate_saved_views(wconn)

        from backend.services.operational_catalog_service import initialize_operational_catalog
        initialize_operational_catalog()

        status = get_schema_status()
        if not status["initialized"]:
            result = initialize_schema()
            logger.info(f"Schema init: {len(result['tables_created'])} tables")
        else:
            logger.info("Schema already initialized")
            seed_calendar_if_empty()

        try:
            backup_result = create_backup()
            if backup_result.get("backup_path"):
                logger.info(f"Auto-backup created: {backup_result['backup_path']} ({backup_result['size_mb']} MB)")
            else:
                logger.info(f"Auto-backup skipped: {backup_result.get('message', 'unknown')}")
        except Exception as e:
            logger.warning(f"Auto-backup failed: {e}")

        _full_app = full
        logger.info("Full app ready")
    except Exception:
        logger.error("Boot failed", exc_info=True)
