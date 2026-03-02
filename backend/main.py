import time
import logging
import threading
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import PlainTextResponse, JSONResponse, FileResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FRONTEND_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"

AUTH_EXEMPT_PATHS = {
    "/", "/healthz", "/api/auth/login", "/api/auth/verify-2fa",
    "/api/auth/me", "/api/auth/logout", "/api/health",
}

_db_initialized = False
_db_lock = threading.Lock()


def _ensure_db():
    global _db_initialized
    if _db_initialized:
        return
    with _db_lock:
        if _db_initialized:
            return
        try:
            from backend.database import init_database
            init_database()
            from backend.services.schema_service import initialize_schema, get_schema_status
            from backend.services.schema_service import _migrate_saved_views
            from backend.database import get_write_connection
            with get_write_connection() as wconn:
                _migrate_saved_views(wconn)
            status = get_schema_status()
            if not status["initialized"]:
                result = initialize_schema()
                logger.info(f"Schema init: {len(result['tables_created'])} tables")
            else:
                logger.info("Schema already initialized")
                from backend.services.calendar_service import seed_calendar_if_empty
                seed_calendar_if_empty()
            logger.info("DB initialization complete")
        except Exception:
            logger.error("DB init error", exc_info=True)
        _db_initialized = True


def _delayed_db_init():
    time.sleep(10)
    _ensure_db()


@asynccontextmanager
async def lifespan(a):
    logger.info("Lifespan startup")
    threading.Thread(target=_delayed_db_init, daemon=True).start()
    yield


app = FastAPI(title="NOC-IS Analytics Platform", version="1.0.0", lifespan=lifespan)
app.state.start_time = time.time()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if path.startswith("/api/") and path not in AUTH_EXEMPT_PATHS:
        from backend.services.auth_service import validate_session
        token = request.cookies.get("nocis_session")
        if not token or not validate_session(token):
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    return await call_next(request)


@app.get("/")
async def root():
    index = FRONTEND_DIST / "index.html"
    if index.is_file():
        return FileResponse(str(index))
    return PlainTextResponse("ok")


@app.get("/healthz")
async def healthz():
    return PlainTextResponse("ok")


from backend.routers import health, upload, admin
from backend.routers import schema, threshold
from backend.routers import imports, orphans, data
from backend.routers import hierarchy, site, sla_target, data_quality, external
from backend.routers import profiler, gangguan, predictive
from backend.routers import dashboard, report_card
from backend.routers import saved_views, comparison
from backend.routers import reports, ndc, auth

app.include_router(auth.router, prefix="/api")
app.include_router(health.router, prefix="/api")
app.include_router(upload.router, prefix="/api")
app.include_router(admin.router, prefix="/api")
app.include_router(schema.router, prefix="/api")
app.include_router(threshold.router, prefix="/api")
app.include_router(imports.router, prefix="/api")
app.include_router(orphans.router, prefix="/api")
app.include_router(data.router, prefix="/api")
app.include_router(hierarchy.router, prefix="/api")
app.include_router(site.router, prefix="/api")
app.include_router(sla_target.router, prefix="/api")
app.include_router(data_quality.router, prefix="/api")
app.include_router(external.router, prefix="/api")
app.include_router(profiler.router, prefix="/api")
app.include_router(gangguan.router, prefix="/api")
app.include_router(predictive.router, prefix="/api")
app.include_router(dashboard.router, prefix="/api")
app.include_router(report_card.router, prefix="/api")
app.include_router(saved_views.router, prefix="/api")
app.include_router(comparison.router, prefix="/api")
app.include_router(reports.router, prefix="/api")
app.include_router(ndc.router, prefix="/api")

if FRONTEND_DIST.is_dir() and (FRONTEND_DIST / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST / "assets")), name="static-assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        file_path = FRONTEND_DIST / full_path
        if full_path and file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(FRONTEND_DIST / "index.html"))
