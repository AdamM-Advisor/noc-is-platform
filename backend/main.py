import time
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.database import init_database
from backend.routers import health, upload, admin
from backend.routers import schema, threshold
from backend.routers import imports, orphans, data
from backend.routers import hierarchy, site, sla_target, data_quality, external
from backend.routers import profiler
from backend.routers import gangguan

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="NOC-IS Analytics Platform", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.state.start_time = time.time()

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


@app.on_event("startup")
async def startup():
    init_database()
    from backend.services.schema_service import initialize_schema, get_schema_status
    status = get_schema_status()
    if not status["initialized"]:
        result = initialize_schema()
        logger.info(f"Schema initialized: {len(result['tables_created'])} tables created")
    else:
        logger.info("Schema already initialized")
        from backend.services.calendar_service import seed_calendar_if_empty
        seed_calendar_if_empty()
