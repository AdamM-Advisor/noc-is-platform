import time
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.database import init_database
from backend.routers import health, upload, admin

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


@app.on_event("startup")
async def startup():
    init_database()
