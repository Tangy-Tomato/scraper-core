from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import Response

from master.config import CORS_ALLOW_ORIGINS, load_settings
from master.database import create_pool
from master.routers import system, tasks


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    pool = create_pool(
        settings.database_url,
        settings.database_pool_min,
        settings.database_pool_max,
    )
    try:
        pool.open()
        pool.wait()
        settings.raw_html_dir.mkdir(parents=True, exist_ok=True)
        app.state.settings = settings
        app.state.db_pool = pool
        yield
    finally:
        pool.close()


app = FastAPI(title="GMaps Extractor Master API", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(CORS_ALLOW_ORIGINS),
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type", "Content-Encoding"],
)
app.include_router(system.router, prefix="/api/v1")
app.include_router(tasks.router, prefix="/api/v1/tasks")


@app.middleware("http")
async def enforce_request_size(
    request: Request, call_next: RequestResponseEndpoint
) -> Response:
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            request_size = int(content_length)
        except ValueError:
            return JSONResponse(
                status_code=400, content={"detail": "invalid Content-Length"}
            )
        if request_size < 0 or request_size > request.app.state.settings.max_upload_bytes:
            return JSONResponse(
                status_code=413, content={"detail": "request payload exceeds size limit"}
            )
    return await call_next(request)
