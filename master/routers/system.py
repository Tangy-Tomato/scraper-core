from fastapi import APIRouter, Depends, HTTPException
from psycopg import Connection

from master.database import get_db_conn
from master.routers.auth import require_worker_auth
from master.services.task_service import get_target_version, heartbeat_worker
from shared.contracts import HeartbeatRequest, HeartbeatResponse


router = APIRouter()


@router.post(
    "/heartbeat",
    response_model=HeartbeatResponse,
    dependencies=[Depends(require_worker_auth)],
)
def heartbeat(
    payload: HeartbeatRequest, conn: Connection = Depends(get_db_conn)
) -> HeartbeatResponse:
    heartbeat_worker(conn, payload.worker_id, payload.version, payload.hostname)
    target_version = get_target_version(conn)
    return HeartbeatResponse(
        target_version=target_version,
        update_required=payload.version != target_version,
    )


@router.get("/health")
def health(conn: Connection = Depends(get_db_conn)) -> dict[str, str]:
    try:
        conn.execute("SELECT 1")
    except Exception as exc:
        raise HTTPException(status_code=503, detail="database unavailable") from exc
    return {"status": "ok"}
