import asyncio
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from psycopg import Connection

from master.database import get_db_conn
from master.routers.auth import require_worker_auth
from master.services import storage_service, task_service
from shared.contracts import (
    SubmissionAck,
    TaskAcquireRequest,
    TaskAcquireResponse,
    TaskEnqueueRequest,
    TaskEnqueueResponse,
    TaskFailureRequest,
)


router = APIRouter(dependencies=[Depends(require_worker_auth)])


def _get_task_status(pool, task_id: UUID) -> tuple[str, str | None]:
    with pool.connection() as conn:
        return task_service.get_task_status(conn, task_id)


def _mark_task_complete(pool, task_id: UUID, worker_id: str) -> bool:
    with pool.connection() as conn:
        return task_service.mark_complete(conn, task_id, worker_id)


@router.post("/enqueue", response_model=TaskEnqueueResponse)
def enqueue_tasks(
    payload: TaskEnqueueRequest, conn: Connection = Depends(get_db_conn)
) -> TaskEnqueueResponse:
    inserted, duplicates = task_service.enqueue_tasks(conn, payload.tasks)
    return TaskEnqueueResponse(inserted=inserted, duplicates=duplicates)


@router.post("/acquire", response_model=TaskAcquireResponse)
def acquire_task(
    payload: TaskAcquireRequest,
    request: Request,
    conn: Connection = Depends(get_db_conn),
) -> TaskAcquireResponse:
    task, lease_until = task_service.acquire_task(
        conn,
        payload.worker_id,
        payload.version,
        request.app.state.settings.task_lease_seconds,
    )
    return TaskAcquireResponse(
        task=task,
        lease_expires_at=lease_until.isoformat() if lease_until else None,
    )


@router.post("/{task_id}/submit", response_model=SubmissionAck)
async def submit_task(
    task_id: UUID,
    request: Request,
    worker_id: str,
) -> SubmissionAck:
    pool = request.app.state.db_pool
    try:
        current_status, leased_worker = await asyncio.to_thread(
            _get_task_status, pool, task_id
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if current_status == "COMPLETED":
        return SubmissionAck(task_id=str(task_id), status="COMPLETED", idempotent=True)
    if current_status != "PROCESSING" or leased_worker != worker_id:
        raise HTTPException(status_code=409, detail="task is not leased to this worker")
    if request.headers.get("content-encoding", "").lower() != "gzip":
        raise HTTPException(status_code=415, detail="Content-Encoding must be gzip")
    try:
        await storage_service.save_payload(
            request.stream(),
            task_id,
            request.app.state.settings.raw_html_dir,
            request.app.state.settings.max_upload_bytes,
            request.app.state.settings.max_uncompressed_html_bytes,
        )
        idempotent = await asyncio.to_thread(
            _mark_task_complete, pool, task_id, worker_id
        )
    except storage_service.UploadTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except storage_service.InvalidPayloadError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return SubmissionAck(
        task_id=str(task_id), status="COMPLETED", idempotent=idempotent
    )


@router.post("/{task_id}/fail", response_model=SubmissionAck)
def fail_task(
    task_id: UUID,
    payload: TaskFailureRequest,
    conn: Connection = Depends(get_db_conn),
) -> SubmissionAck:
    try:
        result = task_service.fail_task(
            conn, task_id, payload.worker_id, payload.reason, payload.retryable
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return SubmissionAck(task_id=str(task_id), status=result)
