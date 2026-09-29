from typing import Any, Optional

from pydantic import BaseModel, Field, validator


class ScrapeTask(BaseModel):
    task_id: str
    task_key: str
    target_url: str
    task_type: str = "text_search"
    search_keyword: str = ""
    search_location: str = ""
    order_id: str = ""
    query_type: str = "Unknown"
    minimum_reviews: int = 0


class TaskAcquireRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=128)
    version: str = Field(default="unknown", max_length=128)


class TaskAcquireResponse(BaseModel):
    task: Optional[ScrapeTask] = None
    lease_expires_at: Optional[str] = None


class TaskFailureRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=128)
    reason: str = Field(min_length=1, max_length=2000)
    retryable: bool = True


class TaskEnqueueItem(BaseModel):
    task_key: str = Field(min_length=1, max_length=256)
    target_url: str = Field(min_length=1, max_length=4096)
    task_type: str = Field(default="text_search", max_length=64)
    search_keyword: str = ""
    search_location: str = ""
    order_id: str = ""
    query_type: str = "Unknown"
    minimum_reviews: int = Field(default=0, ge=0)


class TaskEnqueueRequest(BaseModel):
    tasks: list[TaskEnqueueItem]

    @validator("tasks")
    def validate_task_count(cls, tasks):
        if not 1 <= len(tasks) <= 1000:
            raise ValueError("tasks must contain between 1 and 1000 items")
        return tasks


class TaskEnqueueResponse(BaseModel):
    inserted: int
    duplicates: int


class SubmissionAck(BaseModel):
    task_id: str
    status: str
    idempotent: bool = False


class HeartbeatRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=128)
    version: str = Field(default="unknown", max_length=128)
    hostname: str = Field(default="", max_length=255)


class HeartbeatResponse(BaseModel):
    target_version: str
    update_required: bool


class ApiError(BaseModel):
    detail: str


JsonObject = dict[str, Any]
