import gzip
import random
import time
from collections.abc import Callable
from socket import gethostname

import httpx

from shared.contracts import (
    HeartbeatRequest,
    HeartbeatResponse,
    ScrapeTask,
    SubmissionAck,
    TaskAcquireResponse,
    TaskFailureRequest,
)
from shared.exceptions import ApiClientError


class MasterApiClient:
    def __init__(
        self,
        base_url: str,
        auth_token: str,
        worker_id: str,
        version: str,
        timeout_seconds: float = 60,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.worker_id = worker_id
        self.version = version
        self._sleep = sleep
        self._client = httpx.Client(
            base_url=base_url,
            timeout=timeout_seconds,
            headers={"Authorization": f"Bearer {auth_token}"},
        )

    def close(self) -> None:
        self._client.close()

    def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        delay = 1.0
        while True:
            try:
                response = self._client.request(method, path, **kwargs)
            except httpx.TransportError:
                self._sleep(min(300.0, delay) * random.uniform(0.8, 1.2))
                delay = min(300.0, delay * 2)
                continue
            if response.status_code in {408, 429} or response.status_code >= 500:
                self._sleep(min(300.0, delay) * random.uniform(0.8, 1.2))
                delay = min(300.0, delay * 2)
                continue
            if response.is_error:
                raise ApiClientError(
                    f"master returned HTTP {response.status_code}: {response.text}"
                )
            return response

    def heartbeat(self) -> HeartbeatResponse:
        response = self._request(
            "POST",
            "/api/v1/heartbeat",
            json=HeartbeatRequest(
                worker_id=self.worker_id,
                version=self.version,
                hostname=gethostname(),
            ).dict(),
        )
        return HeartbeatResponse.parse_obj(response.json())

    def acquire(self) -> ScrapeTask | None:
        response = self._request(
            "POST",
            "/api/v1/tasks/acquire",
            json={"worker_id": self.worker_id, "version": self.version},
        )
        result = TaskAcquireResponse.parse_obj(response.json())
        return result.task

    def submit(self, task: ScrapeTask, html: str) -> SubmissionAck:
        compressed = gzip.compress(html.encode("utf-8"))
        response = self._request(
            "POST",
            f"/api/v1/tasks/{task.task_id}/submit",
            params={"worker_id": self.worker_id},
            content=compressed,
            headers={"Content-Encoding": "gzip", "Content-Type": "application/gzip"},
        )
        return SubmissionAck.parse_obj(response.json())

    def fail(self, task: ScrapeTask, reason: str, retryable: bool = True) -> None:
        self._request(
            "POST",
            f"/api/v1/tasks/{task.task_id}/fail",
            json=TaskFailureRequest(
                worker_id=self.worker_id,
                reason=reason[:2000],
                retryable=retryable,
            ).dict(),
        )
