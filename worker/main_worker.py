import logging
import random
import signal
import time

from worker.api_client import MasterApiClient
from worker.config import load_settings
from worker.scraper import scrape
from shared.exceptions import ApiClientError, CaptchaException, ScrapeException


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
shutdown_requested = False


def request_shutdown(signum, frame) -> None:
    global shutdown_requested
    shutdown_requested = True


def main() -> None:
    signal.signal(signal.SIGINT, request_shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_shutdown)
    settings = load_settings()
    client = MasterApiClient(
        settings.master_url, settings.auth_token, settings.worker_id, settings.version
    )
    idle_delay = settings.poll_min_seconds
    last_heartbeat = 0.0
    try:
        while not shutdown_requested:
            try:
                if time.monotonic() - last_heartbeat >= 60:
                    client.heartbeat()
                    last_heartbeat = time.monotonic()
                task = client.acquire()
            except ApiClientError:
                logger.exception("Master API rejected the task acquisition request")
                time.sleep(min(settings.poll_max_seconds, idle_delay))
                idle_delay = min(settings.poll_max_seconds, idle_delay * 2)
                continue
            idle_delay = settings.poll_min_seconds
            if task is None:
                time.sleep(random.uniform(settings.poll_min_seconds, settings.poll_max_seconds))
                continue
            try:
                logger.info("Scraping task %s (%s)", task.task_id, task.target_url)
                html = scrape(task, settings.relay_dir)
                client.submit(task, html)
            except CaptchaException as exc:
                logger.warning("Task %s hit CAPTCHA: %s", task.task_id, exc)
                client.fail(task, str(exc), retryable=True)
                time.sleep(60)
            except ScrapeException as exc:
                logger.exception("Task %s scrape failed", task.task_id)
                client.fail(task, str(exc), retryable=True)
                time.sleep(random.uniform(2, 8))
            except ApiClientError:
                logger.exception(
                    "Master rejected task %s; it will be recovered when its lease expires",
                    task.task_id,
                )
                time.sleep(random.uniform(2, 8))
    finally:
        client.close()


if __name__ == "__main__":
    main()
