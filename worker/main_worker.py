import sys
import time
import signal
import random
import multiprocessing
from pathlib import Path

CLUSTER_ROOT = Path(r"C:\ProgramData\GMapEliteCluster")
REPO_DIR = CLUSTER_ROOT / "app" / "scraper-core"
CONFIG_FILE = CLUSTER_ROOT / "config" / ".env"
sys.path.insert(0, str(REPO_DIR))

from worker.hardware import resolve_optimal_concurrency
from worker.api_client import MasterApiClient
from worker.scraper import scrape
from shared.exceptions import ApiClientError, CaptchaException, ScrapeException

SHUTDOWN = False


def load_env() -> dict:
    data = {}
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    data[k] = v
    return data


def sig_handler(signum, frame):
    global SHUTDOWN
    SHUTDOWN = True


def worker_process_entrypoint(worker_idx: int):
    signal.signal(signal.SIGINT, sig_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, sig_handler)

    env = load_env()
    client = MasterApiClient(
        base_url=env.get("MASTER_URL", ""),
        auth_token=env.get("AUTH_TOKEN", ""),
        worker_id=f"{env.get('WORKER_ID', 'node')}-{worker_idx}",
        version=env.get("LOCAL_VERSION", "1.0.0")
    )

    idle_delay = 5.0
    last_hb = 0.0

    try:
        while not SHUTDOWN:
            if time.monotonic() - last_hb >= 60:
                try:
                    client.heartbeat()
                    last_hb = time.monotonic()
                except Exception:
                    pass

            try:
                task = client.acquire()
            except ApiClientError:
                time.sleep(idle_delay)
                idle_delay = min(60.0, idle_delay * 1.5)
                continue

            if not task:
                time.sleep(random.uniform(5.0, 15.0))
                continue

            idle_delay = 5.0
            try:
                # Scrape directly returns raw HTML string
                html = scrape(task)
                # Client gzip compresses and streams to Master
                client.submit(task, html)
            except CaptchaException as exc:
                client.fail(task, str(exc), retryable=True)
                time.sleep(60.0)
            except (ScrapeException, Exception) as exc:
                client.fail(task, str(exc), retryable=True)
                time.sleep(random.uniform(2.0, 5.0))
    finally:
        client.close()


def main():
    multiprocessing.freeze_support()
    concurrency = resolve_optimal_concurrency()

    processes = []
    for i in range(concurrency):
        p = multiprocessing.Process(target=worker_process_entrypoint, args=(i + 1,), daemon=True)
        p.start()
        processes.append(p)

    while True:
        time.sleep(5)
        # Check process liveness and resurrect crashed children
        for idx, p in enumerate(processes):
            if not p.is_alive():
                new_p = multiprocessing.Process(target=worker_process_entrypoint, args=(idx + 1,), daemon=True)
                new_p.start()
                processes[idx] = new_p


if __name__ == "__main__":
    main()