# GMaps Extractor Cluster

The distributed API, workers, and background processors live alongside the
existing extraction scripts. The new Stage 2 processor calls
`Stage_2_Core_Parser.parse_card`; Stage 4 calls the existing online and
Playwright enrichment functions in `Stage_4_Email_Extractor.py`. The
`WorkerNodeActual.py` scrape workflow remains the source of the Google Maps
browser behavior; `worker/scraper.py` adapts it to leased API tasks.

## Install and configure

Use Python 3.10 or newer.

1. Create the PostgreSQL database and a `.env` file based on `.env.example`.
   Use the same long random secret as `MASTER_AUTH_TOKEN` on the master and
   `AUTH_TOKEN` on each worker. Set a stable, unique `WORKER_ID` per worker.
2. Install the master dependencies with
   `python -m pip install -r requirements_master.txt`.
3. On the master, run `python migrations/runner.py`.
4. Install Chromium on the master and worker machines with
   `python -m playwright install chromium` (the worker additionally installs
   `requirements_worker.txt`).

The Master API and background processors use the same master `.env`. Worker
machines need `MASTER_URL`, `AUTH_TOKEN`, `WORKER_ID`, and optionally
`WORKER_RELAY_DIR` and polling settings.
The updater deploys Git tags; set `system_config.target_version` to a tag
available from the worker's Git remote (including the initial deployed tag).

## Run

Start the API from the repository root:

```text
python -m uvicorn master.main:app --host 0.0.0.0 --port 8000
```

With `MASTER_API_URL` configured, the existing `Master_Controller.py` sends its
matrix, direct-query, and grid tasks to `POST /api/v1/tasks/enqueue` and
preserves keyword, location, order, and review-threshold metadata. It moves the
order file after the API accepts the task list. Alternatively, tasks can be
submitted directly with the shared bearer token, or an existing ticket can be
enqueued with `python -m master.enqueue_tasks path\to\job_ticket.json`.
The master leases tasks with `FOR UPDATE SKIP LOCKED`, saves valid gzip uploads
before completing the database transaction, and retries expired leases.

To change the updater's desired release, update the `target_version` value in
the `system_config` table. Workers check it through `/api/v1/heartbeat`.

Run the processors separately on the master:

```text
python -m parsers.stage2_parser
python -m parsers.stage4_enricher
```

Run a worker with `python -m worker.main_worker`, or use
`bootstrap/daemon.bat` on Windows and `bootstrap/daemon.sh` on Linux. The
dependency-free updater executes before each worker start and can roll back
after repeated failed starts.

## Validation

Run the local tests with `python -m unittest discover -v`. Set
`TEST_DATABASE_URL` to an isolated PostgreSQL test database to enable the
50-worker `SKIP LOCKED` integration test.

`Master_Controller.py` and `db_sync.py` are retained as the previous
filesystem/CSV pipeline; the new API/parser path writes the compatible lead
fields directly to PostgreSQL and does not run those legacy entrypoints.
