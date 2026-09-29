#!/bin/sh
set -u
ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT_DIR"
while true; do
    python3 bootstrap/updater.py
    python3 worker/main_worker.py
    worker_status=$?
    if [ "$worker_status" -ne 0 ]; then
        python3 bootstrap/updater.py --record-failure
    fi
    sleep 5
done
