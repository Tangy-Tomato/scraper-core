#!/usr/bin/env python3
"""
WATCHDOG PROCESS MANAGER (PRODUCTION-GRADE FAILSAFE)
Guards worker lifecycle, forces restart cycles, recovers dropped relay tickets,
and cleans status artifacts.
"""

import os
import sys
import time
import shutil
import psutil
import subprocess
from pathlib import Path

# ================= CONFIGURATION =================
ROOT_DIR = Path(__file__).resolve().parent
ELITE_DIR = Path(r"E:\My Drive\Elite_Scraper_Relay")
if not ELITE_DIR.exists():
    ELITE_DIR = ROOT_DIR / "Elite_Scraper_Relay"

JOB_PENDING_DIR = ELITE_DIR / "Jobs_Pending"
JOB_PROCESSING_DIR = ELITE_DIR / "Jobs_Processing"
SYSTEM_TEMP_DIR = ROOT_DIR / "System_Temp"

# Target worker script
SCRIPT_TO_RUN = ROOT_DIR / "WorkerNodeActual.py"
if not SCRIPT_TO_RUN.exists():
    SCRIPT_TO_RUN = ROOT_DIR / "Stage_1_Downloader_Multithreded.py"

MAX_RUNTIME_SECONDS = 2400  # 20 Minutes failsafe ceiling
PENDING_EMPTY_GRACE_PERIOD = 500  # Seconds to confirm pending stays empty
CHECK_INTERVAL_SECONDS = 5

# Ensure relay directories exist
JOB_PENDING_DIR.mkdir(parents=True, exist_ok=True)
JOB_PROCESSING_DIR.mkdir(parents=True, exist_ok=True)


def kill_process_tree(pid: int) -> None:
    """Recursively terminates a process and all spawned children (e.g., Chromium nodes)."""
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        
        # 1. Soft termination signal
        for child in children:
            try:
                child.terminate()
            except psutil.NoSuchProcess:
                pass
        parent.terminate()

        # Allow 3 seconds for graceful cleanup
        _, alive = psutil.wait_procs(children + [parent], timeout=3)

        # 2. Force kill remaining stubborn processes
        for p in alive:
            try:
                p.kill()
            except psutil.NoSuchProcess:
                pass

    except psutil.NoSuchProcess:
        pass
    except Exception as e:
        print(f"[WATCHDOG ERROR] Failed to kill process tree for PID {pid}: {e}")


def purge_status_json_files() -> None:
    """Removes transient status and progress JSON files to avoid stale pipeline state."""
    target_dirs = [ROOT_DIR, SYSTEM_TEMP_DIR, ELITE_DIR]
    purged = 0

    for directory in target_dirs:
        if not directory.exists():
            continue
        for file_path in directory.glob("*status*.json"):
            try:
                if file_path.is_file():
                    file_path.unlink(missing_ok=True)
                    purged += 1
            except Exception as e:
                print(f"[WATCHDOG WARN] Failed to remove status file {file_path}: {e}")

    if purged > 0:
        print(f"[WATCHDOG] 🧹 Cleared {purged} stale status JSON files.")


def recover_processing_tickets() -> int:
    """Moves any orphaned jobs from Jobs_Processing back to Jobs_Pending."""
    moved_count = 0
    for ticket in JOB_PROCESSING_DIR.glob("*.json"):
        try:
            target_path = JOB_PENDING_DIR / ticket.name
            shutil.move(str(ticket), str(target_path))
            moved_count += 1
        except Exception as e:
            print(f"[WATCHDOG WARN] Could not move ticket {ticket.name}: {e}")

    print(f"[WATCHDOG] 🔄 Re-queued {moved_count} tickets from Processing -> Pending.")
    return moved_count


def count_pending_tickets() -> int:
    """Counts unparsed ticket payloads currently sitting in pending."""
    try:
        return len(list(JOB_PENDING_DIR.glob("*.json")))
    except Exception:
        return 0


def run_watchdog() -> None:
    print("=" * 60)
    print(f" 🐕 WATCHDOG ACTIVE: Guarding {SCRIPT_TO_RUN.name}")
    print(f" Monitoring Pending Queue: {JOB_PENDING_DIR}")
    print("=" * 60)

    while True:
        # Pre-execution cleanup
        purge_status_json_files()
        
        # If pending is totally empty before launch, recover processing jobs immediately
        if count_pending_tickets() == 0:
            print("[WATCHDOG] No pending tickets found at launch. Recovering processing tickets...")
            recover_processing_tickets()
            time.sleep(4)

        print(f"\n[WATCHDOG] Spawning worker: {SCRIPT_TO_RUN} ...")
        process = subprocess.Popen([sys.executable, str(SCRIPT_TO_RUN)])
        start_time = time.time()
        empty_pending_start = None

        while True:
            time.sleep(CHECK_INTERVAL_SECONDS)

            # 1. Check if the process died on its own
            poll_code = process.poll()
            if poll_code is not None:
                if poll_code == 0:
                    print("\n✅ [WATCHDOG] Worker finished naturally (Code 0).")
                else:
                    print(f"\n❌ [WATCHDOG] Worker crashed (Code {poll_code}).")
                
                recover_processing_tickets()
                purge_status_json_files()
                print("[WATCHDOG] Pausing 5s before next run...")
                time.sleep(10)
                break

            # 2. Check for Pending Directory Exhaustion
            pending_count = count_pending_tickets()
            if pending_count == 0:
                if empty_pending_start is None:
                    empty_pending_start = time.time()
                elif (time.time() - empty_pending_start) >= PENDING_EMPTY_GRACE_PERIOD:
                    print("\n🛑 [WATCHDOG] Jobs_Pending is completely empty!")
                    print("[WATCHDOG] Terminating worker to cycle tickets...")
                    kill_process_tree(process.pid)
                    time.sleep(2)

                    recover_processing_tickets()
                    purge_status_json_files()
                    print("[WATCHDOG] Hotfix cycle complete. Re-spawning in 5s...\n")
                    time.sleep(5)
                    break
            else:
                empty_pending_start = None

            # 3. Check for Global Max Timeout
            elapsed = time.time() - start_time
            if elapsed > MAX_RUNTIME_SECONDS:
                print(f"\n⚠️ [WATCHDOG] Worker exceeded max limit ({MAX_RUNTIME_SECONDS}s). Deadlock suspected.")
                kill_process_tree(process.pid)
                time.sleep(2)

                recover_processing_tickets()
                purge_status_json_files()
                print("[WATCHDOG] Reset completed. Re-spawning in 5s...\n")
                time.sleep(5)
                break


if __name__ == "__main__":
    try:
        run_watchdog()
    except KeyboardInterrupt:
        print("\n[WATCHDOG] Shutdown requested by user. Exiting cleanly.")
        sys.exit(0)