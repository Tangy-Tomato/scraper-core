import sys
import os
import time
import json
import random
import gc
import shutil
import signal
import threading
import subprocess
import socket
import urllib.request
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError, Error as PlaywrightError
from datetime import datetime
from lib import io_helpers as ioh

# Cross-platform terminal fallback for selection timeouts (Replaces msvcrt)
if sys.platform != "win32":
    import select

# --- LOCAL IMPORT ---
try:
    from hash_tool import generate_id
except ImportError:
    print("CRITICAL ERROR: hash_tool.py not found or generate_id missing!")
    sys.exit(1)

# ================= STATE ARCHITECTURE =================
SHUTDOWN_REQUESTED = False
NETWORK_LOCK = threading.Lock()
GLOBAL_NETWORK_HEALTH = True  # True = Healthy, False = Loop-checking

def _signal_handler(signum, frame):
    global SHUTDOWN_REQUESTED
    if not SHUTDOWN_REQUESTED:
        SHUTDOWN_REQUESTED = True
        print("\n\n" + "=" * 60)
        print("  ⚠️  SHUTDOWN REQUESTED — finishing current downloads...")
        print("=" * 60 + "\n")

# ================= CONFIGURATION & PATHS =================
# Dynamic path adaptation to protect cross-platform execution profiles
if sys.platform == "win32":
    DEFAULT_RELAY_DIR = Path(r"E:\Google maps Data Elite\Gmap_Extractor_Elite\Elite_Scraper_Relay")
else:
    DEFAULT_RELAY_DIR = Path("/home/danish/gmapExtract")

RELAY_DIR = Path(os.environ.get("WORKER_RELAY_DIR", str(DEFAULT_RELAY_DIR)))
JOBS_PENDING = RELAY_DIR / "Jobs_Pending"
JOBS_PROCESSING = RELAY_DIR / "Jobs_Processing"
HTML_DROPOFF = RELAY_DIR / "HTML_Dropoff"

MAX_CONCURRENT_BROWSERS = 2
MAX_RETRIES_PER_QUERY = 4

CONNECTION_TYPE = "WIFI"
IP_ROTATION_LOCK = threading.Lock()
LAST_ROTATION_TIME = 0

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.6478.127 Safari/537.36",
]

def get_browser_config():
    return {
        "engine": "chromium",
        "viewport": {"width": 1920, "height": 1080},
        "user_agent": random.choice(USER_AGENTS),
        "locale": "en-IN",
        "timezone": "Asia/Kolkata",
    }

# ================= FAIL-SAFE NETWORK POLL ENGINE =================

def check_network_hardware():
    """
    Simplified deterministic reachability check.
    Single HTTP check to https://www.google.com with a short timeout.
    Returns True if reachable, False otherwise.
    """
    try:
        urllib.request.urlopen("https://www.google.com", timeout=3)
        return True
    except Exception:
        return False

def maintain_network_integrity(worker_id):
    """
    Halts thread executions, blocks socket requests, and loops every 10s until health returns.
    Simplified: only checks google.com reachability.
    """
    global GLOBAL_NETWORK_HEALTH
    with NETWORK_LOCK:
        if GLOBAL_NETWORK_HEALTH:
            GLOBAL_NETWORK_HEALTH = False
            print(f"\n[{datetime.utcnow().isoformat()}] [WORKER {worker_id} 🚨 CRITICAL] Connection Dropped! Initializing recovery block...")

        # Loop until google.com is reachable again
        while True:
            time.sleep(10)
            if check_network_hardware():
                GLOBAL_NETWORK_HEALTH = True
                print(f"\n[{datetime.utcnow().isoformat()}] [WORKER {worker_id} ✅ RECOVERY] Connection restored. Resuming scraper operations.")
                break
            print(f"[{datetime.utcnow().isoformat()}] [RETRY LOOP] Gateway unreachable. Re-testing connection in 10 seconds...")

# ================= STATE MANAGEMENT HELPERS =================

def read_worker_state(status_file: Path) -> int:
    if not status_file.exists():
        return -1
    try:
        with open(status_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("last_index", -1)
    except Exception as e:
        print(f"[STATE WARNING] Corrupt state file {status_file.name}, starting from scratch: {e}")
        return -1

def write_worker_state(status_file: Path, index: int):
    pass
    temp_file = status_file.with_suffix(".tmp")
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump({"last_index": index}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, status_file)
    except Exception as e:
        print(f"[STATE ERROR] Failed to update checkpoint for {status_file.name}: {e}")

# ================= HARDENED OPERATIONAL METHODS =================

def setup_connection_mode():
    global CONNECTION_TYPE
    print("\n--- WORKER NODE: NETWORK CONFIGURATION ---")
    print("1. WIFI (Standard - Wait for cooldown)")
    print("2. ADB (USB Tethering - Auto Airplane Mode)")

    timeout = 10
    print(f"[INPUT] Select 1 or 2 (Auto-defaulting to WIFI in {timeout}s): ", end="", flush=True)

    if sys.platform == "win32":
        import msvcrt
        start_time = time.time()
        choice = None
        while (time.time() - start_time) < timeout:
            if msvcrt.kbhit():
                key = msvcrt.getwche()
                if key in ["1", "2"]:
                    choice = key
                    print()
                    break
            time.sleep(0.1)
    else:
        # Secure, non-blocking input capture for headless Linux servers
        ready, _, _ = select.select([sys.stdin], [], [], timeout)
        if ready:
            choice = sys.stdin.readline().strip()
        else:
            choice = None

    if choice == "2":
        CONNECTION_TYPE = "ADB"
        print("[CONFIG] Manual Override: ADB Mode Selected.")
    else:
        CONNECTION_TYPE = "WIFI"
        print("[CONFIG] Running standard system default profile: WIFI.")

def handle_rotation():
    global LAST_ROTATION_TIME
    with IP_ROTATION_LOCK:
        if time.time() - LAST_ROTATION_TIME < 60:
            time.sleep(5)
            return True

        if CONNECTION_TYPE == "ADB":
            print("\n[NETWORK] ✈️ Engaging ADB Airplane Mode Toggle...")
            try:
                subprocess.run(["adb", "shell", "cmd", "connectivity", "airplane-mode", "enable"], check=True)
                time.sleep(5)
                subprocess.run(["adb", "shell", "cmd", "connectivity", "airplane-mode", "disable"], check=True)
                time.sleep(10)
                LAST_ROTATION_TIME = time.time()
                return True
            except Exception as e:
                print(f"[ERROR] ADB Failure: {e}")
                return False
        else:
            print("\n[NETWORK] ⏳ Cool down active. Restricting request rates for 30 seconds...")
            time.sleep(30)
            LAST_ROTATION_TIME = time.time()
            return True

def intercept_heavy_resources(route):
    if route.request.resource_type in {"image", "media", "font"}:
        route.abort()
    else:
        route.continue_()

def is_valid_html(file_path):
    if not os.path.exists(file_path):
        return False
    try:
        if os.path.getsize(file_path) < 1000:
            return False
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read().lower()
        if "recaptcha" in content or "unusual traffic" in content or "consent.google.com" in content:
            return False
        return True
    except Exception:
        return False

def process_query(page, task, worker_id):
    """Surgically isolates dropouts and catches intermediate network losses cleanly."""
    target = task.get("target_url")
    if not target:
        return "ERROR"

    hash_id = task.get("hash_id") or generate_id(target)
    file_path = HTML_DROPOFF / f"{hash_id}.html"

    if is_valid_html(file_path):
        return "SKIP"

    # Pre-flight check before attempting page operations
    if not GLOBAL_NETWORK_HEALTH or not check_network_hardware():
        return "OFFLINE"

    print(f"[WORKER {worker_id}] Processing target URL string location...")

    try:
        if task.get("type") == "grid_url":
            response = page.goto(str(target), timeout=60000, wait_until="domcontentloaded")
        else:
            response = page.goto("https://www.google.com/maps", timeout=60000, wait_until="domcontentloaded")
        
        # Catch instant error templates or empty frames returned from a dropping gateway
        if response is None or (hasattr(response, "ok") and not response.ok) or "chrome-error://" in page.url:
            return "OFFLINE"
            
        time.sleep(2)
    except PlaywrightError as p_err:
        if "net::ERR_" in str(p_err) or "Timeout" in str(p_err):
            return "OFFLINE"
        return "ERROR"
    except Exception:
        return "ERROR"

    try:
        html = page.content()
        if "unusual traffic" in html.lower() or "ERR_INTERNET_DISCONNECTED" in html:
            return "OFFLINE"

        title = page.title().lower()
        if "captcha" in title:
            return "CAPTCHA"

        try:
            page.get_by_text("Accept all").first.click(timeout=3000)
        except Exception:
            pass

        if task.get("type") != "grid_url":
            try:
                search_input = page.locator('input[name="q"]').first
                search_input.wait_for(state="visible", timeout=15000)
                search_input.click(force=True)
                search_input.fill(str(target))
                page.keyboard.press("Enter")
            except Exception:
                if "captcha" in page.content().lower():
                    return "CAPTCHA"
                return "ERROR"

        # Content processing / Infinite scroll sequence
        try:
            page.wait_for_selector('div[role="feed"], h1', timeout=15000)
            sidebar = page.locator('div[role="feed"]').first

            if sidebar.count() > 0:
                last_card_count = 0
                same_count_retries = 0
                while True:
                    sidebar.evaluate("el => el.scrollTo(0, el.scrollHeight)")
                    time.sleep(random.uniform(1.5, 2.5))
                    if page.locator("text=You've reached the end of the list").count() > 0:
                        break
                    current_card_count = page.locator(".Nv2PK").count()
                    if current_card_count == last_card_count:
                        same_count_retries += 1
                        if same_count_retries >= 5:
                            break
                    else:
                        same_count_retries = 0
                        last_card_count = current_card_count
                    if current_card_count >= 120:
                        break
            else:
                time.sleep(2)
        except Exception:
            pass

        # Final string sanity check before data dump
        html = page.content()
        if "unusual traffic" in html.lower() or "err_connection" in html.lower():
            return "OFFLINE"

        # Commit payload parameters
        try:
            safe_task = {str(k): str(v) for k, v in task.items()}
            meta = {
                "order_ids": ([task.get("order_id")] if task.get("order_id") else []),
                "scraped_date": datetime.utcnow().isoformat(),
                "query_type": task.get("query_type", "Unknown"),
                "source_task": safe_task,
            }
            ok = ioh.write_header(file_path, meta, html)
            if not ok:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write("<!--" + json.dumps(meta, ensure_ascii=False) + "-->\n" + html)
        except Exception:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(html)

        print(f"[OK] Saved -> {str(target)[:30]}...")
        return "SUCCESS"

    except Exception:
        return "ERROR"

# ================= CORE WORKER ROUTINE =================

def worker_thread(worker_id):
    global SHUTDOWN_REQUESTED, GLOBAL_NETWORK_HEALTH

    STATUS_FILE = Path(os.getcwd()) / f"worker_{worker_id}_status.json"

    while not SHUTDOWN_REQUESTED:
        # Check global health state before pulling structural work parameters
        if not GLOBAL_NETWORK_HEALTH:
            maintain_network_integrity(worker_id)
            continue

        pending_tickets = list(JOBS_PENDING.glob("*.json"))
        if not pending_tickets:
            time.sleep(5)
            continue

        ticket_to_claim = pending_tickets[0]
        processing_path = JOBS_PROCESSING / ticket_to_claim.name

        try:
            shutil.move(str(ticket_to_claim), str(processing_path))
        except (FileNotFoundError, PermissionError):
            continue  # Concurrency safety lock skip

        print(f"\n[WORKER {worker_id}] 📦 Claimed Ticket: {ticket_to_claim.name}")

        try:
            with open(processing_path, "r", encoding="utf-8") as f:
                job_data = json.load(f)
        except Exception:
            try:
                os.remove(processing_path)
            except:
                pass
            continue

        queries_to_process = job_data.get("tasks") or job_data.get("queries", [])

        if queries_to_process:
            last_completed_index = read_worker_state(STATUS_FILE)
            if last_completed_index >= 0:
                print(f"[WORKER {worker_id}] 🔄 Recovered state at index {last_completed_index + 1}")

            with sync_playwright() as p:
                config = get_browser_config()
                browser, context, page = None, None, None

                try:
                    browser = p.chromium.launch(
                        headless=True,
                        args=[
                            "--disable-blink-features=AutomationControlled",
                            "--no-sandbox",
                            "--disable-dev-shm-usage",
                            "--disable-gpu",
                            "--mute-audio"
                        ],
                    )
                    context = browser.new_context(
                        viewport=config["viewport"],
                        user_agent=config["user_agent"],
                        locale=config["locale"],
                        timezone_id=config["timezone"],
                    )
                    page = context.new_page()
                    page.route("**/*", intercept_heavy_resources)
                    page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

                    ticket_interrupted = False
                    for i, query in enumerate(queries_to_process):
                        if i <= last_completed_index:
                            continue
                        if SHUTDOWN_REQUESTED:
                            break

                        # Check network constraints before evaluating the query item
                        if not GLOBAL_NETWORK_HEALTH:
                            ticket_interrupted = True
                            break

                        attempts = 0
                        while attempts < MAX_RETRIES_PER_QUERY:
                            status = process_query(page, query, worker_id)

                            if status == "OFFLINE":
                                # INTERCEPT HOOK: Safe browser termination with ZERO saves
                                print(f"\n[WORKER {worker_id} ⚠️] Processing aborted mid-flight due to connection failure.")
                                ticket_interrupted = True
                                # Enter recovery block which will wait until google.com is reachable
                                maintain_network_integrity(worker_id)
                                break  # Break loop to force browser destruction

                            elif status == "CAPTCHA":
                                handle_rotation()
                                break
                            elif status == "ERROR":
                                attempts += 1
                                # Exponential backoff but bounded and simple
                                backoff = min(2 ** attempts, 8)
                                time.sleep(backoff)
                            else:
                                write_worker_state(STATUS_FILE, i)
                                if status == "SUCCESS":
                                    time.sleep(random.uniform(2, 4))
                                break
                        
                        if ticket_interrupted:
                            break

                finally:
                    # Clean destruction protocol. Zero leakage.
                    try: page.close() if page else None
                    except: pass
                    try: context.close() if context else None
                    except: pass
                    try: browser.close() if browser else None
                    except: pass
                    gc.collect()

                # If the loop cut early due to network failure, release the ticket file back to Pending
                if ticket_interrupted:
                    print(f"[WORKER {worker_id}] 🔄 Returning unfinished ticket {ticket_to_claim.name} to Pending lane.")
                    moved_back = False
                    try:
                        shutil.move(str(processing_path), str(JOBS_PENDING / ticket_to_claim.name))
                        moved_back = True
                    except Exception:
                        # One guarded retry after a short pause to reduce race-condition losses
                        try:
                            time.sleep(1)
                            shutil.move(str(processing_path), str(JOBS_PENDING / ticket_to_claim.name))
                            moved_back = True
                        except Exception as e:
                            print(f"[WORKER {worker_id}] ERROR returning ticket to pending: {e}")
                    # Only continue loop if we successfully returned the ticket
                    if moved_back:
                        # Keep the status file intact so resume logic remains unchanged
                        continue
                    else:
                        # If we couldn't move it back, attempt to leave it in processing for manual inspection
                        continue

        # Clean validation closure
        try:
            if processing_path.exists():
                os.remove(processing_path)
            if STATUS_FILE.exists():
                os.remove(STATUS_FILE)
            print(f"[WORKER {worker_id}] ✅ Ticket completed cleanly: {ticket_to_claim.name}")
        except Exception as e:
            print(f"[SYSTEM ERROR] Checkpoint removal blockage: {e}")

# ================= MAIN =================

def main():
    signal.signal(signal.SIGINT, _signal_handler)
    for directory in [JOBS_PENDING, JOBS_PROCESSING, HTML_DROPOFF]:
        directory.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print(" 4TH GEN CLUSTER NODE - RUNNING WITH FAIL-SAFE HOOKS")
    print("=" * 60)

    setup_connection_mode()

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_BROWSERS) as executor:
        futures = []
        for i in range(MAX_CONCURRENT_BROWSERS):
            futures.append(executor.submit(worker_thread, i + 1))
            time.sleep(5)

        for future in futures:
            future.result()

    print("\n✅ Clean System Shutdown Complete.")
    sys.exit(0)

if __name__ == "__main__":
    main()
