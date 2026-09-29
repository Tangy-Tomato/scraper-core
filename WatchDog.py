import subprocess
import time
import os
import psutil
from pathlib import Path

# Configuration
ROOT_DIR = Path(__file__).resolve().parent

SCRIPT_TO_RUN = ROOT_DIR / "Stage_4_Email_Extractor.py"
MAX_RUNTIME_SECONDS = 8000  # 20 Minutes

def kill_process_tree(pid):
    """Ruthlessly kills a process and all its children (e.g., zombie Chromium browsers)"""
    try:
        parent = psutil.Process(pid)
        for child in parent.children(recursive=True):
            child.kill()
        parent.kill()
    except psutil.NoSuchProcess:
        pass

def run_watchdog():
    print("==================================================")
    print(f" 🐕 WATCHDOG INITIATED: Guarding {SCRIPT_TO_RUN} ")
    print("==================================================")

    while True:
        print(f"\n[WATCHDOG] Spawning new process for {SCRIPT_TO_RUN}...")
        
        # Start the process
        process = subprocess.Popen(["python", SCRIPT_TO_RUN])
        start_time = time.time()
        
        # Monitor Loop
        while True:
            # Check if process finished naturally
            if process.poll() is not None:
                if process.returncode == 0:
                    print("\n✅ [WATCHDOG] Script completed successfully! Exiting.")
                    return
                else:
                    print(f"\n❌ [WATCHDOG] Script crashed with code {process.returncode}. Restarting in 5s...")
                    time.sleep(5)
                    break # Break inner loop to restart
            
            # Check if process exceeded the 20-minute limit
            elapsed_time = time.time() - start_time
            if elapsed_time > MAX_RUNTIME_SECONDS:
                print(f"\n⚠️ [WATCHDOG] 20-Minute Time Limit Reached. Probable Deadlock.")
                print("[WATCHDOG] Murdering process tree and freeing RAM...")
                kill_process_tree(process.pid)
                time.sleep(3) # Let the OS clean up ports/RAM
                break # Break inner loop to restart
            
            # Sleep briefly to prevent high CPU usage in the watchdog itself
            time.sleep(5)

if __name__ == "__main__":
    run_watchdog()