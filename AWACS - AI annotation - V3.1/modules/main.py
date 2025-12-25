# main.py — FINAL VERSION
import os
import time
import sys
import glob
import pandas as pd
import threading
import queue
from multiprocessing import Process, freeze_support, Manager
from datetime import datetime

# --- PATH SETUP ---
# Ensure we can import sibling modules correctly
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path: sys.path.append(current_dir)

# Import Custom Modules
from ai_tool import config_loader
from ai_tool.config_loader import config
from ai_tool.main_processor import merge_all_session_reports, save_checkpoint
from ai_tool.rate_limiter import Yoda

import scraper_module
import ai_module
import merge_outputs
import quota_checker_module
import update_status
import qa_checker  # Option 9
import audit       # Option 10

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

# --- DASHBOARD RENDERER ---
def dashboard_renderer(status_queue, total_ads, start_time, num_workers, total_keys):
    worker_status = {i: {"state": "STARTING"} for i in range(1, num_workers + 1)}
    completed = 0
    exhausted_keys = 0
    rate_limit_hits = 0
    shutdown = False

    # Fun Worker Names
    WORKER_NAMES = [
        "Terminator", "WALL-E", "R2-D2", "Jarvis", "HAL 9000", 
        "GLaDOS", "Bender", "C-3PO", "Ultron", "Skynet", 
        "Optimus", "Megatron", "Data", "Cortana", "Siri", 
        "Alexa", "Auto", "EVE", "RoboCop", "Iron Giant"
    ]

    while not shutdown:
        try:
            while True:
                msg = status_queue.get_nowait()
                if msg == "STOP":
                    shutdown = True
                    break
                
                # Update Worker Status
                if "worker_id" in msg:
                    w_id = msg["worker_id"]
                    if w_id not in worker_status: worker_status[w_id] = {}
                    worker_status[w_id].update(msg)
                
                # Update Global Counters
                elif msg.get("type") == "progress":
                    completed = msg.get("completed", completed)
                elif msg.get("type") == "key_exhausted":
                    exhausted_keys += 1
                elif msg.get("type") == "rate_limit":
                    rate_limit_hits += 1
        except queue.Empty:
            pass

        clear_screen()
        # Calculate Percentage & Bar
        percent = (completed / total_ads) * 100 if total_ads else 0
        bar_len = 50
        filled_len = int(bar_len * percent / 100)
        bar = "█" * filled_len + "░" * (bar_len - filled_len)
        
        # Calculate Time
        elapsed = int(time.time() - start_time)
        eta = "??:??"
        if completed > 5:
            avg_time = elapsed / completed
            remaining_ads = total_ads - completed
            eta_sec = int(remaining_ads * avg_time)
            eta = f"{eta_sec//60:02d}m {eta_sec%60:02d}s"

        disp_exhausted = min(exhausted_keys, total_keys)

        print("═" * 80)
        print("   AUTOMATED WORKFLOW TOOL v3.1 - PARALLEL CLASSIFICATION")
        print("═" * 80)
        print(f"\n  PROGRESS: [{bar}] {completed}/{total_ads} ({percent:.1f}%)")
        print(f"  Elapsed: {elapsed//60:02d}:{elapsed%60:02d} | ETA ≈ {eta}")
        print(f"  KEYS: {total_keys} Total | {disp_exhausted} Dead | {rate_limit_hits} Damn you moron slow down\n")
        
        print(f"  {'WORKER':<12}  {'STATUS':<10}  {'CURRENT AD':<17}  {'DONE'}")
        print(f"  {'-'*12}  {'-'*10}  {'-'*17}  {'----'}")
        
        for i in range(1, num_workers + 1):
            s = worker_status.get(i, {})
            state = s.get("state", "OFFLINE")
            
            # Icons
            icon = {
                "PROCESSING": "🔨 Working", 
                "WAITING":    "👀 Looking", 
                "FINISHED":   "😴 Sleepy", 
                "ERROR":      "🔥 Tarnished Warrior. 'Twas nobly fought'", 
                "STARTING":   "⚡ Waking",
                "🥶 Cooling": "🧊 Cooling"
            }.get(state, state)
            
            ad = str(s.get("ad_id", "Idle"))[:17]
            prog = s.get("progress", 0)
            
            name = WORKER_NAMES[(i - 1) % len(WORKER_NAMES)]
            print(f"  {name:<12}  {icon:<10}  {ad:<17}  {prog:4d}")
            
        print("═" * 80)
        time.sleep(0.8)

# --- PARALLEL RUNNER ---
def run_parallel_ai(workers=10, high_accuracy=False, use_vision_v2=False):
    files = glob.glob(os.path.join(config.scrapper_output_dir, "Scrapper_*.xlsx"))
    if not files: print("No Scrapper file!"); input(); return
    latest = max(files, key=os.path.getmtime)

    df = pd.read_excel(latest, dtype={"Ad ID": str})
    df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # Resume Logic
    done_set = set()
    for f in glob.glob(os.path.join(config.output_dir, "output_annotated_*.xlsx")):
        try: done_set.update(pd.read_excel(f, usecols=["Ad ID"])["Ad ID"].astype(str).str.strip())
        except: pass

    print(f"Resume: Skipping {len(done_set)} already processed ads")

    run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    m = Manager()
    job_q, res_q, stat_q, key_q = m.Queue(), m.Queue(), m.Queue(), m.Queue()
    
    # Load Keys
    for k in config.gemini_api_keys_info: key_q.put(k)
    total_keys = len(config.gemini_api_keys_info)

    # Load Jobs
    new = 0
    for _, r in df.iterrows():
        if r["Ad ID"] not in done_set:
            job_q.put(r.to_dict())
            new += 1
    if new == 0: print("All done!"); input(); return

    print(f"\nStarting {workers} workers on {new} new ads...")
    print(f"Mode: {'HIGH ACCURACY (2 images)' if high_accuracy else 'FAST (1 or 2)'} | Vision v2: {'ON' if use_vision_v2 else 'OFF'}")

    # --- INITIALIZE YODA ---
    print("🧙 Initializing Yoda (Rate Limiter)...")
    yoda = Yoda(config.gemini_api_keys_info, config.rate_limit_rpm, m)
    # -----------------------

    start = time.time()
    # Start Dashboard
    threading.Thread(target=dashboard_renderer, args=(stat_q, new, start, workers, total_keys), daemon=True).start()

    # Start Workers
    procs = []
    for i in range(1, workers+1):
        p = Process(target=ai_module.start_worker,
                     # PASS YODA HERE
                     args=(i, run_ts, job_q, res_q, stat_q, key_q, high_accuracy, use_vision_v2, yoda))
        p.start()
        procs.append(p)
        time.sleep(0.5) # Minimal stagger needed now

    results = []
    last_save = time.time()

    # Saver Thread
    def checkpoint_timer():
        nonlocal last_save
        while any(p.is_alive() for p in procs):
            if time.time() - last_save >= 60 and results:
                save_checkpoint(run_ts, results.copy(), df)
                last_save = time.time()
            time.sleep(10)

    threading.Thread(target=checkpoint_timer, daemon=True).start()

    # Result Collector
    while any(p.is_alive() for p in procs) or not res_q.empty():
        try:
            r = res_q.get(timeout=1)
            if r and r.get("Ad ID"):
                results.append(r)
                stat_q.put({"type": "progress", "completed": len(results)})
        except queue.Empty: continue

    for p in procs: p.join(timeout=10) or p.terminate()
    
    # Drain Queue
    while not res_q.empty():
        try:
            r = res_q.get_nowait()
            if r and r.get("Ad ID"):
                results.append(r)
        except: break
    
    stat_q.put("STOP")
    
    # Final Save
    if results:
        target_filename = f"output_annotated_{run_ts}.xlsx"
        print(f"\nSaving final data to '{target_filename}'...")
        try:
            save_checkpoint(run_ts, results, df)
            print(f"✅ File saved successfully.")
        except Exception as e:
            print(f"\n❌ ERROR SAVING FILE: {e}")
            print(f"Dumping raw results to 'EMERGENCY_DUMP_{run_ts}.xlsx'...")
            try:
                pd.DataFrame(results).to_excel(f"EMERGENCY_DUMP_{run_ts}.xlsx", index=False)
            except:
                print("Critical failure: Could not even dump raw results.")
            
    merge_all_session_reports(run_ts)
    
    # Merge Logs
    from ai_tool import utils
    utils.merge_worker_logs(run_ts)
    
    print("\nRUN COMPLETED!")
    input("Press Enter...")

# --- MAIN MENU ---
def main_menu():
    logo = """
==================================================================
          +----------+   +----------+   +----------+
          | Scrape | → |    AI    | → |  Merge   |
          +----------+   +----------+   +----------+
                     AUTOMATED WORKFLOW TOOL v3.1
==================================================================
"""
    while True:
        clear_screen(); print(logo)
        print(" 1. Scraper -> Auto Parallel AI (High Accuracy, No Vision v2)")
        print(" 2. AI High Accuracy (Single Process)")
        print(" 3. AI Fast Mode (Single Process)")
        print(" 4. Parallel Mode (Custom)")
        print("\n 5. Merge Outputs")
        print(" 6. Check API Quota")
        print(" 7. Re-Annotate Status (Compare AI vs Scraper)")
        print(" 8. Scrape Only (Supports Resume)")
        print("\n 9. Run QA Checker (Live Website Validation)")
        print(" 10. Run Accuracy Audit (Compare vs Manual Feedback)")
        print(" 11. Exit")
        print("-" * 66)
        c = input("\nChoice (1-11): ").strip()

        if c == "1":
            scraper_module.run_scraper(resume=False)
            print("\nStarting HIGH ACCURACY Parallel AI (10 workers, 2 images, Vision v2 OFF)...")
            run_parallel_ai(workers=10, high_accuracy=True, use_vision_v2=False)
        elif c == "2": ai_module.run_ai(fast_mode=False)
        elif c == "3": ai_module.run_ai(fast_mode=True)
        elif c == "4":
            w = input("Workers (10): ").strip(); workers = int(w) if w.isdigit() else 10
            acc = input("High Accuracy (2 images)? (y/N): ").lower() == 'y'
            v2 = input("Vision v2? (y/N): ").lower() == 'y'
            run_parallel_ai(workers, high_accuracy=acc, use_vision_v2=v2)
        elif c == "5": merge_outputs.merge_excel_files()
        elif c == "6": quota_checker_module.run_quota_check()
        
        elif c == "7": 
            # Re-Annotate Status
            try:
                update_status.run_status_updater()
            except Exception as e:
                print(f"Error running status updater: {e}")
                input("Press Enter...")

        elif c == "8":
            # Scrape Only
            print("\n--- Scrape Only Mode ---")
            print("1. Start Fresh (Overwrites old partial files)")
            print("2. Resume (Continues from latest Scrapper_*.xlsx)")
            sc = input("Choice (1/2): ").strip()
            if sc == "2":
                scraper_module.run_scraper(resume=True)
            else:
                scraper_module.run_scraper(resume=False)
        
        elif c == "9":
            try: qa_checker.run_qa_check()
            except Exception as e: print(f"Error: {e}"); input()

        elif c == "10":
            try: audit.run_audit()
            except Exception as e: print(f"Error: {e}"); input()

        elif c == "11": break
        input("\nPress Enter...")

if __name__ == "__main__":
    freeze_support()
    config_loader.load_config()
    main_menu()