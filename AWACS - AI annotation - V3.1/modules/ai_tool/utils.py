import os
import glob
import shutil
import pandas as pd
from datetime import datetime, timedelta
from .config_loader import config

LOG_FILE = ""

def initialize_logging(run_ts: str, worker_id: int = 0):
    """
    Sets up the log file path for a specific worker.
    Creates the file and writes the header.
    """
    global LOG_FILE
    
    # Define filename: log_2025-11-26_10-00-00_worker_01.txt
    log_filename = f"log_{run_ts}_worker_{worker_id:02d}.txt"
    
    try:
        # Ensure logs folder exists
        os.makedirs(config.log_dir, exist_ok=True)
        
        LOG_FILE = os.path.join(config.log_dir, log_filename)
        
        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.write(f"================================================================\n")
            f.write(f"LOG STARTED: Worker {worker_id}\n")
            f.write(f"Run ID: {run_ts}\n")
            f.write(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"================================================================\n\n")
            
    except Exception as e:
        print(f"CRITICAL: Failed to initialize log file at {LOG_FILE}. Error: {e}")

def log_msg(msg: str, worker_id: int = -1):
    """
    Logs a message to the worker's specific text file with a timestamp.
    """
    if LOG_FILE:
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            prefix = f"[{timestamp}] [W-{worker_id:02d}] "
            
            # Indent multiline messages so they look clean
            clean_msg = str(msg).replace("\n", f"\n{' ' * len(prefix)}")
            
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(prefix + clean_msg + "\n")
        except Exception:
            pass # Fail silently to avoid crashing the worker

def fmt_secs(seconds: float) -> str:
    """Formats seconds into a readable HH:MM:SS string."""
    return str(timedelta(seconds=int(seconds)))

def merge_worker_logs(run_ts):
    """
    Combines all individual 'log_{run_ts}_worker_*.txt' files 
    into a single 'MASTER_LOG_{run_ts}.txt'.
    """
    master_log_path = os.path.join(config.log_dir, f"MASTER_LOG_{run_ts}.txt")
    pattern = os.path.join(config.log_dir, f"log_{run_ts}_worker_*.txt")
    worker_files = glob.glob(pattern)
    
    if not worker_files:
        print("⚠️ No worker logs found to merge.")
        return

    print(f"\n📝 Merging {len(worker_files)} worker logs into Master Log...")
    
    try:
        with open(master_log_path, 'w', encoding='utf-8') as master:
            master.write(f"================================================================\n")
            master.write(f"MASTER SESSION LOG - RUN ID: {run_ts}\n")
            master.write(f"Merged at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            master.write(f"================================================================\n\n")
            
            # Sort files so Worker 01 comes before Worker 02, etc.
            worker_files.sort()
            
            for wf in worker_files:
                try:
                    # Extract worker ID from filename for the header
                    w_name = os.path.basename(wf).replace(f"log_{run_ts}_", "").replace(".txt", "")
                    
                    master.write(f"\n{'='*20} {w_name.upper()} {'='*20}\n")
                    
                    with open(wf, 'r', encoding='utf-8') as f:
                        content = f.read()
                        master.write(content)
                        
                    master.write("\n") # Spacing between workers
                except Exception as e:
                    master.write(f"\n[ERROR READING LOG FILE: {wf} - {e}]\n")
        
        # Cleanup: Delete individual files to keep folder clean
        for wf in worker_files:
            try: os.remove(wf)
            except: pass
            
        print(f"✅ Master Log saved: {os.path.basename(master_log_path)}")
        
    except Exception as e:
        print(f"❌ Error merging logs: {e}")

def calculate_cost_cents(input_tokens, output_tokens, model_name):
    """
    Calculates the cost of an API call in Cents based on the specific Gemini model.
    """
    model = model_name.lower()
    
    # --- PRICING TABLE (Per 1 Million Tokens) ---
    
    # Default: Gemini 2.5 Flash (Standard)
    # Input: $0.30 | Output: $2.50
    price_input_per_m = 0.30
    price_output_per_m = 2.50

    # Logic: Gemini 2.5 Flash-8B (Lite)
    # Input: $0.10 | Output: $0.40
    if "lite" in model or "8b" in model:
        price_input_per_m = 0.10
        price_output_per_m = 0.40
        
    # --- CALCULATION ---
    cost_usd = (input_tokens / 1_000_000 * price_input_per_m) + \
               (output_tokens / 1_000_000 * price_output_per_m)
               
    # Convert to Cents
    return round(cost_usd * 100, 4)

def generate_session_reports(key_usage_stats_data, token_usage_stats, run_ts, worker_id=0):
    """
    1. Generates the per-worker Excel report (Key/Token usage).
    2. Logs a text summary to the log file.
    """
    key_stats = key_usage_stats_data.get("stats", {})
    has_key_data = bool(key_stats)
    has_token_data = token_usage_stats and token_usage_stats.get('api_calls', 0) > 0

    if not has_key_data and not has_token_data:
        return

    # --- Part 1: Text Logging ---
    log_msg("\n--- WORKER SESSION STATISTICS ---", worker_id)
    
    if has_token_data:
        total_tokens = token_usage_stats['total_tokens']
        total_calls = token_usage_stats['api_calls']
        avg = int(total_tokens / total_calls) if total_calls else 0
        log_msg(f"API Calls: {total_calls} | Total Tokens: {total_tokens} (Avg: {avg}/call)", worker_id)
    
    if has_key_data:
        # Sort by key index
        sorted_keys = sorted(key_stats.items())
        for k, s in sorted_keys:
            log_msg(f"Key #{k}: Success={s.get('success',0)} | Quota Failures={s.get('quota_failure',0)}", worker_id)
    
    log_msg("---------------------------------", worker_id)

    # --- Part 2: Excel Report Saving ---
    try:
        # Prepare Data for Excel
        key_report_data = []
        if has_key_data:
            # We need the full key info to get the partial key string
            # Re-read config here to be safe inside worker
            key_info_map = {info['original_index']: info for info in config.gemini_api_keys_info}

            for key_idx, stats in sorted_keys:
                key_info = key_info_map.get(key_idx, {})
                key_report_data.append({
                    "Original Key Name": f"Key {key_idx}",
                    "Partial API Key": key_info.get('partial_key', 'N/A'),
                    "Successful Calls": stats.get('success', 0),
                    "Quota Failures": stats.get('quota_failure', 0),
                    "Total Attempts": stats.get('success', 0) + stats.get('quota_failure', 0)
                })

        token_report_data = []
        if has_token_data:
            token_report_data = [
                {"Metric": "Total API Calls Made", "Value": token_usage_stats['api_calls']},
                {"Metric": "Total Tokens Used", "Value": token_usage_stats['total_tokens']},
                {"Metric": "Average Tokens per Call", "Value": int(token_usage_stats['total_tokens'] / token_usage_stats['api_calls']) if token_usage_stats['api_calls'] else 0},
            ]

        # Save to File
        os.makedirs(config.key_report_dir, exist_ok=True)
        report_filename = f"Session_Report_worker_{worker_id}_{run_ts}.xlsx"
        report_path = os.path.join(config.key_report_dir, report_filename)
        
        with pd.ExcelWriter(report_path) as writer:
            if key_report_data:
                pd.DataFrame(key_report_data).to_excel(writer, sheet_name='Key Usage', index=False)
            if token_report_data:
                pd.DataFrame(token_report_data).to_excel(writer, sheet_name='Token Usage', index=False)
        
        log_msg(f"✅ Excel Report saved: {os.path.basename(report_path)}", worker_id)

    except Exception as e:
        log_msg(f"⚠️ Could not save Excel session report. Error: {e}", worker_id)