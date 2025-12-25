import glob, os
from ai_tool.config_loader import config, load_config

def run_ai(fast_mode=False):
    files = glob.glob(os.path.join(config.scrapper_output_dir, "Scrapper_*.xlsx"))
    if not files: print("No Scrapper file!"); return
    latest = max(files, key=os.path.getmtime)
    from ai_tool.main_processor import run_single_process
    run_single_process(latest, fast_mode=fast_mode)

def start_worker(worker_id, run_ts, job_queue, results_queue, status_queue, key_queue,
                 high_accuracy=False, use_vision_v2=False, yoda_instance=None):
    try:
        load_config()
        from ai_tool.main_processor import run_worker_process
        from ai_tool import utils
        
        # Log Dually Detection settings for this worker
        darth_status = f"ON (threshold={config.darth_cv2_dually_threshold})" if config.enable_darth_cv2_dually else "OFF"
        utils.log_msg(f"[W-{worker_id}] 🔧 Dually Settings: Darth CV2={darth_status}", worker_id)
        
        # Ensure 'yoda_instance' is passed down to the processor
        run_worker_process(
            worker_id, run_ts, job_queue, results_queue, status_queue,
            key_queue, high_accuracy=high_accuracy, 
            use_vision_v2=use_vision_v2, yoda_instance=yoda_instance
        )
        
    except Exception as e:
        log_path = os.path.join("logs", f"CRASH_{worker_id}_{run_ts}.txt")
        os.makedirs("logs", exist_ok=True)
        with open(log_path, "w") as f: f.write(str(e))