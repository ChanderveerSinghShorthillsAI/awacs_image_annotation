import glob, os
import queue
import time
from ai_tool.config_loader import config, load_config
from ai_tool.awacs_logger import setup_logger

logger = setup_logger("awacs.ai_module")

def run_ai(fast_mode=False):
    files = glob.glob(os.path.join(config.scrapper_output_dir, "Scrapper_*.xlsx"))
    if not files: logger.error("No Scrapper file!"); return
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


# ==================== DUALLY VERIFICATION WORKER FUNCTIONS ====================

def _verify_single_dually(verification_job: dict, key_queue, status_queue, 
                          results_queue, worker_id: int, yoda_instance):
    """
    Worker function to verify a single dually listing.
    Processes one listing and puts the result in the results queue.
    """
    from ai_tool.utils import calculate_cost_cents
    from ai_tool import classification
    
    idx = verification_job['idx']
    ad_id = verification_job['ad_id']
    img_bytes_list = verification_job['images']
    row_data = verification_job['row_data']
    
    logger.info("   [W-%d] 🔍 Starting verification for Ad %s with %d image(s)...", worker_id, ad_id, len(img_bytes_list))
    
    listing_verify_start = time.time()
    
    try:
        # Call LLM verification with ALL images (Yoda handles rate limiting)
        logger.info("   [W-%d] 📤 Sending Ad %s to LLM for verification...", worker_id, ad_id)
        is_dually, confidence, in_tok, out_tok, cached_tok = classification.verify_dually_with_llm(
            img_bytes_list, 
            yoda_instance, 
            key_queue, 
            worker_id=worker_id, 
            ad_id=ad_id, 
            status_queue=status_queue
        )
        
        # Calculate cost for this verification (with cached token discount)
        cost = calculate_cost_cents(in_tok, out_tok, config.gemini_model_dually_verification, cached_tok)
        listing_time = time.time() - listing_verify_start
        
        # Prepare result
        result = {
            'idx': idx,
            'ad_id': ad_id,
            'is_dually': is_dually,
            'confidence': confidence,
            'cost': cost,
            'in_tokens': in_tok,
            'out_tokens': out_tok,
            'cached_tokens': cached_tok,
            'listing_time': listing_time,
            'row_data': row_data,
            'success': True
        }
        
        if is_dually:
            logger.info("   [W-%d] ✅ Ad %s: CONFIRMED as Dually | Cost: %.4f¢ | Time: %.2fs", worker_id, ad_id, cost, listing_time)
        else:
            logger.info("   [W-%d] ❌ Ad %s: FALSE POSITIVE - NOT Dually | Cost: %.4f¢ | Time: %.2fs", worker_id, ad_id, cost, listing_time)
        
        results_queue.put(result)
        return result
        
    except Exception as e:
        listing_time = time.time() - listing_verify_start
        logger.warning("   [W-%d] ⚠️ Ad %s: Error during verification - %s | Time: %.2fs", worker_id, ad_id, str(e)[:50], listing_time)
        
        # Put error result in queue
        error_result = {
            'idx': idx,
            'ad_id': ad_id,
            'is_dually': True,  # Keep as dually on error
            'confidence': 0,
            'cost': 0,
            'in_tokens': 0,
            'out_tokens': 0,
            'listing_time': listing_time,
            'row_data': row_data,
            'success': False,
            'error': str(e)
        }
        results_queue.put(error_result)
        return error_result


def start_dually_verification_worker(worker_id: int, job_queue, results_queue,
                                     status_queue, key_queue, yoda_instance):
    """
    Worker process for dually verification.
    Pulls jobs from queue and verifies each one.
    """
    try:
        load_config()
        from ai_tool import classification
        
        # Initialize classification trackers for this worker
        classification.initialize_all_trackers()
        
        logger.info("   [W-%d] 🚀 Dually Verification Worker %d STARTED", worker_id, worker_id)
        
        processed = 0
        
        status_queue.put({"worker_id": worker_id, "state": "WAITING", "ad_id": None, "progress": 0})
        
        while True:
            verification_job = None
            try:
                # Get next verification job from queue
                verification_job = job_queue.get(timeout=2)
                ad_id = verification_job['ad_id']
                
                logger.info("   [W-%d] 📋 Picked up verification job for Ad %s", worker_id, ad_id)
                
                status_queue.put({
                    "worker_id": worker_id,
                    "state": "VERIFYING",
                    "ad_id": ad_id,
                    "progress": processed
                })
                
                # Process the verification job
                _verify_single_dually(
                    verification_job, 
                    key_queue, 
                    status_queue, 
                    results_queue, 
                    worker_id, 
                    yoda_instance
                )
                
                processed += 1
                status_queue.put({
                    "worker_id": worker_id,
                    "state": "WAITING",
                    "ad_id": ad_id,
                    "progress": processed
                })
                
            except queue.Empty:
                # No more jobs in queue
                status_queue.put({"worker_id": worker_id, "state": "FINISHED", "progress": processed})
                logger.info("   [W-%d] ✅ Worker %d FINISHED - Processed %d verifications", worker_id, worker_id, processed)
                break
                
            except Exception as e:
                logger.warning("   [W-%d] ⚠️ Worker %d encountered error: %s", worker_id, worker_id, e)
                if verification_job and verification_job.get('ad_id'):
                    # Put error result in queue
                    error_result = {
                        'idx': verification_job['idx'],
                        'ad_id': verification_job['ad_id'],
                        'is_dually': True,
                        'success': False,
                        'error': str(e),
                        'cost': 0
                    }
                    results_queue.put(error_result)
                processed += 1
                status_queue.put({"worker_id": worker_id, "state": "ERROR", "progress": processed})
    
    except Exception as e:
        logger.error("   [W-%d] 💀 Worker %d CRASHED: %s", worker_id, worker_id, e)
        status_queue.put({"worker_id": worker_id, "state": "CRASHED", "progress": processed})