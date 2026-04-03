# FastAPI Backend for AWACS AI Annotation Tool
import os
import sys
import glob
import uuid
import asyncio
import time
import threading
import tempfile
from datetime import datetime
from typing import Dict
from pathlib import Path
from multiprocessing import Process, Manager, Queue, freeze_support
import queue
# import random
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import pandas as pd

# Add modules to path BEFORE any other imports
PROJECT_ROOT = Path(__file__).parent.parent
MODULES_PATH = str(PROJECT_ROOT / "modules")
if MODULES_PATH not in sys.path:
    sys.path.insert(0, MODULES_PATH)

# Set multiprocessing start method for Windows compatibility
if sys.platform == 'win32':
    try:
        from multiprocessing import set_start_method
        set_start_method('spawn', force=True)
    except RuntimeError:
        pass  # Already set

from ai_tool.config_loader import config, load_config
from ai_tool.rate_limiter import Yoda
from ai_tool.main_processor import save_checkpoint, merge_all_session_reports
from ai_tool.data_processing import load_rules, normalize_text
from ai_tool import web_utils, classification, ad_tracker
import ai_module

# Initialize config
load_config()

# Initialize Ad Annotation Limit Tracker (Turso DB)
if config.enable_ad_annotation_limit:
    print("\n" + "="*60)
    print("📊 AD ANNOTATION LIMIT TRACKER: ✅ ENABLED")
    print(f"   Max annotation runs per ad: {config.max_annotation_runs}")
    print(f"   Turso DB URL: {config.turso_db_url[:40]}...")
    try:
        ad_tracker.init_tracker(config.turso_db_url, config.turso_auth_token)
    except Exception as e:
        print(f"   ❌ Ad Tracker initialization failed: {e}")
        print(f"   ⚠️ Disabling Ad Annotation Limit for this session")
        config.enable_ad_annotation_limit = False
    print("="*60 + "\n")
else:
    print("\n📊 AD ANNOTATION LIMIT TRACKER: ❌ DISABLED (bypassed)\n")

app = FastAPI(title="AWACS AI Annotation API", version="1.0.0")

# CORS settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global state for job management
jobs: Dict[str, dict] = {}
jobs_lock = threading.RLock()  # Thread-safe lock for concurrent job dict access
job_progress: Dict[str, dict] = {}  # Track real-time progress per job
audit_jobs: Dict[str, dict] = {}  # Track audit jobs

# Temp file tracking — collects paths of files saved to OS temp dir so they
# can be deleted by the background cleanup thread.
_temp_output_files: set = set()
_temp_files_lock = threading.Lock()

# Unique tag embedded in every temp filename so startup cleanup only touches
# files from THIS instance (safe if multiple AWACS instances share the same OS).
_AWACS_TEMP_TAG = f"awacs8000_"

# Filename prefixes that AWACS writes to temp dir (used for startup scan)
_AWACS_TEMP_PREFIXES = (
    f"{_AWACS_TEMP_TAG}output_annotated_",
    f"{_AWACS_TEMP_TAG}output_reannotated_",
    f"{_AWACS_TEMP_TAG}output_db_annotated_",
    f"{_AWACS_TEMP_TAG}batch_",
    f"{_AWACS_TEMP_TAG}Audit_Report_",
    f"{_AWACS_TEMP_TAG}DB_Fetch_",
    f"{_AWACS_TEMP_TAG}upload_",
    f"{_AWACS_TEMP_TAG}patch_summary_",
)

# How long (in hours) to keep temp output files before deleting them.
# Files older than this are removed by the background cleanup thread.
_TEMP_FILE_MAX_AGE_HOURS = 24

# How often (in seconds) the background cleanup thread runs.
_TEMP_CLEANUP_INTERVAL_SECONDS = 3600  # every hour


def _temp_path(filename: str) -> str:
    """Return a tagged temp path for a given filename, safe across AWACS instances."""
    tagged = _AWACS_TEMP_TAG + filename
    return os.path.join(tempfile.gettempdir(), tagged)


def _register_temp_file(path: str):
    """Register a temp output file for periodic cleanup."""
    with _temp_files_lock:
        _temp_output_files.add(path)
    print(f"   [TEMP] Registered for cleanup: {os.path.basename(path)}")


def _cleanup_old_temp_files():
    """Delete registered temp files older than _TEMP_FILE_MAX_AGE_HOURS."""
    cutoff = time.time() - (_TEMP_FILE_MAX_AGE_HOURS * 3600)
    deleted = []
    with _temp_files_lock:
        for path in list(_temp_output_files):
            try:
                if os.path.exists(path):
                    if os.path.getmtime(path) < cutoff:
                        os.remove(path)
                        _temp_output_files.discard(path)
                        deleted.append(os.path.basename(path))
                else:
                    _temp_output_files.discard(path)
            except Exception as e:
                print(f"   [TEMP] Could not delete {os.path.basename(path)}: {e}")
    if deleted:
        print(f"\n[TEMP CLEANUP] Deleted {len(deleted)} file(s) older than {_TEMP_FILE_MAX_AGE_HOURS}h:")
        for name in deleted:
            print(f"   - {name}")
    else:
        print(f"[TEMP CLEANUP] No files older than {_TEMP_FILE_MAX_AGE_HOURS}h — nothing to delete.")


def _cleanup_leftover_awacs_temp_files():
    """Delete any AWACS temp files left from a previous crashed/restarted session."""
    tmp = tempfile.gettempdir()
    deleted = []
    for fname in os.listdir(tmp):
        if fname.endswith(".xlsx") and any(fname.startswith(p) for p in _AWACS_TEMP_PREFIXES):
            try:
                os.remove(os.path.join(tmp, fname))
                deleted.append(fname)
            except Exception as e:
                print(f"   [TEMP] Could not delete leftover {fname}: {e}")
    if deleted:
        print(f"[TEMP CLEANUP] Removed {len(deleted)} leftover file(s) from previous session:")
        for name in deleted:
            print(f"   - {name}")
    else:
        print("[TEMP CLEANUP] No leftover files from previous session.")


def _temp_cleanup_loop():
    """Background thread: runs cleanup every _TEMP_CLEANUP_INTERVAL_SECONDS."""
    while True:
        time.sleep(_TEMP_CLEANUP_INTERVAL_SECONDS)
        print(f"\n[TEMP CLEANUP] Background cleanup running (interval: {_TEMP_CLEANUP_INTERVAL_SECONDS // 3600}h)...")
        _cleanup_old_temp_files()


@app.on_event("startup")
def on_startup():
    print("\n[TEMP CLEANUP] Checking for leftover temp files from previous session...")
    _cleanup_leftover_awacs_temp_files()
    t = threading.Thread(target=_temp_cleanup_loop, daemon=True)
    t.start()
    print(f"[TEMP CLEANUP] Background cleanup thread started — runs every {_TEMP_CLEANUP_INTERVAL_SECONDS // 3600}h, deletes files older than {_TEMP_FILE_MAX_AGE_HOURS}h\n")


@app.on_event("shutdown")
def on_shutdown():
    print("\n[TEMP CLEANUP] Server shutting down — running final cleanup...")
    _cleanup_old_temp_files()


class JobStatus:
    PENDING = "pending"
    SCRAPING = "scraping"
    PROCESSING = "processing"
    VERIFYING_DUALLY = "verifying_dually"  # New status for dually verification phase
    COMPLETED = "completed"
    FAILED = "failed"


def scrape_ads_sync(df: pd.DataFrame, job_id: str):
    """
    Synchronous scraping function for backend frontend-triggered jobs.
    
    ULTRA-OPTIMIZED: Maximum speed while maintaining accuracy.
    All filtering rules are preserved (breadcrumb filtering, image validation, etc.)
    """
    from ai_tool.web_utils import setup_driver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.common.action_chains import ActionChains
    
    total = len(df)
    processed = 0
    scraping_start = time.time()
    
    driver = None
    try:
        driver = setup_driver(headless=True)
        print(f"\n{'='*80}")
        print(f"🚀 SCRAPING PHASE STARTED - {total} ads (ULTRA-OPTIMIZED)")
        print(f"{'='*80}\n")
        
        for idx, row in df.iterrows():
            ad_id = str(row.get("Ad ID", "")).strip()
            if not ad_id:
                continue
            
            # Start timing for this listing
            listing_start = time.time()
            url = f"https://www.commercialtrucktrader.com/listing/{ad_id}"
            
            try:
                driver.set_page_load_timeout(8)  # ULTRA-OPTIMIZED: Reduced from 10s to 8s
                try:
                    driver.get(url)
                except TimeoutException:
                    driver.execute_script("window.stop();")
                
                current_url = driver.current_url
                page_title = driver.title.strip()
                
                # Check if inactive
                if f"/listing/{ad_id}" not in current_url:
                    df.at[idx, "Breadcrumb_Top1"] = "Inactive ad"
                    processed += 1
                    listing_time = time.time() - listing_start
                    print(f"[{processed}/{total}] ⚠️ {ad_id}: Inactive | ⏱️ {listing_time:.2f}s")
                    continue
                
                lower_title = page_title.lower()
                if "no longer available" in lower_title or "listing not found" in lower_title:
                    df.at[idx, "Breadcrumb_Top1"] = "Inactive ad"
                    processed += 1
                    listing_time = time.time() - listing_start
                    print(f"[{processed}/{total}] ⚠️ {ad_id}: Inactive | ⏱️ {listing_time:.2f}s")
                    continue
                
                # Extract breadcrumbs (All filtering rules preserved)
                try:
                    nav = WebDriverWait(driver, 5).until(  # ULTRA-OPTIMIZED: Reduced from 6s to 5s
                        EC.presence_of_element_located((By.CSS_SELECTOR, "nav.breadcrumbs"))
                    )
                    links = nav.find_elements(By.TAG_NAME, "a")
                    clean_texts = []
                    
                    for link in links:
                        text = link.text.strip().rstrip(',')
                        href = link.get_attribute("href") or ""
                        t_lower = text.lower()
                        h_lower = href.lower()
                        
                        # Preserve all filtering rules
                        if not text or any(n in t_lower for n in ["home", "browse", "commercial trucks", "for sale"]):
                            continue
                        if any(param in h_lower for param in ["make=", "model=", "state=", "city=", "zip=", "year="]):
                            continue
                        clean_texts.append(text)
                    
                    breadcrumbs = clean_texts[:3]
                    if breadcrumbs:
                        df.at[idx, "Breadcrumb_Top1"] = breadcrumbs[0] if len(breadcrumbs) > 0 else ""
                        df.at[idx, "Breadcrumb_Top2"] = breadcrumbs[1] if len(breadcrumbs) > 1 else ""
                        df.at[idx, "Breadcrumb_Top3"] = breadcrumbs[2] if len(breadcrumbs) > 2 else ""
                    else:
                        df.at[idx, "Breadcrumb_Top1"] = "Inactive ad"
                        
                except Exception:
                    df.at[idx, "Breadcrumb_Top1"] = "Inactive ad"
                
                # Extract images (ULTRA-OPTIMIZED - Maximum speed with same accuracy)
                try:
                    # ULTRA-OPTIMIZED: Reduced wait from 5s to 4s (still sufficient)
                    WebDriverWait(driver, 4).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "img.rsImg"))
                    )
                    time.sleep(0.2)  # ULTRA-OPTIMIZED: Reduced from 0.3s to 0.2s
                    
                    # Try to interact with gallery to load more images (aim for 3 images)
                    try:
                        arrow = driver.find_element(By.CSS_SELECTOR, ".rsArrowRight .rsArrowIcn")
                        action = ActionChains(driver)
                        # ULTRA-OPTIMIZED: Reduced max clicks to 3 (sufficient for 3 images)
                        for click_count in range(3):
                            try:
                                action.click(arrow).perform()
                                time.sleep(0.1)  # ULTRA-OPTIMIZED: Reduced from 0.15s to 0.1s
                                # Check how many images we have now
                                current_imgs = driver.find_elements(By.CSS_SELECTOR, "img.rsImg")
                                current_urls = []
                                for img in current_imgs:
                                    src = img.get_attribute("src") or img.get_attribute("data-src") or img.get_attribute("data-lazy-src")
                                    if src and "placeholder" not in src.lower() and src not in current_urls:
                                        if src.startswith("http") or src.startswith("//"):
                                            current_urls.append(src)
                                if len(current_urls) >= 3:
                                    break
                            except:
                                break
                    except:
                        pass  # No arrow found, continue anyway
                    
                    # ULTRA-OPTIMIZED: Reduced final wait from 0.2s to 0.1s
                    time.sleep(0.1)
                    
                    imgs = driver.find_elements(By.CSS_SELECTOR, "img.rsImg")
                    image_urls = []
                    
                    for im in imgs:
                        # Try multiple attributes for lazy-loaded images
                        src = im.get_attribute("src") or im.get_attribute("data-src") or im.get_attribute("data-lazy-src")
                        if not src:
                            continue
                        
                        # Check data-adid: only skip if it exists AND doesn't match
                        # If data-adid doesn't exist, include the image (less strict)
                        elem_adid = im.get_attribute("data-adid")
                        if elem_adid and str(elem_adid).strip() != ad_id:
                            continue
                        
                        # Filter placeholders and duplicates
                        if "placeholder" not in src.lower() and src not in image_urls:
                            # Make sure it's a valid image URL
                            if src.startswith("http") or src.startswith("//"):
                                image_urls.append(src)
                    
                    df.at[idx, "Image_URLs"] = ",".join(image_urls[:config.max_images])
                    processed += 1
                    listing_time = time.time() - listing_start
                    print(f"[{processed}/{total}] ✅ {ad_id}: {df.at[idx, 'Breadcrumb_Top1']} | {len(image_urls)} imgs | ⏱️ {listing_time:.2f}s")
                except Exception as e:
                    # Fallback: try to get at least one image
                    try:
                        imgs = driver.find_elements(By.CSS_SELECTOR, "img.rsImg")
                        image_urls = []
                        for im in imgs[:10]:  # Check first 10 images
                            src = im.get_attribute("src") or im.get_attribute("data-src") or im.get_attribute("data-lazy-src")
                            if src and "placeholder" not in src.lower() and src not in image_urls:
                                if src.startswith("http") or src.startswith("//"):
                                    image_urls.append(src)
                                    if len(image_urls) >= config.max_images:
                                        break
                        df.at[idx, "Image_URLs"] = ",".join(image_urls[:config.max_images])
                        processed += 1
                        listing_time = time.time() - listing_start
                        print(f"[{processed}/{total}] ✅ {ad_id}: {df.at[idx, 'Breadcrumb_Top1']} | {len(image_urls)} imgs (fallback) | ⏱️ {listing_time:.2f}s")
                    except:
                        df.at[idx, "Image_URLs"] = ""
                        processed += 1
                        listing_time = time.time() - listing_start
                        print(f"[{processed}/{total}] ✅ {ad_id}: {df.at[idx, 'Breadcrumb_Top1']} | 0 imgs | ⏱️ {listing_time:.2f}s")
                    
            except Exception as e:
                df.at[idx, "Breadcrumb_Top1"] = "Inactive ad"
                processed += 1
                listing_time = time.time() - listing_start
                print(f"[{processed}/{total}] ❌ {ad_id}: Error | ⏱️ {listing_time:.2f}s")
            
            time.sleep(0.1)  # ULTRA-OPTIMIZED: Reduced from 0.15s to 0.1s
            
    except Exception as e:
        print(f"❌ Scraper error: {str(e)}")
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
    
    scraping_elapsed = time.time() - scraping_start
    print(f"\n{'='*80}")
    print(f"✅ SCRAPING PHASE COMPLETE: {processed}/{total} ads")
    print(f"⏱️ Total Scraping Time: {scraping_elapsed:.2f}s ({scraping_elapsed/60:.2f} minutes)")
    print(f"⏱️ Average Time per Ad: {scraping_elapsed/max(processed, 1):.2f}s")
    print(f"{'='*80}\n")
    return df


# Import the worker function from separate module (required for Windows multiprocessing)
from scraper_worker import scrape_process_worker


def scrape_ads_parallel(df: pd.DataFrame, job_id: str, num_workers: int = 3):
    """
    MULTIPROCESSING parallel scraper with multiple browser processes.
    
    Uses multiprocessing.Process (same as AI phase) for:
    - Complete process isolation (crash-safe)
    - Better Windows compatibility
    - True parallelism (no GIL)
    - ~3x faster scraping with 3 workers
    
    Features:
    - Staggered random delays to avoid IP ban
    - Automatic driver cleanup (RAM management)
    - Comprehensive terminal logging
    - Graceful shutdown
    
    Args:
        df: DataFrame with Ad IDs
        job_id: Job identifier for tracking
        num_workers: Number of parallel browser processes (default: 3)
        
    Returns:
        DataFrame with scraped data
    """
    import random
    from multiprocessing import Process, Queue
    
    total = len(df)
    scraping_start = time.time()
    
    print(f"\n{'='*80}")
    print(f"🚀 MULTIPROCESSING SCRAPER - {num_workers} PARALLEL BROWSERS")
    print(f"{'='*80}")
    print(f"   📊 Total Ads: {total}")
    print(f"   🖥️  Browsers: {num_workers} isolated processes")
    print(f"   🛡️  Anti-Ban: Random delays (0.2-0.5s) + staggered starts")
    print(f"   💾 RAM: ~250MB per browser, total ~{num_workers * 250}MB")
    print(f"   ⚡ Expected speedup: ~{num_workers}x faster than sequential")
    print(f"{'='*80}\n")

    # Create job queue and result queue
    job_queue = Queue()
    result_queue = Queue()
    
    # Load jobs into queue
    job_count = 0
    for idx, row in df.iterrows():
        ad_id = str(row.get("Ad ID", "")).strip()
        if ad_id:
            job_queue.put((idx, ad_id))
            job_count += 1
    
    print(f"   📦 Loaded {job_count} jobs into queue")
    print(f"   📊 Distribution: ~{job_count // num_workers} ads per worker\n")
    
    # Start worker processes with staggered, random delays
    processes = []
    for i in range(num_workers):
        p = Process(
            target=scrape_process_worker,
            args=(i + 1, job_queue, result_queue, config.max_images)
        )
        p.start()
        processes.append(p)
        print(f"   🚀 Started Worker-{i+1} (PID: {p.pid})")
        # Staggered random delay to avoid resource spike + IP ban prevention
        stagger_delay = random.uniform(0.8, 1.2)
        time.sleep(stagger_delay)
    
    print(f"\n   All {num_workers} workers running. Collecting results...\n")
    
    # Collect results
    results = {}
    completed = 0
    last_progress = 0
    
    while completed < job_count:
        try:
            result = result_queue.get(timeout=60)  # 60s timeout per result
            idx = result["idx"]
            results[idx] = result
            completed += 1
            
            # Progress update every 10 ads
            if completed - last_progress >= 10:
                elapsed = time.time() - scraping_start
                rate = completed / elapsed * 60
                eta = (job_count - completed) / rate if rate > 0 else 0
                print(f"   📈 Progress: {completed}/{job_count} ({completed*100//job_count}%) | Rate: {rate:.0f}/min | ETA: {eta:.0f}s")
                last_progress = completed
                
        except:
            # Check if all processes are still alive
            alive = sum(1 for p in processes if p.is_alive())
            if alive == 0:
                print(f"   ⚠️ All workers finished. Collected {completed}/{job_count}")
                break
    
    # Graceful shutdown
    print(f"\n   🛑 Shutting down workers...")
    for p in processes:
        p.join(timeout=5)
        if p.is_alive():
            p.terminate()
            print(f"   ⚠️ Terminated Worker (PID: {p.pid})")
    
    # Apply results to dataframe
    for idx, result in results.items():
        df.at[idx, "Breadcrumb_Top1"] = result.get("Breadcrumb_Top1", "")
        df.at[idx, "Breadcrumb_Top2"] = result.get("Breadcrumb_Top2", "")
        df.at[idx, "Breadcrumb_Top3"] = result.get("Breadcrumb_Top3", "")
        df.at[idx, "Image_URLs"] = result.get("Image_URLs", "")
    
    scraping_elapsed = time.time() - scraping_start
    processed = len(results)
    rate = processed / scraping_elapsed * 60 if scraping_elapsed > 0 else 0
    
    print(f"\n{'='*80}")
    print(f"✅ MULTIPROCESSING SCRAPING COMPLETE")
    print(f"{'='*80}")
    print(f"   📊 Processed: {processed}/{total} ads")
    print(f"   ⏱️  Total Time: {scraping_elapsed:.1f}s ({scraping_elapsed/60:.1f} min)")
    print(f"   ⏱️  Avg per Ad: {scraping_elapsed/max(processed, 1):.2f}s")
    print(f"   📈 Rate: {rate:.0f} ads/minute")
    print(f"   🚀 Speedup: ~{num_workers}x vs sequential")
    print(f"{'='*80}\n")
    
    return df



def status_queue_drainer(stat_q, job_id: str, stop_event: threading.Event):
    """
    Background thread to continuously drain the status queue.
    This prevents workers from blocking when they try to push status updates.
    Also updates job_progress for real-time tracking.
    """
    worker_status = {}
    
    while not stop_event.is_set():
        try:
            while True:
                msg = stat_q.get_nowait()
                if "worker_id" in msg:
                    w_id = msg["worker_id"]
                    worker_status[w_id] = msg
                    # Update global progress
                    if job_id in job_progress:
                        job_progress[job_id]["workers"] = worker_status.copy()
        except:
            pass
        time.sleep(0.1)  # Small sleep to prevent CPU spin
    
    # Final drain
    try:
        while True:
            stat_q.get_nowait()
    except:
        pass


def run_parallel_ai(df: pd.DataFrame, run_ts: str, job_id: str, num_workers: int = 10):
    """
    Run parallel AI processing with multiple workers - ULTRA-OPTIMIZED for speed
    """
    load_config()
    
    total = len(df)
    ai_start_time = time.time()
    
    print(f"\n{'='*80}")
    print(f"🤖 AI ANNOTATION PHASE STARTED (ULTRA-OPTIMIZED)")
    print(f"{'='*80}")
    print(f"   Total Ads: {total}")
    print(f"   Workers: {num_workers}")
    print(f"   Models:")
    print(f"      📋 Promo Check:         {config.gemini_model_promo_check}")
    print(f"      📋 Classification:      {config.gemini_model_classification}")
    print(f"      📋 Dually Verification: {config.gemini_model_dually_verification}")
    print(f"   API Keys: {len(config.gemini_api_keys)}")
    print(f"   📋 [Rules.json] Will be loaded by each worker from: {config.rules_json}")
    print(f"\n   🔧 DUALLY DETECTION SETTINGS:")
    print(f"      🌑 Darth CV2 (OpenCV) Detection: {'✅ ENABLED' if config.enable_darth_cv2_dually else '❌ DISABLED'}")
    if config.enable_darth_cv2_dually:
        print(f"         └─ Threshold: {config.darth_cv2_dually_threshold} (tire-like contours required)")
    print(f"      🔍 Post-Processing LLM Verification: {'✅ ENABLED' if config.enable_dually_llm_verification else '❌ DISABLED'}")
    print(f"{'='*80}\n")
    
    # Initialize progress tracking
    job_progress[job_id] = {
        "total": int(total),  # Convert numpy.int64 to native int
        "completed": 0,
        "workers": {},
        "start_time": time.time()
    }
    
    # Create manager for shared resources
    m = Manager()
    job_q = m.Queue()
    res_q = m.Queue()
    stat_q = m.Queue()
    key_q = m.Queue()
    
    # Load Keys
    for k in config.gemini_api_keys_info:
        key_q.put(k)
    
    # Load Jobs
    for _, row in df.iterrows():
        job_q.put(row.to_dict())
    
    # Initialize Yoda rate limiter
    print("🧙 Initializing Yoda (Rate Limiter)...")
    yoda = Yoda(config.gemini_api_keys_info, config.rate_limit_rpm, m)
    
    start_time = time.time()
    
    # Start status queue drainer thread (CRITICAL FIX!)
    stop_drain = threading.Event()
    drain_thread = threading.Thread(
        target=status_queue_drainer, 
        args=(stat_q, job_id, stop_drain),
        daemon=True
    )
    drain_thread.start()
    print("   ✅ Status queue drainer started")
    
    # Start Workers
    procs = []
    for i in range(1, num_workers + 1):
        p = Process(
            target=ai_module.start_worker,
            args=(i, run_ts, job_q, res_q, stat_q, key_q, True, False, yoda)
            # high_accuracy=True, use_vision_v2=False
        )
        p.start()
        procs.append(p)
        print(f"   Started Worker-{i} (PID: {p.pid})")
        time.sleep(0.3)  # ULTRA-OPTIMIZED: Reduced from 0.5s to 0.3s
    
    print(f"\n   All {num_workers} workers started. Processing...")
    print(f"   📊 Real-time results will appear below:\n")
    
    results = []
    last_print = 0
    check_count = 0
    listing_times = {}  # Track timing for each listing
    
    # Result Collector (with per-listing timing)
    while any(p.is_alive() for p in procs) or not res_q.empty():
        try:
            r = res_q.get(timeout=2)
            if r and r.get("Ad ID"):
                results.append(r)
                ad_id = r.get("Ad ID", "?")
                status = r.get("Status", "?")
                top1 = r.get("Annotated_Top1", "?")
                
                # Calculate timing
                current_time = time.time()
                elapsed_since_start = current_time - start_time
                avg_time_per_listing = elapsed_since_start / len(results) if len(results) > 0 else 0
                
                print(f"   ✅ [{len(results)}/{total}] {ad_id}: {top1} | Status: {status} | ⏱️ Avg: {avg_time_per_listing:.2f}s/ad")
                last_print = len(results)
                
                # Update progress tracking
                if job_id in job_progress:
                    job_progress[job_id]["completed"] = int(len(results))  # Convert to native int
        except queue.Empty:
            check_count += 1
            # Every 10 checks (~20 seconds), show status
            if check_count % 10 == 0:
                alive = sum(1 for p in procs if p.is_alive())
                elapsed = int(time.time() - start_time)
                rate = len(results) / elapsed if elapsed > 0 else 0
                eta = (total - len(results)) / rate if rate > 0 else 0
                print(f"   ⏳ Progress: {len(results)}/{total} done | {alive} workers active | ⏱️ {elapsed}s elapsed | ETA: {eta:.0f}s")
            continue
    
    # Stop the drain thread
    stop_drain.set()
    drain_thread.join(timeout=2)
    
    # Wait for workers to finish
    for p in procs:
        p.join(timeout=10)
        if p.is_alive():
            p.terminate()
    
    # Drain remaining results
    while not res_q.empty():
        try:
            r = res_q.get_nowait()
            if r and r.get("Ad ID"):
                results.append(r)
        except:
            break
    
    elapsed = time.time() - start_time
    avg_time = elapsed / len(results) if len(results) > 0 else 0
    
    print(f"\n{'='*80}")
    print(f"✅ AI ANNOTATION PHASE COMPLETE")
    print(f"{'='*80}")
    print(f"   Processed: {len(results)}/{total} ads")
    print(f"   ⏱️ Total Time: {elapsed:.2f}s ({elapsed/60:.2f} minutes)")
    print(f"   ⏱️ Average Time per Ad: {avg_time:.2f}s")
    print(f"   📊 Processing Rate: {len(results)/elapsed*60:.1f} ads/minute")
    print(f"{'='*80}\n")
    
    # Update final progress
    if job_id in job_progress:
        job_progress[job_id]["completed"] = int(len(results))  # Convert to native int
        job_progress[job_id]["elapsed"] = int(elapsed)
    
    # Create output dataframe
    if not results:
        return pd.DataFrame()
    
    result_df = pd.DataFrame(results)
    result_df["Ad ID"] = result_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # Ensure all columns exist
    final_columns = [
        "Ad ID", "Breadcrumb_Top1", "Breadcrumb_Top2", "Breadcrumb_Top3",
        "Annotated_Top1", "Annotated_Top2", "Annotated_Top3",
        "Annotated_Top1_Score", "Annotated_Top2_Score", "Annotated_Top3_Score",
        "Image_Count", "Image_URLs", "Status", "Cost_Cents"
    ]
    for col in final_columns:
        if col not in result_df.columns:
            result_df[col] = ""
    
    # ✅ FIX: Preserve original input order by merging from input DataFrame (use "left" to maintain order)
    clean_input_df = df[["Ad ID"]].copy()
    clean_input_df["Ad ID"] = clean_input_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    result_df = pd.merge(clean_input_df, result_df, on="Ad ID", how="left") 
    
    return result_df[final_columns]


def verify_dually_listings(result_df: pd.DataFrame, job_id: str, yoda_instance):
    """
    MULTI-THREADED Post-processing step: LLM verification for Dually false positives with 5 workers.
    
    This function:
    1. Identifies all listings marked with "Dually" in any annotation column
    2. Pre-fetches all images to avoid delays during LLM calls
    3. Distributes verification jobs across 5 workers for parallel processing
    4. Makes LLM calls to verify if each one really has dual rear wheels
    5. Removes "Dually" from annotations if the LLM says it's a false positive
    
    Returns: (Updated DataFrame, verification_cost_cents)
    """
    from ai_tool.utils import calculate_cost_cents
    
    verification_start = time.time()
    
    print("\n" + "="*80)
    print("🔍 DUALLY VERIFICATION PHASE - Multi-Threaded with 5 Workers (ULTRA-OPTIMIZED)")
    print("="*80)
    
    # Find all rows that have "Dually" in any annotation column
    annotation_cols = ["Annotated_Top1", "Annotated_Top2", "Annotated_Top3"]
    
    dually_mask = result_df[annotation_cols].apply(
        lambda row: any("dually" in str(val).lower() for val in row), 
        axis=1
    )
    
    dually_listings = result_df[dually_mask].copy()
    
    if len(dually_listings) == 0:
        print("   No listings marked as Dually. Skipping verification.")
        return result_df, 0  # Return 0 cost when no verification needed
    
    total_dually = len(dually_listings)
    num_workers = 5  # Fixed to 5 workers as requested
    
    print(f"   Found {total_dually} listings marked as Dually")
    print(f"   🧵 Using {num_workers} workers for parallel verification")
    print(f"   📊 Expected speedup: ~{num_workers}x faster than sequential processing")
    
    # Update job status for frontend
    if job_id in jobs:
        jobs[job_id]['status'] = JobStatus.VERIFYING_DUALLY
        jobs[job_id]['dually_total'] = int(total_dually)
        jobs[job_id]['dually_verified'] = 0
    
    # STEP 1: Pre-fetch all images first (PARALLEL for speed)
    if getattr(config, 'verbose_cache_logging', True):
        print("\n   📥 STEP 1: Pre-fetching ALL images from cache...")
    prefetch_start = time.time()
    prefetched_images = {}
    
    # Helper function for parallel fetching
    def fetch_images_for_ad(idx_row_tuple):
        idx, row = idx_row_tuple
        ad_id = str(row.get("Ad ID", "")).strip()
        image_urls_str = str(row.get("Image_URLs", "")).strip()
        if image_urls_str:
            image_urls = [url.strip() for url in image_urls_str.split(",") if url.strip()]
            if image_urls:
                # Get only first 10 images (same as scrapping feature for consistency)
                img_bytes_list = web_utils.get_images_with_caching(image_urls[:9])
                # Filter out None/empty images
                valid_images = [img for img in img_bytes_list if img]
                if valid_images:
                    print(f"   📸 Ad {ad_id}: Pre-fetched {len(valid_images)} image(s)")
                    return (idx, ad_id, valid_images, row)
        return None
    
    # Use ThreadPoolExecutor for parallel I/O-bound image fetching
    # 10 concurrent threads for downloading images (I/O bound, so more threads = faster)
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_images_for_ad, (idx, row)) 
                   for idx, row in dually_listings.iterrows()]
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                idx, ad_id, valid_images, row = result
                prefetched_images[idx] = (ad_id, valid_images, row)
    
    prefetch_time = time.time() - prefetch_start
    print(f"   ✅ Pre-fetched images for {len(prefetched_images)} listings in {prefetch_time:.2f}s (PARALLEL)")
    
    if len(prefetched_images) == 0:
        print("   ⚠️ No images available for any dually listings. Skipping verification.")
        return result_df, 0
    
    # STEP 2: Setup multiprocessing resources
    print(f"\n   🔧 STEP 2: Setting up {num_workers} workers...")
    m = Manager()
    job_q = m.Queue()
    res_q = m.Queue()
    stat_q = m.Queue()
    key_q = m.Queue()
    
    # Load Keys
    for k in config.gemini_api_keys_info:
        key_q.put(k)
    print(f"   🔑 Loaded {len(config.gemini_api_keys_info)} API keys into queue")
    
    # Load verification jobs into queue (skip rule-based dually that shouldn't be verified)
    print(f"   📋 Loading verification jobs into queue...")
    skipped_rule_based = 0
    for idx, (ad_id, img_bytes_list, row) in prefetched_images.items():
        # Skip verification for Landscape + Cabover COE — always dually by rule
        annotations = [
            str(row.get("Annotated_Top1", "")).lower(),
            str(row.get("Annotated_Top2", "")).lower(),
            str(row.get("Annotated_Top3", "")).lower()
        ]
        has_landscape = any("landscape" in a for a in annotations)
        has_cabover_coe = any("cabover" in a and "coe" in a for a in annotations)
        if has_landscape and has_cabover_coe:
            skipped_rule_based += 1
            print(f"   ⏭️ Ad {ad_id}: Skipping verification — Landscape + Cabover COE (always dually by rule)")
            continue

        verification_job = {
            'idx': idx,
            'ad_id': ad_id,
            'images': img_bytes_list,
            'row_data': row.to_dict()
        }
        job_q.put(verification_job)
        print(f"   ➕ Added verification job for Ad {ad_id} to queue")

    if skipped_rule_based > 0:
        print(f"   ⏭️ Skipped {skipped_rule_based} listings (rule-based dually, no verification needed)")
    
    jobs_loaded = len(prefetched_images) - skipped_rule_based
    print(f"   ✅ {jobs_loaded} jobs loaded into queue ({skipped_rule_based} skipped as rule-based dually)")

    if jobs_loaded == 0:
        print("   ⚠️ No listings need dually verification after rule-based filtering. Skipping.")
        return result_df, 0

    # STEP 3: Start worker processes
    print(f"\n   🚀 STEP 3: Starting {num_workers} worker processes...")
    procs = []
    for i in range(1, num_workers + 1):
        p = Process(
            target=ai_module.start_dually_verification_worker,
            args=(i, job_q, res_q, stat_q, key_q, yoda_instance)
        )
        p.start()
        procs.append(p)
        print(f"   ✅ Started Dually Verification Worker-{i} (PID: {p.pid})")
        time.sleep(0.3)  # Small delay to avoid race conditions
    
    print(f"\n   🏃 All {num_workers} workers started. Beginning parallel verification...")
    print(f"   📊 Real-time results will appear below:\n")
    
    # STEP 4: Collect results from workers
    verification_loop_start = time.time()
    verified_count = 0
    removed_count = 0
    error_count = 0
    total_cost = 0
    results_collected = 0
    
    print("="*80)
    print("📊 REAL-TIME VERIFICATION RESULTS")
    print("="*80 + "\n")
    
    # Result Collector
    while any(p.is_alive() for p in procs) or not res_q.empty():
        try:
            result = res_q.get(timeout=2)
            results_collected += 1
            current_num = results_collected
            
            idx = result['idx']
            ad_id = result['ad_id']
            is_dually = result['is_dually']
            cost = result.get('cost', 0)
            listing_time = result.get('listing_time', 0)
            success = result.get('success', True)
            
            # Update job progress for frontend
            if job_id in jobs:
                jobs[job_id]['dually_verified'] = int(current_num)
            
            # Calculate timing stats
            elapsed = time.time() - verification_loop_start
            avg_time = elapsed / current_num if current_num > 0 else 0
            rate = current_num / elapsed if elapsed > 0 else 0
            eta = (total_dually - current_num) / rate if rate > 0 else 0
            
            if not success:
                error_count += 1
                error_msg = result.get('error', 'Unknown error')
                print(f"   [{current_num}/{total_dually}] ⚠️ {ad_id}: ERROR - {error_msg[:40]} | ⏱️ {listing_time:.2f}s | ETA: {eta:.0f}s")
            else:
                # Update cost tracking
                total_cost += cost
                
                # ADD verification cost to this listing's Cost_Cents in the dataframe
                current_cost = result_df.at[idx, 'Cost_Cents'] if 'Cost_Cents' in result_df.columns else 0
                try:
                    current_cost = float(current_cost) if pd.notna(current_cost) else 0
                except:
                    current_cost = 0
                
                new_cost = current_cost + cost
                result_df.at[idx, 'Cost_Cents'] = new_cost
                
                if is_dually:
                    verified_count += 1
                    if getattr(config, 'verbose_cache_logging', True):
                        print(f"   [{current_num}/{total_dually}] ✅ {ad_id}: CONFIRMED | Cost: {cost:.4f}¢ → Total: {new_cost:.4f}¢ | Time: {listing_time:.2f}s | Avg: {avg_time:.2f}s | Rate: {rate*60:.1f}/min | ETA: {eta:.0f}s")
                    
                    # RECALCULATE STATUS for confirmed dually
                    breadcrumbs = [
                        str(result_df.at[idx, "Breadcrumb_Top1"]).strip() if pd.notna(result_df.at[idx, "Breadcrumb_Top1"]) else "",
                        str(result_df.at[idx, "Breadcrumb_Top2"]).strip() if pd.notna(result_df.at[idx, "Breadcrumb_Top2"]) else "",
                        str(result_df.at[idx, "Breadcrumb_Top3"]).strip() if pd.notna(result_df.at[idx, "Breadcrumb_Top3"]) else ""
                    ]
                    annotations = [
                        str(result_df.at[idx, "Annotated_Top1"]).strip() if pd.notna(result_df.at[idx, "Annotated_Top1"]) else "",
                        str(result_df.at[idx, "Annotated_Top2"]).strip() if pd.notna(result_df.at[idx, "Annotated_Top2"]) else "",
                        str(result_df.at[idx, "Annotated_Top3"]).strip() if pd.notna(result_df.at[idx, "Annotated_Top3"]) else ""
                    ]
                    
                    bc_set = {b.lower() for b in breadcrumbs if b}
                    ann_set = {a.lower() for a in annotations if a}
                    result_df.at[idx, "Status"] = "No change" if bc_set == ann_set else "Require Update"
                    
                else:
                    removed_count += 1
                    if getattr(config, 'verbose_cache_logging', True):
                        print(f"   [{current_num}/{total_dually}] ❌ {ad_id}: FALSE POSITIVE - REMOVING | Cost: {cost:.4f}¢ → Total: {new_cost:.4f}¢ | Time: {listing_time:.2f}s | Avg: {avg_time:.2f}s | Rate: {rate*60:.1f}/min | ETA: {eta:.0f}s")
                    
                    # Remove "Dually" from each annotation column
                    for col in annotation_cols:
                        val = str(result_df.at[idx, col]).strip()
                        if "dually" in val.lower():
                            if " dually" in val.lower():
                                result_df.at[idx, col] = val.lower().replace(" dually", "").title().strip()
                            elif "dually " in val.lower():
                                result_df.at[idx, col] = val.lower().replace("dually ", "").title().strip()
                            elif val.lower() == "dually":
                                result_df.at[idx, col] = ""
                            else:
                                result_df.at[idx, col] = val.replace("Dually", "").replace("dually", "").strip()
                    
                    # Shift annotations up if Annotated_Top1 became empty
                    if not result_df.at[idx, "Annotated_Top1"]:
                        result_df.at[idx, "Annotated_Top1"] = result_df.at[idx, "Annotated_Top2"]
                        result_df.at[idx, "Annotated_Top1_Score"] = result_df.at[idx, "Annotated_Top2_Score"]
                        result_df.at[idx, "Annotated_Top2"] = result_df.at[idx, "Annotated_Top3"]
                        result_df.at[idx, "Annotated_Top2_Score"] = result_df.at[idx, "Annotated_Top3_Score"]
                        result_df.at[idx, "Annotated_Top3"] = ""
                        result_df.at[idx, "Annotated_Top3_Score"] = 0
                    
                    # RECALCULATE STATUS after removing Dually
                    breadcrumbs = [
                        str(result_df.at[idx, "Breadcrumb_Top1"]).strip() if pd.notna(result_df.at[idx, "Breadcrumb_Top1"]) else "",
                        str(result_df.at[idx, "Breadcrumb_Top2"]).strip() if pd.notna(result_df.at[idx, "Breadcrumb_Top2"]) else "",
                        str(result_df.at[idx, "Breadcrumb_Top3"]).strip() if pd.notna(result_df.at[idx, "Breadcrumb_Top3"]) else ""
                    ]
                    annotations = [
                        str(result_df.at[idx, "Annotated_Top1"]).strip() if pd.notna(result_df.at[idx, "Annotated_Top1"]) else "",
                        str(result_df.at[idx, "Annotated_Top2"]).strip() if pd.notna(result_df.at[idx, "Annotated_Top2"]) else "",
                        str(result_df.at[idx, "Annotated_Top3"]).strip() if pd.notna(result_df.at[idx, "Annotated_Top3"]) else ""
                    ]
                    
                    bc_set = {b.lower() for b in breadcrumbs if b}
                    ann_set = {a.lower() for a in annotations if a}
                    result_df.at[idx, "Status"] = "No change" if bc_set == ann_set else "Require Update"
            
        except queue.Empty:
            # Check if all workers are still alive
            alive = sum(1 for p in procs if p.is_alive())
            if alive == 0 and res_q.empty():
                print(f"   ⏳ All workers finished. Breaking out of result collection loop.")
                break
            continue
    
    # Wait for all workers to finish
    print(f"\n   ⏳ Waiting for all workers to complete...")
    for i, p in enumerate(procs, 1):
        p.join(timeout=30)
        if p.is_alive():
            print(f"   ⚠️ Worker-{i} did not finish in time, terminating...")
            p.terminate()
            p.join()
        else:
            print(f"   ✅ Worker-{i} finished successfully")
    
    verification_loop_elapsed = time.time() - verification_loop_start
    total_verification_elapsed = time.time() - verification_start
    avg_verify_time = verification_loop_elapsed / total_dually if total_dually > 0 else 0
    
    # NO SLEEP HERE - Workers handle everything!
    
    verification_loop_elapsed = time.time() - verification_loop_start
    total_verification_elapsed = time.time() - verification_start
    avg_verify_time = verification_loop_elapsed / results_collected if results_collected > 0 else 0
    
    # VERIFICATION: Check that costs were actually added to the DataFrame
    if getattr(config, 'verbose_cache_logging', True):
        print("\n" + "="*80)
        print("📊 COST VERIFICATION: Checking Cost_Cents Updates")
        print("="*80)
        cost_sum_in_df = result_df['Cost_Cents'].sum() if 'Cost_Cents' in result_df.columns else 0
        print(f"   Total Cost_Cents in DataFrame: {cost_sum_in_df:.4f}¢")
        print(f"   Dually verification costs calculated: {total_cost:.4f}¢")
        print("="*80 + "\n")
    
    print("\n" + "="*80)
    print("🔍 DUALLY VERIFICATION PHASE COMPLETE - MULTI-THREADED RESULTS")
    print("="*80)
    print(f"   🧵 Workers Used: {num_workers}")
    print(f"   📋 Total Checked: {total_dually}")
    print(f"   ✅ Confirmed: {verified_count}")
    print(f"   ❌ Removed (False Positives): {removed_count}")
    print(f"   ⚠️ Errors: {error_count}")
    print(f"   💰 Dually Verification Cost: {total_cost:.4f}¢")
    print(f"\n   ⏱️ PERFORMANCE METRICS:")
    print(f"      Total Time: {total_verification_elapsed:.2f}s ({total_verification_elapsed/60:.2f} minutes)")
    print(f"      Verification Loop Time: {verification_loop_elapsed:.2f}s")
    print(f"      Average Time per Listing: {avg_verify_time:.2f}s")
    print(f"      Verification Rate: {results_collected/verification_loop_elapsed*60:.1f} listings/minute")
    print(f"      Speedup vs Sequential: ~{num_workers}x faster")
    print("="*80 + "\n")
    
    # Update job status back to processing for final save
    if job_id in jobs:
        jobs[job_id]['status'] = JobStatus.PROCESSING
        jobs[job_id]['dually_verified'] = int(results_collected)
        jobs[job_id]['dually_removed'] = int(removed_count)
        jobs[job_id]['dually_verification_cost'] = float(total_cost)
    
    # FINAL STEP: Recalculate ALL statuses after verification with proper normalization
    print("   🔄 STEP 5: Recalculating all statuses after verification...")
    
    # Load normalization rules
    from ai_tool.data_processing import load_rules, normalize_text
    rules = load_rules(config.rules_json)
    norm_map = rules.get('normalize_map', {})
    
    status_updated_count = 0
    for idx, row in result_df.iterrows():
        # Skip inactive ads, errors, and no-image cases
        current_status = str(row.get("Status", "")).strip()
        if any(x in current_status.lower() for x in ["inactive", "error", "image not clear", "no images present"]):
            continue
        
        # Get breadcrumbs and annotations
        breadcrumbs = [
            str(row.get("Breadcrumb_Top1", "")).strip() if pd.notna(row.get("Breadcrumb_Top1")) else "",
            str(row.get("Breadcrumb_Top2", "")).strip() if pd.notna(row.get("Breadcrumb_Top2")) else "",
            str(row.get("Breadcrumb_Top3", "")).strip() if pd.notna(row.get("Breadcrumb_Top3")) else ""
        ]
        annotations = [
            str(row.get("Annotated_Top1", "")).strip() if pd.notna(row.get("Annotated_Top1")) else "",
            str(row.get("Annotated_Top2", "")).strip() if pd.notna(row.get("Annotated_Top2")) else "",
            str(row.get("Annotated_Top3", "")).strip() if pd.notna(row.get("Annotated_Top3")) else ""
        ]
        
        # Normalize each value using the normalization map (same as annotation phase)
        bc_normalized = {normalize_text(b, norm_map).lower() for b in breadcrumbs if b}
        ann_normalized = {normalize_text(a, norm_map).lower() for a in annotations if a}
        
        # Calculate new status
        new_status = "No change" if bc_normalized == ann_normalized else "Require Update"
        
        # Update if changed
        if new_status != current_status:
            result_df.at[idx, "Status"] = new_status
            status_updated_count += 1
    
    print(f"   ✅ Status recalculation complete: {status_updated_count} status(es) corrected\n")
    
    return result_df, total_cost  # Return both dataframe and verification cost


def run_job_pipeline_sync(job_id: str, file_path: str):
    """Main pipeline: Scraping -> Parallel AI Processing (runs synchronously)"""
    job = jobs[job_id]
    run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    try:
        # Load input file
        df = pd.read_excel(file_path, dtype={"Ad ID": str})
        df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # ✅ FIX: Deduplicate Ad IDs BEFORE processing to prevent exponential growth
        duplicates_before = df[df.duplicated(subset=["Ad ID"], keep=False)]
        if not duplicates_before.empty:
            print(f"   ⚠️ WARNING: Found {len(duplicates_before)} duplicate rows in input Excel!")
            print(f"   ⚠️ Duplicate Ad IDs: {duplicates_before['Ad ID'].unique().tolist()}")
            print(f"   ✅ Removing duplicates, keeping first occurrence...")
            df = df.drop_duplicates(subset=["Ad ID"], keep='first').reset_index(drop=True)
            print(f"   ✅ After deduplication: {len(df)} unique Ad IDs")
        
        # Add required columns
        for col in ["Breadcrumb_Top1", "Breadcrumb_Top2", "Breadcrumb_Top3", "Image_URLs"]:
            if col not in df.columns:
                df[col] = ""
        
        job['total_ads'] = int(len(df))  # Convert numpy.int64 to native int
        job['status'] = JobStatus.SCRAPING
        
        print(f"\n{'='*60}")
        print(f"JOB {job_id} STARTED")
        print(f"File: {job.get('filename')}")
        print(f"Total Ads: {len(df)}")
        print(f"{'='*60}\n")
        
        # ── Ad Annotation Limit: Pre-filter (before scraping) ──
        if config.enable_ad_annotation_limit:
            over_limit = ad_tracker.filter_over_limit_ads(df["Ad ID"].tolist(), config.max_annotation_runs)
            if over_limit:
                print(f"   🚫 Ad Tracker: Skipping {len(over_limit)} ads (already annotated {config.max_annotation_runs}+ times)")
                print(f"   🚫 Skipped Ad IDs: {sorted(over_limit)[:10]}{'...' if len(over_limit) > 10 else ''}")
                df = df[~df["Ad ID"].isin(over_limit)].reset_index(drop=True)
                job['total_ads'] = int(len(df))
                print(f"   ✅ Ad Tracker: {len(df)} ads remaining after filter\n")
            else:
                print(f"   ✅ Ad Tracker: All {len(df)} ads are within annotation limit\n")
        
        # Phase 1: MULTIPROCESSING Scraping with 3 workers (~3x faster)
        df = scrape_ads_parallel(df, job_id, num_workers=5)
        
        # Fallback to synchronous scraper if needed (for troubleshooting):
        # df = scrape_ads_sync(df, job_id)

        # Save scraped data (skipped if EnableScrapperOutput=False — df already in memory, file not read back)
        if getattr(config, 'enable_scrapper_output', True):
            os.makedirs(config.scrapper_output_dir, exist_ok=True)
            scraper_output_path = os.path.join(config.scrapper_output_dir, f"Scrapper_{run_ts}.xlsx")
            df.to_excel(scraper_output_path, index=False)
        
        job['status'] = JobStatus.PROCESSING
        
        # Phase 2: Parallel AI Processing - ULTRA-OPTIMIZED with more workers
        # Increase workers from 5 to 10 for 2x faster processing
        # num_workers = min(10, max(1, len(config.gemini_api_keys)))
        num_workers = 5
        print(f"\n🤖 Using {num_workers} parallel workers for faster processing")
        result_df = run_parallel_ai(df, run_ts, job_id, num_workers)
        
        # Phase 3: Dually Verification - LLM double-check for false positives
        # Controlled by config.enable_dually_llm_verification flag
        dually_verification_cost = 0
        if not result_df.empty:
            if config.enable_dually_llm_verification:
                print("\n" + "="*60)
                print("🔍 DUALLY LLM VERIFICATION: ✅ ENABLED")
                print("   Starting post-processing verification for Dually annotations...")
                print("="*60)
                # Create a new Yoda instance for verification
                m_verify = Manager()
                yoda_verify = Yoda(config.gemini_api_keys_info, config.rate_limit_rpm, m_verify)
                result_df, dually_verification_cost = verify_dually_listings(result_df, job_id, yoda_verify)
            else:
                print("\n" + "="*60)
                print("🔍 DUALLY LLM VERIFICATION: ❌ DISABLED (Skipping)")
                print("   Post-processing verification is turned OFF in config.ini")
                print("   Set 'EnableDuallyLLMVerification = True' to enable")
                print("="*60)
        
        # ── Ad Annotation Limit: Post-increment for successful ads ──
        if config.enable_ad_annotation_limit and not result_df.empty:
            if "Status" in result_df.columns:
                successful_ids = result_df[result_df["Status"] != "AI Error"]["Ad ID"].tolist()
            else:
                successful_ids = result_df["Ad ID"].tolist()
            if successful_ids:
                ad_tracker.increment_annotation_counts(successful_ids)
                print(f"   📊 Ad Tracker: Updated annotation counts for {len(successful_ids)} successfully processed ads in Turso DB")
        
        # ✅ FIX: Deduplicate result DataFrame before saving to prevent duplicate entries
        if not result_df.empty:
            duplicates_final = result_df[result_df.duplicated(subset=["Ad ID"], keep=False)]
            if not duplicates_final.empty:
                print(f"   ⚠️ WARNING: Found {len(duplicates_final)} duplicate rows in final result!")
                print(f"   ⚠️ Duplicate Ad IDs: {duplicates_final['Ad ID'].unique().tolist()}")
                print(f"   ✅ Removing duplicates, keeping first occurrence...")
                result_df = result_df.drop_duplicates(subset=["Ad ID"], keep='first').reset_index(drop=True)
                print(f"   ✅ After deduplication: {len(result_df)} unique rows")
        
        # Save final output
        output_filename = f"output_annotated_{run_ts}.xlsx"
        if getattr(config, 'enable_ai_output', True):
            os.makedirs(config.output_dir, exist_ok=True)
            output_path = os.path.join(config.output_dir, output_filename)
        else:
            output_path = _temp_path(output_filename)
            _register_temp_file(output_path)
        result_df.to_excel(output_path, index=False)

        job['status'] = JobStatus.COMPLETED
        job['output_file'] = output_path
        job['output_filename'] = output_filename

        # Calculate summary - Cost_Cents already includes dually verification costs (added in line 737)
        total_cost_in_df = result_df['Cost_Cents'].sum() if 'Cost_Cents' in result_df.columns else 0
        
        # For reporting: separate annotation cost (before dually) and dually cost
        annotation_cost_only = total_cost_in_df - dually_verification_cost  # Back-calculate annotation-only cost
        
        job['total_cost'] = float(total_cost_in_df)  # Total cost (includes dually costs already)
        job['annotation_cost'] = float(annotation_cost_only)  # Annotation cost without dually
        job['dually_verification_cost'] = float(dually_verification_cost)  # Separate dually cost for reporting
        
        # Merge session reports
        merge_all_session_reports(run_ts)
        
        print(f"\n{'='*60}")
        print(f"🎉 JOB {job_id} COMPLETE!")
        print(f"Output: {output_filename}")
        print(f"💰 Annotation Cost: {annotation_cost_only}¢")
        print(f"💰 Dually Verification Cost: {dually_verification_cost}¢")
        print(f"💰 TOTAL COST: {total_cost_in_df}¢")
        print(f"{'='*60}\n")
        
    except Exception as e:
        job['status'] = JobStatus.FAILED
        job['error'] = str(e)
        print(f"\n❌ JOB {job_id} FAILED: {str(e)}\n")
        import traceback
        traceback.print_exc()


async def run_job_pipeline(job_id: str, file_path: str):
    """Async wrapper for the pipeline"""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, run_job_pipeline_sync, job_id, file_path)


def run_reannotation_pipeline_sync(job_id: str, file_path: str):
    """Reannotation pipeline: Skip scraping, go directly to AI annotation"""
    job = jobs[job_id]
    run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    try:
        # Load already-scraped file
        df = pd.read_excel(file_path, dtype={"Ad ID": str})
        df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # Validate required columns exist
        required_cols = ["Ad ID", "Breadcrumb_Top1", "Image_URLs"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns for reannotation: {', '.join(missing)}")
        
        # Ensure all breadcrumb columns exist
        for col in ["Breadcrumb_Top2", "Breadcrumb_Top3"]:
            if col not in df.columns:
                df[col] = ""
        
        job['total_ads'] = int(len(df))  # Convert numpy.int64 to native int
        job['status'] = JobStatus.PROCESSING
        
        print(f"\n{'='*60}")
        print(f"🔄 RE-ANNOTATION JOB {job_id} STARTED")
        print(f"File: {job.get('filename')}")
        print(f"Total Ads: {len(df)}")
        print(f"Skipping scraping - using existing data")
        print(f"{'='*60}\n")
        
        # ── Ad Annotation Limit: Pre-filter (before AI) ──
        if config.enable_ad_annotation_limit:
            over_limit = ad_tracker.filter_over_limit_ads(df["Ad ID"].tolist(), config.max_annotation_runs)
            if over_limit:
                print(f"   🚫 Ad Tracker: Skipping {len(over_limit)} ads (already annotated {config.max_annotation_runs}+ times)")
                print(f"   🚫 Skipped Ad IDs: {sorted(over_limit)[:10]}{'...' if len(over_limit) > 10 else ''}")
                df = df[~df["Ad ID"].isin(over_limit)].reset_index(drop=True)
                job['total_ads'] = int(len(df))
                print(f"   ✅ Ad Tracker: {len(df)} ads remaining after filter\n")
            else:
                print(f"   ✅ Ad Tracker: All {len(df)} ads are within annotation limit\n")
        
        # Phase 1: Parallel AI Processing (no scraping) - ULTRA-OPTIMIZED
        num_workers = min(10, max(1, len(config.gemini_api_keys)))
        print(f"\n🤖 Using {num_workers} parallel workers for faster processing")
        result_df = run_parallel_ai(df, run_ts, job_id, num_workers)
        
        # Phase 2: Dually Verification (if enabled)
        dually_verification_cost = 0
        if not result_df.empty:
            if config.enable_dually_llm_verification:
                print("\n" + "="*60)
                print("🔍 DUALLY LLM VERIFICATION: ✅ ENABLED")
                print("   Starting post-processing verification for Dually annotations...")
                print("="*60)
                m_verify = Manager()
                yoda_verify = Yoda(config.gemini_api_keys_info, config.rate_limit_rpm, m_verify)
                result_df, dually_verification_cost = verify_dually_listings(result_df, job_id, yoda_verify)
            else:
                print("\n" + "="*60)
                print("🔍 DUALLY LLM VERIFICATION: ❌ DISABLED (Skipping)")
                print("="*60)
        
        # ── Ad Annotation Limit: Post-increment for successful ads ──
        if config.enable_ad_annotation_limit and not result_df.empty:
            if "Status" in result_df.columns:
                successful_ids = result_df[result_df["Status"] != "AI Error"]["Ad ID"].tolist()
            else:
                successful_ids = result_df["Ad ID"].tolist()
            if successful_ids:
                ad_tracker.increment_annotation_counts(successful_ids)
                print(f"   📊 Ad Tracker: Updated annotation counts for {len(successful_ids)} successfully processed ads in Turso DB")
        
        # Save final output
        output_filename = f"output_reannotated_{run_ts}.xlsx"
        if getattr(config, 'enable_ai_output', True):
            os.makedirs(config.output_dir, exist_ok=True)
            output_path = os.path.join(config.output_dir, output_filename)
        else:
            output_path = _temp_path(output_filename)
            _register_temp_file(output_path)
        result_df.to_excel(output_path, index=False)

        job['status'] = JobStatus.COMPLETED
        job['output_file'] = output_path
        job['output_filename'] = output_filename

        # Calculate summary
        # Calculate summary - Cost_Cents already includes dually verification costs (added in line 737)
        total_cost_in_df = result_df['Cost_Cents'].sum() if 'Cost_Cents' in result_df.columns else 0
        annotation_cost_only = total_cost_in_df - dually_verification_cost  # Back-calculate annotation-only cost
        
        job['total_cost'] = float(total_cost_in_df)  # Total cost (includes dually costs already)
        job['annotation_cost'] = float(annotation_cost_only)  # Annotation cost without dually
        job['dually_verification_cost'] = float(dually_verification_cost)  # Separate dually cost for reporting
        
        # Merge session reports
        merge_all_session_reports(run_ts)
        
        print(f"\n{'='*60}")
        print(f"🎉 RE-ANNOTATION JOB {job_id} COMPLETE!")
        print(f"Output: {output_filename}")
        print(f"💰 Annotation Cost: {annotation_cost_only}¢")
        print(f"💰 Dually Verification Cost: {dually_verification_cost}¢")
        print(f"💰 TOTAL COST: {total_cost_in_df}¢")
        print(f"{'='*60}\n")
        
    except Exception as e:
        job['status'] = JobStatus.FAILED
        job['error'] = str(e)
        print(f"\n❌ RE-ANNOTATION JOB {job_id} FAILED: {str(e)}\n")
        import traceback
        traceback.print_exc()


async def run_reannotation_pipeline(job_id: str, file_path: str):
    """Async wrapper for the reannotation pipeline"""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, run_reannotation_pipeline_sync, job_id, file_path)


def run_db_annotation_pipeline_sync(job_id: str, file_path: str):
    """
    AI Annotation pipeline for already-fetched database data — BATCH MODE (500 per batch)
    
    Processes in batches of 500 listings. Each batch runs the COMPLETE pipeline:
    1. AI annotation (promo check, refinement, classification)
    2. Dually verification (if enabled)
    3. Save batch Excel (downloadable immediately)
    
    After all batches: combines into one final complete file.
    Handles any size: 700→[500,200], 300→[300], 1500→[500,500,500]
    """
    BATCH_SIZE = 500
    job = jobs[job_id]
    run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    try:
        print(f"\n{'='*80}")
        print(f"🤖 AI ANNOTATION JOB {job_id} STARTED — BATCH MODE (DB Fetched Data)")
        print(f"{'='*80}")
        print(f"   Source File: {os.path.basename(file_path)}")
        print(f"   Batch Size: {BATCH_SIZE}")
        print(f"{'='*80}\n")
        
        # Load already-fetched data
        df = pd.read_excel(file_path, dtype={"Ad ID": str})
        df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # Validate required columns exist
        required_cols = ["Ad ID", "Breadcrumb_Top1", "Image_URLs"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {', '.join(missing)}")
        
        # Ensure all breadcrumb columns exist
        for col in ["Breadcrumb_Top2", "Breadcrumb_Top3"]:
            if col not in df.columns:
                df[col] = ""
        
        # ── Ad Annotation Limit: Pre-filter (before batch loop) ──
        if config.enable_ad_annotation_limit:
            over_limit = ad_tracker.filter_over_limit_ads(df["Ad ID"].tolist(), config.max_annotation_runs)
            if over_limit:
                print(f"   🚫 Ad Tracker: Skipping {len(over_limit)} ads (already annotated {config.max_annotation_runs}+ times)")
                print(f"   🚫 Skipped Ad IDs: {sorted(over_limit)[:10]}{'...' if len(over_limit) > 10 else ''}")
                df = df[~df["Ad ID"].isin(over_limit)].reset_index(drop=True)
                print(f"   ✅ Ad Tracker: {len(df)} ads remaining after filter\n")
            else:
                print(f"   ✅ Ad Tracker: All {len(df)} ads are within annotation limit\n")
        
        total_listings = len(df)
        job['total_ads'] = int(total_listings)
        
        # Calculate batch plan
        num_batches = (total_listings + BATCH_SIZE - 1) // BATCH_SIZE  # ceil division
        job['batches'] = []
        job['total_batches'] = num_batches
        job['current_batch'] = 0
        
        print(f"   ✅ Loaded {total_listings} ads from fetched data")
        print(f"   📦 Will process in {num_batches} batch(es): ", end="")
        for b in range(num_batches):
            start = b * BATCH_SIZE
            end = min(start + BATCH_SIZE, total_listings)
            print(f"[{start+1}-{end}]", end=" ")
        print("\n")
        
        # Track totals across all batches
        total_annotation_cost = 0
        total_dually_cost = 0
        num_workers = 5
        
        os.makedirs(config.output_dir, exist_ok=True)
        
        # ========== PROCESS EACH BATCH ==========
        for batch_idx in range(num_batches):
            batch_start = batch_idx * BATCH_SIZE
            batch_end = min(batch_start + BATCH_SIZE, total_listings)
            batch_df = df.iloc[batch_start:batch_end].copy().reset_index(drop=True)
            batch_num = batch_idx + 1
            batch_count = len(batch_df)
            
            job['current_batch'] = batch_num
            
            print(f"\n{'#'*80}")
            print(f"📦 BATCH {batch_num}/{num_batches} — Listings {batch_start+1} to {batch_end} ({batch_count} ads)")
            print(f"{'#'*80}\n")
            
            # Use a unique run_ts per batch so session reports don't collide
            batch_run_ts = f"{run_ts}_batch{batch_num}"
            
            # --- PHASE A: AI ANNOTATION for this batch ---
            print("="*80)
            print(f"🤖 BATCH {batch_num} — PHASE A: AI ANNOTATION")
            print("="*80)
            print(f"   Ads in this batch: {batch_count}")
            print(f"   Using {num_workers} parallel workers")
            print("="*80 + "\n")
            
            batch_result_df = run_parallel_ai(batch_df, batch_run_ts, job_id, num_workers)
            
            # --- PHASE B: DUALLY VERIFICATION for this batch ---
            batch_dually_cost = 0
            if not batch_result_df.empty and config.enable_dually_llm_verification:
                print(f"\n{'='*60}")
                print(f"🔍 BATCH {batch_num} — PHASE B: DUALLY LLM VERIFICATION")
                print(f"{'='*60}")
                m_verify = Manager()
                yoda_verify = Yoda(config.gemini_api_keys_info, config.rate_limit_rpm, m_verify)
                batch_result_df, batch_dually_cost = verify_dually_listings(batch_result_df, job_id, yoda_verify)
            elif not batch_result_df.empty:
                print(f"\n   🔍 BATCH {batch_num} — Dually verification: ❌ DISABLED (Skipping)")
            
            # ── Ad Annotation Limit: Post-increment for successful ads in this batch ──
            if config.enable_ad_annotation_limit and not batch_result_df.empty:
                if "Status" in batch_result_df.columns:
                    successful_ids = batch_result_df[batch_result_df["Status"] != "AI Error"]["Ad ID"].tolist()
                else:
                    successful_ids = batch_result_df["Ad ID"].tolist()
                if successful_ids:
                    ad_tracker.increment_annotation_counts(successful_ids)
                    print(f"   📊 Ad Tracker: Updated annotation counts for {len(successful_ids)} ads (batch {batch_num}) in Turso DB")
            
            # --- PHASE C: SAVE BATCH OUTPUT (atomic write) ---
            print(f"\n{'='*80}")
            print(f"💾 BATCH {batch_num} — PHASE C: SAVING BATCH OUTPUT")
            print(f"{'='*80}")
            
            batch_filename = f"batch_{batch_num}_{batch_start+1}-{batch_end}_annotated_{run_ts}.xlsx"
            if getattr(config, 'enable_ai_output', True):
                batch_path = os.path.join(config.output_dir, batch_filename)
            else:
                batch_path = _temp_path(batch_filename)
                _register_temp_file(batch_path)

            # Write to temp file first, then move atomically to prevent corruption
            temp_path = batch_path + '.writing.xlsx'
            batch_result_df.to_excel(temp_path, index=False)
            os.replace(temp_path, batch_path)  # Atomic on most filesystems
            
            # Calculate batch costs (with empty check)
            if len(batch_result_df) == 0:
                print(f"   ⚠️ Batch {batch_num} returned no results!")
            batch_total_cost = batch_result_df['Cost_Cents'].sum() if 'Cost_Cents' in batch_result_df.columns else 0
            batch_annotation_cost = batch_total_cost - batch_dually_cost
            
            total_annotation_cost += batch_annotation_cost
            total_dually_cost += batch_dually_cost
            
            # Store batch metadata (thread-safe)
            batch_info = {
                'batch_index': batch_idx,
                'batch_num': batch_num,
                'start': batch_start + 1,
                'end': batch_end,
                'row_count': int(len(batch_result_df)),
                'filename': batch_filename,
                'file_path': batch_path,
                'status': 'completed',
                'annotation_cost': float(batch_annotation_cost),
                'dually_cost': float(batch_dually_cost),
                'total_cost': float(batch_total_cost)
            }
            with jobs_lock:
                job['batches'].append(batch_info)
            
            # No need to keep batch DataFrame in memory — read from disk when combining
            del batch_result_df
            
            print(f"   ✅ Batch {batch_num} saved: {batch_filename}")
            print(f"   📊 Rows: {batch_info['row_count']} | Cost: {batch_total_cost:.2f}¢")
            print(f"   📥 Available for download immediately!")
            print(f"{'='*80}\n")
            
            # Merge session reports for this batch
            merge_all_session_reports(batch_run_ts)
        
        # ========== COMBINE ALL BATCHES INTO FINAL FILE ==========
        print(f"\n{'='*80}")
        print(f"💾 COMBINING ALL {num_batches} BATCHES INTO FINAL OUTPUT")
        print(f"{'='*80}")
        
        # Read from saved batch files instead of keeping in memory (memory-efficient)
        with jobs_lock:
            batch_files = [b['file_path'] for b in job.get('batches', [])]
        
        if batch_files:
            batch_dfs = [pd.read_excel(f, dtype={"Ad ID": str}) for f in batch_files]
            final_result_df = pd.concat(batch_dfs, ignore_index=True)
            del batch_dfs  # Free memory
            
            # ✅ FIX: Deduplicate when combining batches to prevent duplicate entries
            if not final_result_df.empty:
                final_result_df["Ad ID"] = final_result_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                duplicates_combined = final_result_df[final_result_df.duplicated(subset=["Ad ID"], keep=False)]
                if not duplicates_combined.empty:
                    print(f"   ⚠️ WARNING: Found {len(duplicates_combined)} duplicate rows when combining batches!")
                    print(f"   ⚠️ Duplicate Ad IDs: {duplicates_combined['Ad ID'].unique().tolist()}")
                    print(f"   ✅ Removing duplicates, keeping first occurrence...")
                    final_result_df = final_result_df.drop_duplicates(subset=["Ad ID"], keep='first').reset_index(drop=True)
                    print(f"   ✅ After deduplication: {len(final_result_df)} unique rows")
        else:
            final_result_df = pd.DataFrame()
        
        output_filename = f"output_db_annotated_{run_ts}.xlsx"
        if getattr(config, 'enable_ai_output', True):
            output_path = os.path.join(config.output_dir, output_filename)
        else:
            output_path = _temp_path(output_filename)
            _register_temp_file(output_path)
        final_result_df.to_excel(output_path, index=False)

        total_cost = total_annotation_cost + total_dually_cost

        job['status'] = JobStatus.COMPLETED
        job['output_file'] = output_path
        job['output_filename'] = output_filename
        job['total_cost'] = float(total_cost)
        job['annotation_cost'] = float(total_annotation_cost)
        job['dually_verification_cost'] = float(total_dually_cost)

        print(f"   ✅ Final combined file saved: {output_filename}")
        print(f"   📊 Total rows: {len(final_result_df)}")
        print(f"   📁 Path: {output_path}")
        print(f"{'='*80}\n")
        
        print(f"\n{'='*80}")
        print(f"🎉 AI ANNOTATION JOB {job_id} COMPLETE! ({num_batches} batches)")
        print(f"{'='*80}")
        print(f"   🤖 Total Ads Annotated: {len(final_result_df)}")
        print(f"   📦 Batches Completed: {num_batches}")
        print(f"   📄 Final Output File: {output_filename}")
        print(f"   💰 Annotation Cost: {total_annotation_cost:.2f}¢")
        print(f"   💰 Dually Verification Cost: {total_dually_cost:.2f}¢")
        print(f"   💰 TOTAL COST: {total_cost:.2f}¢")
        print(f"{'='*80}\n")
        
    except Exception as e:
        job['status'] = JobStatus.FAILED
        job['error'] = str(e)
        job['partial_completion'] = True
        job['completed_batches'] = len(job.get('batches', []))
        # Already-completed batches remain in job['batches'] and are still downloadable
        print(f"\n❌ AI ANNOTATION JOB {job_id} FAILED: {str(e)}")
        if job.get('batches'):
            print(f"   📦 {len(job['batches'])} batch(es) completed before failure and are still downloadable.")
        import traceback
        traceback.print_exc()


def run_db_fetch_pipeline_sync(
    job_id: str,
    client_id: str,
    client_secret: str, 
    grant_type: str,
    min_last_update: int,
    max_last_update: int,
    listing_start: int,
    listing_end: int
):
    """
    Complete DB Fetch + AI Annotation pipeline (PRODUCTION)
    
    This is the NEW feature that:
    1. Fetches data from database API (replaces scraping) - PRODUCTION ENV
    2. Runs AI annotation (same as before)
    3. Outputs annotated Excel (same format as before)
    """
    job = jobs[job_id]
    run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    try:
        print(f"\n{'='*80}")
        print(f"🗄️ DB FETCH + AI ANNOTATION JOB {job_id} STARTED (PRODUCTION)")
        print(f"{'='*80}")
        print(f"   Date Range: {min_last_update} → {max_last_update}")
        print(f"   Listing Range: {listing_start} → {listing_end}")
        print(f"{'='*80}\n")
        
        # ========== PHASE 1: FETCH DATA FROM DATABASE API (PRODUCTION) ==========
        print("\n" + "="*80)
        print("🗄️ PHASE 1: FETCHING DATA FROM DATABASE API (PRODUCTION)")
        print("="*80)
        
        # Step 1: Get access token
        print(f"\n   🔑 Using credentials from: {'Request' if client_id != config.db_api_client_id else 'config.ini'}")
        print(f"   🔑 Client ID: {client_id[:10]}...")
        print(f"   🔑 Grant Type: {grant_type}\n")
        
        token_data = get_access_token(client_id, client_secret, grant_type)
        access_token = token_data['access_token']
        
        # Step 2: Calculate how many trucks to fetch
        total_listings_needed = listing_end - listing_start
        print(f"\n   📊 Need to fetch {total_listings_needed} listings")
        print(f"   📦 Will use pagination with max 500 per request\n")
        
        # Step 3: Fetch trucks with pagination
        all_trucks = []
        current_offset = listing_start
        limit_per_request = 500
        
        print("="*80)
        print("📦 FETCHING TRUCKS DATA WITH PAGINATION")
        print("="*80)
        
        while len(all_trucks) < total_listings_needed:
            remaining = total_listings_needed - len(all_trucks)
            current_limit = min(limit_per_request, remaining)
            
            print(f"\n   🔄 Request {len(all_trucks) // 500 + 1}: Offset={current_offset}, Limit={current_limit}")
            
            batch_data = fetch_trucks_from_db(
                access_token,
                min_last_update,
                max_last_update,
                current_limit,
                current_offset
            )
            
            batch_trucks = batch_data.get('result', [])
            pagination = batch_data.get('pagination', {})
            total_available = pagination.get('total', 0)
            
            if not batch_trucks:
                print(f"   ⚠️ No more trucks available")
                break
            
            # Debug: Check for duplicates before extending
            existing_ids = set(truck.get('id') for truck in all_trucks)
            new_ids = [truck.get('id') for truck in batch_trucks]
            duplicate_count = sum(1 for tid in new_ids if tid in existing_ids)
            
            if duplicate_count > 0:
                print(f"   ⚠️ WARNING: {duplicate_count} duplicate Ad IDs detected in this batch!")
            
            all_trucks.extend(batch_trucks)
            current_offset += len(batch_trucks)
            
            print(f"   ✅ Batch complete. Total fetched so far: {len(all_trucks)}")
            print(f"   📊 API reports total available: {total_available}")
            print(f"   📊 Unique Ad IDs so far: {len(set(truck.get('id') for truck in all_trucks))}")
            
            if current_offset >= total_available:
                print(f"   ℹ️  Reached end of available data (total: {total_available})")
                break
        
        print("\n" + "="*80)
        print(f"✅ FETCHING COMPLETE - Retrieved {len(all_trucks)} trucks")
        print("="*80 + "\n")
        
        # Step 4: Process truck data into DataFrame
        print("="*80)
        print("🔄 PROCESSING TRUCK DATA INTO DATAFRAME")
        print("="*80)
        
        print(f"\n   📊 Input: {len(all_trucks)} trucks from API")
        print(f"   📊 Unique IDs in raw data: {len(set(truck.get('id') for truck in all_trucks))}")
        
        processed_trucks = []
        for i, truck in enumerate(all_trucks, 1):
            processed = process_truck_data(truck)
            processed_trucks.append(processed)
            
            if i % 100 == 0:
                print(f"   ✅ Processed {i}/{len(all_trucks)} trucks")
        
        print(f"   ✅ Processed all {len(processed_trucks)} trucks")
        print("="*80 + "\n")
        
        # Create DataFrame
        df = pd.DataFrame(processed_trucks)
        
        print(f"   📊 Before deduplication: {len(df)} rows")
        
        df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # Check for duplicates after processing
        duplicates = df[df.duplicated(subset=['Ad ID'], keep=False)]
        if not duplicates.empty:
            print(f"   ⚠️ Found {len(duplicates)} duplicate rows!")
            print(f"   ⚠️ Duplicate Ad IDs: {duplicates['Ad ID'].unique().tolist()}")
            # Remove duplicates, keeping first occurrence
            df = df.drop_duplicates(subset=['Ad ID'], keep='first')
            print(f"   ✅ After deduplication: {len(df)} rows")
        else:
            print(f"   ✅ No duplicates found")
        
        # Save intermediate DB fetch output (read back by start_db_annotation — must always be saved)
        db_fetch_filename = f"DB_Fetch_{run_ts}.xlsx"
        if getattr(config, 'enable_scrapper_output', True):
            db_fetch_dir = os.path.join(config.project_root, "Scrapper output")
            os.makedirs(db_fetch_dir, exist_ok=True)
            db_fetch_path = os.path.join(db_fetch_dir, db_fetch_filename)
        else:
            db_fetch_path = _temp_path(db_fetch_filename)
            _register_temp_file(db_fetch_path)
        df.to_excel(db_fetch_path, index=False)

        print(f"   ✅ Intermediate DB Fetch file saved: {db_fetch_filename}")
        print(f"   📁 Path: {db_fetch_path}\n")

        job['total_ads'] = int(len(df))
        job['status'] = JobStatus.PROCESSING
        
        # ── Ad Annotation Limit: Pre-filter (before AI annotation) ──
        if config.enable_ad_annotation_limit:
            over_limit = ad_tracker.filter_over_limit_ads(df["Ad ID"].tolist(), config.max_annotation_runs)
            if over_limit:
                print(f"   🚫 Ad Tracker: Skipping {len(over_limit)} ads (already annotated {config.max_annotation_runs}+ times)")
                print(f"   🚫 Skipped Ad IDs: {sorted(over_limit)[:10]}{'...' if len(over_limit) > 10 else ''}")
                df = df[~df["Ad ID"].isin(over_limit)].reset_index(drop=True)
                job['total_ads'] = int(len(df))
                print(f"   ✅ Ad Tracker: {len(df)} ads remaining after filter\n")
            else:
                print(f"   ✅ Ad Tracker: All {len(df)} ads are within annotation limit\n")
        
        # ========== PHASE 2: AI ANNOTATION ==========
        print("\n" + "="*80)
        print("🤖 PHASE 2: AI ANNOTATION (Same as existing feature)")
        print("="*80)
        print(f"   Total Ads to Annotate: {len(df)}")
        print("="*80 + "\n")
        
        # Run parallel AI annotation (same as existing feature)
        num_workers = 5
        print(f"\n🤖 Using {num_workers} parallel workers for AI annotation")
        result_df = run_parallel_ai(df, run_ts, job_id, num_workers)
        
        # ========== PHASE 3: DUALLY VERIFICATION ==========
        dually_verification_cost = 0
        if not result_df.empty:
            if config.enable_dually_llm_verification:
                print("\n" + "="*60)
                print("🔍 DUALLY LLM VERIFICATION: ✅ ENABLED")
                print("   Starting post-processing verification for Dually annotations...")
                print("="*60)
                m_verify = Manager()
                yoda_verify = Yoda(config.gemini_api_keys_info, config.rate_limit_rpm, m_verify)
                result_df, dually_verification_cost = verify_dually_listings(result_df, job_id, yoda_verify)
            else:
                print("\n" + "="*60)
                print("🔍 DUALLY LLM VERIFICATION: ❌ DISABLED (Skipping)")
                print("="*60)
        
        # ── Ad Annotation Limit: Post-increment for successful ads ──
        if config.enable_ad_annotation_limit and not result_df.empty:
            if "Status" in result_df.columns:
                successful_ids = result_df[result_df["Status"] != "AI Error"]["Ad ID"].tolist()
            else:
                successful_ids = result_df["Ad ID"].tolist()
            if successful_ids:
                ad_tracker.increment_annotation_counts(successful_ids)
                print(f"   📊 Ad Tracker: Updated annotation counts for {len(successful_ids)} successfully processed ads in Turso DB")
        
        # ========== PHASE 4: SAVE FINAL OUTPUT ==========
        print("\n" + "="*80)
        print("💾 PHASE 4: SAVING FINAL ANNOTATED OUTPUT")
        print("="*80)
        
        output_filename = f"output_db_annotated_{run_ts}.xlsx"
        if getattr(config, 'enable_ai_output', True):
            os.makedirs(config.output_dir, exist_ok=True)
            output_path = os.path.join(config.output_dir, output_filename)
        else:
            output_path = _temp_path(output_filename)
            _register_temp_file(output_path)
        result_df.to_excel(output_path, index=False)

        print(f"   ✅ Final annotated file saved: {output_filename}")
        print(f"   📁 Path: {output_path}")
        print("="*80 + "\n")

        job['status'] = JobStatus.COMPLETED
        job['output_file'] = output_path
        job['output_filename'] = output_filename
        
        # Calculate summary costs - Cost_Cents already includes dually verification costs (added in line 737)
        total_cost_in_df = result_df['Cost_Cents'].sum() if 'Cost_Cents' in result_df.columns else 0
        annotation_cost_only = total_cost_in_df - dually_verification_cost  # Back-calculate annotation-only cost
        
        job['total_cost'] = float(total_cost_in_df)  # Total cost (includes dually costs already)
        job['annotation_cost'] = float(annotation_cost_only)  # Annotation cost without dually
        job['dually_verification_cost'] = float(dually_verification_cost)  # Separate dually cost for reporting
        
        # Merge session reports
        merge_all_session_reports(run_ts)
        
        print(f"\n{'='*80}")
        print(f"🎉 DB FETCH + AI ANNOTATION JOB {job_id} COMPLETE!")
        print(f"{'='*80}")
        print(f"   📊 Total Trucks Fetched: {len(df)}")
        print(f"   🤖 Total Ads Annotated: {len(result_df)}")
        print(f"   📄 Output File: {output_filename}")
        print(f"   💰 Annotation Cost: {annotation_cost_only}¢")
        print(f"   💰 Dually Verification Cost: {dually_verification_cost}¢")
        print(f"   💰 TOTAL COST: {total_cost_in_df}¢")
        print(f"{'='*80}\n")
        
    except Exception as e:
        job['status'] = JobStatus.FAILED
        job['error'] = str(e)
        print(f"\n❌ DB FETCH + AI ANNOTATION JOB {job_id} FAILED: {str(e)}\n")
        import traceback
        traceback.print_exc()


@app.get("/")
async def root():
    return {"message": "AWACS AI Annotation API", "version": "1.0.0"}


@app.get("/health")
async def health():
    return {"status": "healthy", "api_keys_count": len(config.gemini_api_keys)}


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload an Excel file and create a job"""
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are supported")
    
    job_id = str(uuid.uuid4())[:8]

    # Save uploaded file
    upload_filename = f"upload_{job_id}_{file.filename}"
    if getattr(config, 'enable_uploads', True):
        upload_dir = os.path.join(config.project_root, "uploads")
        os.makedirs(upload_dir, exist_ok=True)
        file_path = os.path.join(upload_dir, upload_filename)
    else:
        file_path = _temp_path(upload_filename)
        _register_temp_file(file_path)

    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)

    # Validate file has Ad ID column
    try:
        df = pd.read_excel(file_path)
        if "Ad ID" not in df.columns:
            os.remove(file_path)
            raise HTTPException(status_code=400, detail="Excel file must have an 'Ad ID' column")
        ad_count = len(df)
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(status_code=400, detail=f"Invalid Excel file: {str(e)}")

    # Create job
    jobs[job_id] = {
        "id": job_id,
        "filename": file.filename,
        "file_path": file_path,
        "status": JobStatus.PENDING,
        "total_ads": ad_count,
        "created_at": datetime.now().isoformat()
    }
    
    return {
        "job_id": job_id,
        "filename": file.filename,
        "ad_count": ad_count,
        "status": JobStatus.PENDING,
        "message": f"File uploaded successfully. {ad_count} ads found."
    }


@app.post("/api/jobs/{job_id}/start")
async def start_job(job_id: str, background_tasks: BackgroundTasks):
    """Start processing a job"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    if job['status'] != JobStatus.PENDING:
        raise HTTPException(status_code=400, detail=f"Job is already {job['status']}")
    
    # Start the pipeline in background
    background_tasks.add_task(run_job_pipeline, job_id, job['file_path'])
    
    job['status'] = JobStatus.SCRAPING
    job['started_at'] = datetime.now().isoformat()
    
    return {
        "job_id": job_id,
        "status": job['status'],
        "message": "Processing started with parallel workers"
    }


@app.post("/api/reannotate")
async def reannotate_file(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
    """
    Re-annotate an already scraped file (skips scraping, goes directly to AI annotation).
    Expects a file with Ad ID, Breadcrumb columns, and Image_URLs already populated.
    """
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are supported")
    
    job_id = str(uuid.uuid4())[:8]

    # Save uploaded file
    upload_filename = f"upload_{job_id}_reannotate_{file.filename}"
    if getattr(config, 'enable_uploads', True):
        upload_dir = os.path.join(config.project_root, "uploads")
        os.makedirs(upload_dir, exist_ok=True)
        file_path = os.path.join(upload_dir, upload_filename)
    else:
        file_path = _temp_path(upload_filename)
        _register_temp_file(file_path)
    
    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)
    
    # Validate file structure
    try:
        df = pd.read_excel(file_path)
        if "Ad ID" not in df.columns:
            os.remove(file_path)
            raise HTTPException(status_code=400, detail="Excel file must have an 'Ad ID' column")
        
        # Check if file has required columns for reannotation
        required_cols = ["Breadcrumb_Top1", "Image_URLs"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            os.remove(file_path)
            raise HTTPException(
                status_code=400, 
                detail=f"File appears to not be scraped yet. Missing columns: {', '.join(missing_cols)}. Please upload and scrape first, or upload a previously scraped file."
            )
        
        ad_count = len(df)
    except HTTPException:
        raise
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(status_code=400, detail=f"Invalid Excel file: {str(e)}")
    
    # Create job
    jobs[job_id] = {
        "id": job_id,
        "filename": file.filename,
        "file_path": file_path,
        "status": JobStatus.PROCESSING,  # Skip scraping, go directly to processing
        "total_ads": ad_count,
        "created_at": datetime.now().isoformat(),
        "is_reannotation": True  # Flag to indicate this is a reannotation
    }
    
    # Start annotation directly (skip scraping)
    background_tasks.add_task(run_reannotation_pipeline, job_id, file_path)
    
    return {
        "job_id": job_id,
        "filename": file.filename,
        "ad_count": ad_count,
        "status": JobStatus.PROCESSING,
        "message": f"Re-annotation started for {ad_count} ads (skipping scraping)."
    }


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    """Get job status"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    progress = job_progress.get(job_id, {})
    
    response = {
        "job_id": job_id,
        "status": job['status'],
        "total_ads": job.get('total_ads', 0),
        "completed_ads": progress.get('completed', 0),
        "filename": job.get('filename', ''),
        "output_filename": job.get('output_filename'),
        "error": job.get('error'),
        "total_cost": job.get('total_cost', 0),
        "annotation_cost": job.get('annotation_cost', 0),
        "dually_verification_cost": job.get('dually_verification_cost', 0),
        "elapsed": progress.get('elapsed', 0),
        # Batch info
        "batch_count": len(job.get('batches', [])),
        "current_batch": job.get('current_batch', 0),
        "total_batches": job.get('total_batches', 0)
    }
    
    # Add dually verification progress if in that phase
    if job.get('status') == JobStatus.VERIFYING_DUALLY or job.get('dually_total'):
        response['dually_verification'] = {
            "total": job.get('dually_total', 0),
            "verified": job.get('dually_verified', 0),
            "removed": job.get('dually_removed', 0)
        }
    
    return response


@app.get("/api/jobs/{job_id}/progress")
async def get_job_progress(job_id: str):
    """Get real-time progress for a job"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    progress = job_progress.get(job_id, {})
    workers = progress.get("workers", {})
    
    # Calculate stats
    total = progress.get("total", 0)
    completed = progress.get("completed", 0)
    start_time = progress.get("start_time", time.time())
    elapsed = int(time.time() - start_time)
    
    # Calculate ETA
    eta = None
    if completed > 5:
        avg_time = elapsed / completed
        remaining = total - completed
        eta = int(remaining * avg_time)
    
    return {
        "job_id": job_id,
        "total": total,
        "completed": completed,
        "percentage": round((completed / total * 100), 1) if total > 0 else 0,
        "elapsed_seconds": elapsed,
        "eta_seconds": eta,
        "workers_alive": sum(1 for w in workers.values() if w.get("state") not in ["FINISHED", "ERROR"]),
        "worker_details": [
            {"id": k, "state": v.get("state", "UNKNOWN"), "progress": v.get("progress", 0)}
            for k, v in sorted(workers.items())
        ]
    }


@app.get("/listings/progress")
async def listings_progress_redirect():
    """Handle legacy /listings/progress calls - redirect to prevent 404 spam"""
    # Return an empty response for legacy endpoints to stop 404 spam
    return {"message": "Use /api/jobs/{job_id}/progress instead", "deprecated": True}


@app.get("/api/jobs/{job_id}/download")
async def download_result(job_id: str):
    """Download the annotated Excel file"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]
    if job['status'] != JobStatus.COMPLETED:
        raise HTTPException(status_code=400, detail="Job is not completed yet")

    output_path = job.get('output_file')
    if not output_path or not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="Output file not found")

    return FileResponse(
        path=output_path,
        filename=job.get('output_filename', 'output.xlsx'),
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@app.get("/api/jobs/{job_id}/batches")
async def get_job_batches(job_id: str):
    """Get list of completed annotation batches for a job"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    batches = job.get('batches', [])
    
    return {
        "job_id": job_id,
        "total_batches": job.get('total_batches', 0),
        "current_batch": job.get('current_batch', 0),
        "completed_batches": len(batches),
        "batches": [
            {
                "batch_index": b['batch_index'],
                "batch_num": b['batch_num'],
                "start": b['start'],
                "end": b['end'],
                "row_count": b['row_count'],
                "filename": b['filename'],
                "status": b['status'],
                "total_cost": b['total_cost']
            }
            for b in batches
        ]
    }


@app.get("/api/jobs/{job_id}/batches/{batch_index}/download")
async def download_batch(job_id: str, batch_index: int):
    """Download a specific batch Excel file"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]
    batches = job.get('batches', [])

    # Find the batch by index
    batch = None
    for b in batches:
        if b['batch_index'] == batch_index:
            batch = b
            break

    if not batch:
        raise HTTPException(status_code=404, detail=f"Batch {batch_index} not found")

    file_path = batch.get('file_path')
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Batch file not found")

    # Batch temp files are NOT deleted here — the combine step still needs to read them all.
    # They are deleted after the final combined file is written.

    return FileResponse(
        path=file_path,
        filename=batch['filename'],
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@app.get("/api/config")
async def get_config():
    """Get configuration info"""
    # Check if DB API credentials are configured
    db_api_configured = (
        config.db_api_client_id and 
        config.db_api_client_id != 'your_client_id_here' and
        config.db_api_client_secret and 
        config.db_api_client_secret != 'your_client_secret_here'
    )
    
    return {
        "api_keys_count": len(config.gemini_api_keys),
        "model": config.gemini_model,
        "max_images_per_ad": config.max_images,
        "rate_limit_rpm": config.rate_limit_rpm,
        "db_api_configured": db_api_configured,
        "db_api_client_id": config.db_api_client_id if db_api_configured else "",
        "db_api_grant_type": config.db_api_grant_type if db_api_configured else "client_credentials"
    }


# ==================== AUDIT FEATURE ====================

def get_normalized_set(row, cols, norm_map):
    """Helper to extract columns, normalize them, and return a set."""
    res_set = set()
    for c in cols:
        val = row.get(c)
        if pd.notna(val):
            val_str = str(val).strip()
            if val_str and val_str.lower() not in ['nan', 'none', '']:
                norm_val = normalize_text(val_str, norm_map)
                if norm_val:
                    res_set.add(str(norm_val).lower())
    return res_set


def run_audit_comparison(ai_df: pd.DataFrame, manual_df: pd.DataFrame, audit_id: str) -> dict:
    """
    Compare AI annotated data with manual feedback data.
    Returns audit results with summary statistics.
    """
    # Load normalization rules
    try:
        print(f"📋 [Rules.json] Loading for AUDIT comparison...")
        rules = load_rules(config.rules_json)
        norm_map = rules['normalize_map']
        print(f"📋 [Rules.json] AUDIT using normalize_map with {len(norm_map)} entries")
    except Exception as e:
        return {"error": f"Could not load Rules.json: {str(e)}"}
    
    # Standardize Ad ID column in AI data
    # Convert column names to strings to handle integer column names from Excel
    ai_df.columns = [str(c) for c in ai_df.columns]
    ai_cols_lower = {str(c).lower(): c for c in ai_df.columns}
    if 'ad id' in ai_cols_lower:
        ai_df.rename(columns={ai_cols_lower['ad id']: "Ad ID"}, inplace=True)
    elif 'ad_id' in ai_cols_lower:
        ai_df.rename(columns={ai_cols_lower['ad_id']: "Ad ID"}, inplace=True)
    
    if "Ad ID" not in ai_df.columns:
        return {"error": "AI annotated file must have an 'Ad ID' column"}
    
    ai_df["Ad ID"] = ai_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # Standardize Ad ID column in Manual data
    # Convert column names to strings to handle integer column names from Excel
    manual_df.columns = [str(c) for c in manual_df.columns]
    manual_cols_lower = {str(c).lower(): c for c in manual_df.columns}
    if 'ad id' in manual_cols_lower:
        manual_df.rename(columns={manual_cols_lower['ad id']: "Ad ID"}, inplace=True)
    elif 'ad_id' in manual_cols_lower:
        manual_df.rename(columns={manual_cols_lower['ad_id']: "Ad ID"}, inplace=True)
    
    if "Ad ID" not in manual_df.columns:
        return {"error": "Manual feedback file must have an 'Ad ID' column"}
    
    manual_df["Ad ID"] = manual_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # Deduplicate both dataframes before merging to prevent row multiplication
    ai_df = ai_df.drop_duplicates(subset=["Ad ID"], keep='last')
    manual_df = manual_df.drop_duplicates(subset=["Ad ID"], keep='last')

    # Merge data
    merged = pd.merge(ai_df, manual_df, on="Ad ID", how="inner", suffixes=('', '_manual'))
    
    if merged.empty:
        return {"error": "No matching Ad IDs found between AI Output and Manual Feedback"}
    
    # Define column names for comparison
    ai_cols = ["Annotated_Top1", "Annotated_Top2", "Annotated_Top3"]
    human_keys = ["Primary Category", "Add'l Category 1", "Add'l Category 2"]
    
    # Find matching columns (case-insensitive)
    manual_cols_list = list(manual_df.columns)
    found_human_cols = []
    for hk in human_keys:
        for mc in manual_cols_list:
            if str(hk).lower() == str(mc).lower():
                found_human_cols.append(mc)
                break
    
    # Perform comparison
    audit_results = []
    
    for idx, row in merged.iterrows():
        ai_set = get_normalized_set(row, ai_cols, norm_map)
        ai_status = str(row.get("Status", "")).lower()
        
        human_set = get_normalized_set(row, found_human_cols, norm_map)
        
        status = "Rejected"  # Default
        
        if ai_set == human_set:
            status = "Accepted"
        elif len(human_set) == 0:
            if "image not clear" in ai_set:
                status = "Accepted"
            elif "inactive ad" in ai_status or "inactive" in ai_status:
                status = "Accepted"
            elif "inactive ad" in ai_set:
                status = "Accepted"
        
        audit_results.append({
            "Ad ID": row["Ad ID"],
            "Feedback Status": status,
            "AI Categories": ", ".join(sorted(ai_set)),
            "Manual Categories": ", ".join(sorted(human_set))
        })
    
    # Assign Feedback Status directly — do NOT merge on Ad ID, as duplicate
    # Ad IDs cause a cartesian product (N rows × N rows = N² rows)
    merged["Feedback Status"] = [r["Feedback Status"] for r in audit_results]
    audit_df = pd.DataFrame(audit_results)
    final_output = merged
    
    # Generate Summary
    total = len(final_output)
    
    # Identify Inactive Rows
    is_inactive = final_output['Status'].astype(str).str.contains('inactive', case=False, na=False) if 'Status' in final_output.columns else pd.Series([False] * total)
    inactive_count = is_inactive.sum()
    
    active_total = total - inactive_count
    
    accepted_mask = (final_output["Feedback Status"] == "Accepted")
    rejected_mask = (final_output["Feedback Status"] == "Rejected")
    
    total_accepted = len(final_output[accepted_mask])
    total_rejected = len(final_output[rejected_mask])
    
    active_accepted = len(final_output[accepted_mask & (~is_inactive)])
    
    global_acc_pct = (total_accepted / total) * 100 if total > 0 else 0
    active_acc_pct = (active_accepted / active_total) * 100 if active_total > 0 else 0
    
    summary_data = [
        {"Metric": "Total Ads Audited", "Value": total},
        {"Metric": "Total Inactive Ads", "Value": inactive_count},
        {"Metric": "Total Active Ads", "Value": active_total},
        {"Metric": "---", "Value": "---"},
        {"Metric": "Global Accuracy (Including Inactive)", "Value": f"{global_acc_pct:.2f}%"},
        {"Metric": "Active Accuracy (Excluding Inactive)", "Value": f"{active_acc_pct:.2f}%"},
        {"Metric": "---", "Value": "---"},
        {"Metric": "Total Accepted", "Value": total_accepted},
        {"Metric": "Total Rejected", "Value": total_rejected}
    ]
    summary_df = pd.DataFrame(summary_data)
    
    # Hall of Shame - Most common mismatch patterns
    failures = audit_df[audit_df["Feedback Status"] == "Rejected"].copy()
    if not failures.empty:
        failures["Mismatch Pattern"] = "AI: [" + failures["AI Categories"] + "] vs Manual: [" + failures["Manual Categories"] + "]"
        hall_of_shame = failures["Mismatch Pattern"].value_counts().reset_index()
        hall_of_shame.columns = ["Mismatch Scenario", "Count"]
        # Add Ad IDs column showing which ads caused each mismatch
        ad_ids_per_pattern = failures.groupby("Mismatch Pattern")["Ad ID"].apply(lambda x: ", ".join(x)).reset_index()
        ad_ids_per_pattern.columns = ["Mismatch Scenario", "Ad IDs"]
        hall_of_shame = hall_of_shame.merge(ad_ids_per_pattern, on="Mismatch Scenario", how="left")
    else:
        hall_of_shame = pd.DataFrame([{"Message": "No Rejections! Perfect accuracy!"}])
    
    # Save Audit Report
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_filename = f"Audit_Report_{timestamp}.xlsx"
    if getattr(config, 'enable_audit_reports', True):
        audit_dir = os.path.join(config.project_root, "Audit Reports")
        os.makedirs(audit_dir, exist_ok=True)
        report_path = os.path.join(audit_dir, report_filename)
    else:
        report_path = _temp_path(report_filename)
        _register_temp_file(report_path)

    try:
        with pd.ExcelWriter(report_path) as writer:
            final_output.to_excel(writer, sheet_name="Detailed Audit", index=False)
            summary_df.to_excel(writer, sheet_name="Summary", index=False, startrow=0, startcol=0)
            hall_of_shame.to_excel(writer, sheet_name="Summary", index=False, startrow=len(summary_df)+3, startcol=0)
        
        print(f"\n✅ Audit Complete!")
        print(f"   Global Accuracy: {global_acc_pct:.2f}%")
        print(f"   Active Accuracy: {active_acc_pct:.2f}%")
        print(f"   Report Saved: {report_filename}")
        
    except Exception as e:
        return {"error": f"Error saving audit report: {str(e)}"}
    
    return {
        "audit_id": audit_id,
        "report_path": report_path,
        "report_filename": report_filename,
        "summary": {
            "total_audited": total,
            "total_inactive": int(inactive_count),
            "total_active": int(active_total),
            "total_accepted": total_accepted,
            "total_rejected": total_rejected,
            "global_accuracy": round(global_acc_pct, 2),
            "active_accuracy": round(active_acc_pct, 2)
        },
        "matching_ads": len(merged),
        "ai_file_ads": len(ai_df),
        "manual_file_ads": len(manual_df)
    }


@app.post("/api/audit")
async def run_audit(
    ai_file: UploadFile = File(..., description="AI annotated Excel file"),
    manual_file: UploadFile = File(..., description="Manual feedback Excel file from data team")
):
    """
    Upload two Excel files for audit comparison:
    - ai_file: The AI annotated output file
    - manual_file: The manual feedback file from data team
    
    Returns audit results with accuracy metrics and a downloadable report.
    """
    # Validate file types
    if not ai_file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="AI file must be an Excel file (.xlsx, .xls)")
    
    if not manual_file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Manual feedback file must be an Excel file (.xlsx, .xls)")
    
    audit_id = str(uuid.uuid4())[:8]
    
    # Save uploaded files temporarily
    upload_dir = os.path.join(config.project_root, "uploads", "audit")
    os.makedirs(upload_dir, exist_ok=True)
    
    ai_file_path = os.path.join(upload_dir, f"{audit_id}_ai_{ai_file.filename}")
    manual_file_path = os.path.join(upload_dir, f"{audit_id}_manual_{manual_file.filename}")
    
    try:
        # Save AI file
        with open(ai_file_path, "wb") as f:
            content = await ai_file.read()
            f.write(content)
        
        # Save Manual file
        with open(manual_file_path, "wb") as f:
            content = await manual_file.read()
            f.write(content)
        
        # Read Excel files
        ai_df = pd.read_excel(ai_file_path, dtype=str)
        manual_df = pd.read_excel(manual_file_path, dtype=str)
        
        # Run comparison
        result = run_audit_comparison(ai_df, manual_df, audit_id)
        
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        
        # Store audit job for download
        audit_jobs[audit_id] = {
            "id": audit_id,
            "ai_filename": ai_file.filename,
            "manual_filename": manual_file.filename,
            "report_path": result["report_path"],
            "report_filename": result["report_filename"],
            "created_at": datetime.now().isoformat()
        }
        
        return {
            "audit_id": audit_id,
            "message": "Audit completed successfully",
            "ai_file": ai_file.filename,
            "manual_file": manual_file.filename,
            "summary": result["summary"],
            "stats": {
                "ai_file_total_ads": result["ai_file_ads"],
                "manual_file_total_ads": result["manual_file_ads"],
                "matching_ads_compared": result["matching_ads"]
            },
            "download_url": f"/api/audit/{audit_id}/download"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing audit: {str(e)}")
    finally:
        # Clean up temporary files
        for path in [ai_file_path, manual_file_path]:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except:
                    pass


@app.get("/api/audit/{audit_id}/download")
async def download_audit_report(audit_id: str, background_tasks: BackgroundTasks):
    """Download the audit report Excel file"""
    if audit_id not in audit_jobs:
        raise HTTPException(status_code=404, detail="Audit report not found")

    audit = audit_jobs[audit_id]
    report_path = audit.get('report_path')

    if not report_path or not os.path.exists(report_path):
        raise HTTPException(status_code=404, detail="Audit report file not found")

    # If audit reports are stored in temp dir, clean up after serving
    if not getattr(config, 'enable_audit_reports', True):
        background_tasks.add_task(os.remove, report_path)

    return FileResponse(
        path=report_path,
        filename=audit.get('report_filename', 'audit_report.xlsx'),
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@app.get("/api/audit/{audit_id}")
async def get_audit_status(audit_id: str):
    """Get audit job details"""
    if audit_id not in audit_jobs:
        raise HTTPException(status_code=404, detail="Audit not found")
    
    return audit_jobs[audit_id]


# ==================== DB FETCH FEATURE ====================

import requests
from pydantic import BaseModel
from typing import Optional, List

class DBFetchRequest(BaseModel):
    client_id: Optional[str] = None  # Optional - will use config if not provided
    client_secret: Optional[str] = None  # Optional - will use config if not provided
    grant_type: Optional[str] = "client_credentials"  # Optional - will use config if not provided
    min_last_update: int  # Unix timestamp
    max_last_update: int  # Unix timestamp
    listing_start: int = 0  # Starting offset (e.g., 0 for first 1000)
    listing_end: int = 1000  # Ending offset (e.g., 1000 for first 1000)
    category_filters: Optional[List[str]] = None  # Optional - filter by category names (e.g., ["Pickup Truck", "Cab-Chassis"])

def get_access_token(client_id: str, client_secret: str, grant_type: str) -> dict:
    """
    Get access token from the authentication API (PRODUCTION)
    """
    print("\n" + "="*80)
    print("🔑 FETCHING ACCESS TOKEN FROM DB API (PRODUCTION)")
    print("="*80)
    
    # PRODUCTION TOKEN URL
    token_url = "https://nebulous-prod.traderonline.com/vLatest/token"
    
    # Strip whitespace from all parameters to avoid auth issues
    client_id = client_id.strip()
    client_secret = client_secret.strip()
    grant_type = grant_type.strip()
    
    form_data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': grant_type
    }
    
    # Set explicit headers to match Postman
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    try:
        print(f"   📤 POST {token_url}")
        print(f"   📝 Form Data: client_id={client_id}, grant_type={grant_type}")
        print(f"   📝 Client Secret: {len(client_secret)} chars, starts with '{client_secret[:10]}...'")
        print(f"   📋 Headers: {headers}")
        
        response = requests.post(token_url, data=form_data, headers=headers)
        
        # Print detailed error information if request fails
        if not response.ok:
            print(f"   ❌ HTTP {response.status_code}: {response.reason}")
            print(f"   📋 Response Headers: {dict(response.headers)}")
            try:
                error_detail = response.json()
                print(f"   📋 Error JSON: {error_detail}")
            except:
                print(f"   📋 Response Text: {response.text}")
            
        response.raise_for_status()
        
        token_data = response.json()
        print(f"   ✅ Access token received: {token_data['access_token'][:20]}...")
        print(f"   ⏱️  Expires in: {token_data['expires_in']} seconds")
        print("="*80 + "\n")
        
        return token_data
    except requests.exceptions.HTTPError as e:
        print(f"   ❌ HTTP Error: {str(e)}")
        print("="*80 + "\n")
        raise
    except Exception as e:
        print(f"   ❌ Error fetching access token: {str(e)}")
        print("="*80 + "\n")
        raise


def fetch_trucks_from_db(access_token: str, min_last_update: int, max_last_update: int, limit: int = 500, offset: int = 0) -> dict:
    """
    Fetch truck data from the DB API with pagination (PRODUCTION)
    """
    # PRODUCTION TRUCKS URL
    trucks_url = "https://nebulous-prod.traderonline.com/v1/trucks"
    
    params = {
        'bypassCache': 'true',
        'minLastUpdate': min_last_update,
        'maxLastUpdate': max_last_update,
        'limit': limit,
        'offset': offset
    }
    
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    
    try:
        print(f"   📤 GET {trucks_url}")
        print(f"   📝 Params: minLastUpdate={min_last_update}, maxLastUpdate={max_last_update}, limit={limit}, offset={offset}")
        
        response = requests.get(trucks_url, params=params, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        total = data.get('pagination', {}).get('total', 0)
        returned = len(data.get('result', []))
        
        print(f"   ✅ Fetched {returned} trucks (Total available: {total})")
        
        return data
    except Exception as e:
        print(f"   ❌ Error fetching trucks: {str(e)}")
        raise


def process_truck_data(truck: dict, debug: bool = False) -> dict:
    """
    Process individual truck data to extract photos and categories
    """
    ad_id = truck.get('id', '')
    
    processed = {
        'Ad ID': ad_id,
        'Breadcrumb_Top1': '',
        'Breadcrumb_Top2': '',
        'Breadcrumb_Top3': '',
        'Image_URLs': ''
    }
    
    # ✅ Check if truck has status field indicating inactive
    status = truck.get('status', '').lower()
    if status and 'inactive' in status:
        processed['Breadcrumb_Top1'] = 'Inactive ad'
        return processed
    
    # Extract categories for breadcrumbs
    categories = truck.get('categories', [])
    if categories:
        for i, category in enumerate(categories[:3]):  # Get up to 3 categories
            category_name = category.get('name', '')
            if i == 0:
                processed['Breadcrumb_Top1'] = category_name
            elif i == 1:
                processed['Breadcrumb_Top2'] = category_name
            elif i == 2:
                processed['Breadcrumb_Top3'] = category_name
    
    # Extract photos and construct CDN URLs
    photos = truck.get('photos', [])
    
    # Debug logging for first truck
    if debug:
        print(f"\n   🔍 DEBUG - Processing truck {ad_id}:")
        print(f"      Photos array exists: {photos is not None}")
        print(f"      Photos count: {len(photos) if photos else 0}")
        if photos:
            print(f"      First photo data: {photos[0]}")
    
    if photos:
        cdn_urls = []
        for photo in photos:
            # Extract URL directly from the photo object (API provides full CDN URL)
            photo_url = photo.get('url', '')
            if photo_url:
                cdn_urls.append(photo_url)
            elif debug:
                print(f"      ⚠️ Photo missing url field: {photo}")
        
        processed['Image_URLs'] = ','.join(cdn_urls)
        
        if debug:
            print(f"      Final Image_URLs: {processed['Image_URLs'][:100]}..." if len(processed['Image_URLs']) > 100 else f"      Final Image_URLs: {processed['Image_URLs']}")
    elif debug:
        print(f"      ⚠️ No photos array in truck data")
    
    return processed


def fetch_single_truck_by_id(access_token: str, ad_id: str) -> dict:
    """
    Fetch a single truck by Ad ID from the DB API (PRODUCTION)
    
    Args:
        access_token: Bearer token for authentication
        ad_id: The truck Ad ID to fetch
        
    Returns:
        dict: Truck data from API
    """
    # PRODUCTION TRUCKS URL (individual truck endpoint)
    truck_url = f"https://nebulous-prod.traderonline.com/vLatest/trucks/{ad_id}"
    
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    
    try:
        response = requests.get(truck_url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        # The API returns data in format: {"url": "...", "result": {...}}
        # We need the "result" object which contains the truck data
        return data.get('result', {})
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"   ⚠️ Truck {ad_id} not found (404)")
            return None
        else:
            print(f"   ❌ Error fetching truck {ad_id}: HTTP {e.response.status_code}")
            raise
    except Exception as e:
        print(f"   ❌ Error fetching truck {ad_id}: {str(e)}")
        raise


def _fetch_single_truck_worker(ad_id: str, access_token: str, index: int, total: int):
    """
    Worker function to fetch a single truck by Ad ID (for multithreading)
    
    Args:
        ad_id: The truck Ad ID to fetch
        access_token: Bearer token for authentication
        index: Current index (for progress display)
        total: Total number of trucks to fetch
        
    Returns:
        tuple: (status, ad_id, truck_data_or_error)
            status: 'success', 'not_found', or 'error'
            ad_id: The Ad ID that was fetched
            truck_data_or_error: Truck data dict if success, None if not found, error message if error
    """
    try:
        print(f"   🔄 [{index}/{total}] Fetching truck {ad_id}...", end=" ", flush=True)
        truck_data = fetch_single_truck_by_id(access_token, ad_id)
        
        if truck_data:
            print(f"✅")
            return ('success', ad_id, truck_data)
        else:
            print(f"❌ Not Found")
            return ('not_found', ad_id, None)
            
    except Exception as e:
        error_msg = str(e)[:50]
        print(f"❌ Error: {error_msg}")
        return ('error', ad_id, str(e))


def run_db_fetch_by_ids_sync(job_id: str, file_path: str, client_id: str, client_secret: str, grant_type: str):
    """
    Fetch trucks by Ad IDs from uploaded Excel file (PRODUCTION)
    
    This feature:
    1. Reads Ad IDs from uploaded Excel file
    2. Fetches each truck individually from the database API (no scraping)
    3. Processes the data to extract photos and categories
    4. Saves to Excel ready for annotation
    """
    job = jobs[job_id]
    run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    try:
        print(f"\n{'='*80}")
        print(f"🗄️ DB FETCH BY AD IDs JOB {job_id} STARTED (PRODUCTION)")
        print(f"{'='*80}")
        print(f"   Source File: {os.path.basename(file_path)}")
        print(f"{'='*80}\n")
        
        # ========== STEP 1: Load Ad IDs from Excel ==========
        print("="*80)
        print("📄 STEP 1: LOADING AD IDs FROM EXCEL")
        print("="*80)
        
        df = pd.read_excel(file_path, dtype={"Ad ID": str})
        
        # Standardize Ad ID column
        if "Ad ID" not in df.columns:
            raise ValueError("Excel file must have an 'Ad ID' column")
        
        df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # ✅ FIX: Deduplicate Ad IDs BEFORE processing to prevent exponential growth
        duplicates_before = df[df.duplicated(subset=["Ad ID"], keep=False)]
        if not duplicates_before.empty:
            print(f"   ⚠️ WARNING: Found {len(duplicates_before)} duplicate rows in input Excel!")
            print(f"   ⚠️ Duplicate Ad IDs: {duplicates_before['Ad ID'].unique().tolist()}")
            print(f"   ✅ Removing duplicates, keeping first occurrence...")
            df = df.drop_duplicates(subset=["Ad ID"], keep='first').reset_index(drop=True)
            print(f"   ✅ After deduplication: {len(df)} unique Ad IDs")
        
        ad_ids = df["Ad ID"].tolist()
        
        print(f"   ✅ Loaded {len(ad_ids)} unique Ad IDs from Excel")
        print(f"   📊 First 5 Ad IDs: {ad_ids[:5]}")
        print("="*80 + "\n")
        
        # ── Ad Annotation Limit: Pre-filter (BEFORE DB API calls — saves API bandwidth) ──
        if config.enable_ad_annotation_limit:
            over_limit = ad_tracker.filter_over_limit_ads(ad_ids, config.max_annotation_runs)
            if over_limit:
                print(f"   🚫 Ad Tracker: Skipping {len(over_limit)} ads (already annotated {config.max_annotation_runs}+ times)")
                print(f"   🚫 Skipped Ad IDs: {sorted(over_limit)[:10]}{'...' if len(over_limit) > 10 else ''}")
                df = df[~df["Ad ID"].isin(over_limit)].reset_index(drop=True)
                ad_ids = df["Ad ID"].tolist()
                print(f"   ✅ Ad Tracker: {len(ad_ids)} ads remaining — only these will be fetched from DB API\n")
            else:
                print(f"   ✅ Ad Tracker: All {len(ad_ids)} ads are within annotation limit\n")
        
        job['total_ads'] = int(len(ad_ids))
        job['status'] = JobStatus.PROCESSING
        
        # ========== STEP 2: Get Access Token ==========
        print("="*80)
        print("🔑 STEP 2: AUTHENTICATING WITH DB API")
        print("="*80)
        
        token_data = get_access_token(client_id, client_secret, grant_type)
        access_token = token_data['access_token']
        print("="*80 + "\n")
        
        # ========== STEP 3: Fetch Trucks by Ad ID (MULTITHREADED) ==========
        print("="*80)
        print(f"📦 STEP 3: FETCHING {len(ad_ids)} TRUCKS FROM DB API (MULTITHREADED)")
        print("="*80)
        print(f"   Using endpoint: https://nebulous-prod.traderonline.com/vLatest/trucks/{{ad_id}}")
        print(f"   🚀 Using 5 concurrent workers for SUPER FAST fetching!")
        print(f"   This is MUCH faster than scraping! (~1-2 seconds per truck)\n")
        
        fetched_trucks = []
        not_found_ids = []
        error_ids = []
        
        # Use ThreadPoolExecutor with 5 workers for concurrent fetching
        max_workers = 5
        print(f"⚡ Starting {max_workers} concurrent fetch workers...\n")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all fetch tasks
            future_to_ad_id = {
                executor.submit(_fetch_single_truck_worker, ad_id, access_token, i, len(ad_ids)): ad_id
                for i, ad_id in enumerate(ad_ids, 1)
            }
            
            # Process results as they complete
            for future in as_completed(future_to_ad_id):
                status, ad_id, result = future.result()
                
                if status == 'success':
                    fetched_trucks.append(result)
                elif status == 'not_found':
                    not_found_ids.append(ad_id)
                elif status == 'error':
                    error_ids.append(ad_id)
        
        print("\n" + "="*80)
        print(f"✅ FETCHING COMPLETE")
        print("="*80)
        print(f"   Successfully fetched: {len(fetched_trucks)}/{len(ad_ids)} trucks")
        if not_found_ids:
            print(f"   ⚠️ Not found: {len(not_found_ids)} trucks - {not_found_ids[:10]}")
        if error_ids:
            print(f"   ❌ Errors: {len(error_ids)} trucks - {error_ids[:10]}")
        print("="*80 + "\n")
        
        if len(fetched_trucks) == 0:
            raise ValueError("No trucks were successfully fetched from the database")
        
        # ========== STEP 4: Process Truck Data ==========
        print("="*80)
        print("🔄 STEP 4: PROCESSING TRUCK DATA")
        print("="*80)
        
        processed_trucks = []
        for i, truck in enumerate(fetched_trucks, 1):
            debug = (i == 1)  # Debug first truck only
            processed = process_truck_data(truck, debug=debug)
            processed_trucks.append(processed)
            
            if i % 50 == 0:
                print(f"   ✅ Processed {i}/{len(fetched_trucks)} trucks")
        
        # ✅ FIX: Add not_found and error trucks with "Inactive ad" status
        for ad_id in not_found_ids:
            processed_trucks.append({
                'Ad ID': ad_id,
                'Breadcrumb_Top1': 'Inactive ad',
                'Breadcrumb_Top2': '',
                'Breadcrumb_Top3': '',
                'Image_URLs': ''
            })
        
        for ad_id in error_ids:
            processed_trucks.append({
                'Ad ID': ad_id,
                'Breadcrumb_Top1': 'Inactive ad',
                'Breadcrumb_Top2': '',
                'Breadcrumb_Top3': '',
                'Image_URLs': ''
            })
        
        print(f"   ✅ Processed {len(fetched_trucks)} successful trucks")
        if not_found_ids:
            print(f"   ⚠️ Added {len(not_found_ids)} inactive (not found) trucks")
        if error_ids:
            print(f"   ⚠️ Added {len(error_ids)} inactive (error) trucks")
        print(f"   ✅ Total processed: {len(processed_trucks)} trucks")
        print("="*80 + "\n")
        
        # ========== STEP 5: Save to Excel ==========
        print("="*80)
        print("💾 STEP 5: SAVING TO EXCEL")
        print("="*80)
        
        result_df = pd.DataFrame(processed_trucks)
        result_df["Ad ID"] = result_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # ✅ FIX: Deduplicate result DataFrame to prevent duplicate entries
        duplicates_after = result_df[result_df.duplicated(subset=["Ad ID"], keep=False)]
        if not duplicates_after.empty:
            print(f"   ⚠️ WARNING: Found {len(duplicates_after)} duplicate rows in processed data!")
            print(f"   ⚠️ Duplicate Ad IDs: {duplicates_after['Ad ID'].unique().tolist()}")
            print(f"   ✅ Removing duplicates, keeping first occurrence...")
            result_df = result_df.drop_duplicates(subset=["Ad ID"], keep='first').reset_index(drop=True)
            print(f"   ✅ After deduplication: {len(result_df)} unique rows")
        
        # ✅ FIX: Preserve input order by merging with original df
        # Create a copy of the input df with just Ad ID to preserve the original order
        ordered_df = df[["Ad ID"]].copy()
        ordered_df["Ad ID"] = ordered_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # Merge to preserve the input order (left join keeps the order of ordered_df)
        result_df = pd.merge(ordered_df, result_df, on="Ad ID", how="left")
        
        # Save intermediate DB fetch output (read back for annotation + download — must always be saved)
        db_fetch_filename = f"DB_Fetch_ByIDs_{run_ts}.xlsx"
        if getattr(config, 'enable_scrapper_output', True):
            db_fetch_dir = os.path.join(config.project_root, "Scrapper output")
            os.makedirs(db_fetch_dir, exist_ok=True)
            db_fetch_path = os.path.join(db_fetch_dir, db_fetch_filename)
        else:
            db_fetch_path = _temp_path(db_fetch_filename)
            _register_temp_file(db_fetch_path)
        result_df.to_excel(db_fetch_path, index=False)
        
        print(f"   ✅ Excel file saved: {db_fetch_filename}")
        print(f"   📁 Path: {db_fetch_path}")
        print("="*80 + "\n")
        
        # Update job status
        job['status'] = 'fetched'
        job['file_path'] = db_fetch_path
        job['output_file'] = db_fetch_path
        job['output_filename'] = db_fetch_filename
        job['total_ads'] = int(len(result_df))
        job['preview_data'] = processed_trucks[:10]
        job['is_db_fetch_by_ids'] = True
        
        print("="*80)
        print("🎉 DB FETCH BY AD IDs COMPLETE!")
        print("="*80)
        print(f"   📊 Successfully fetched: {len(fetched_trucks)} trucks")
        print(f"   📄 Excel file: {db_fetch_filename}")
        print(f"   ✅ Ready for annotation!")
        print(f"   📊 Preview available: First {len(processed_trucks[:10])} trucks")
        print("="*80 + "\n")
        
    except Exception as e:
        job['status'] = JobStatus.FAILED
        job['error'] = str(e)
        print(f"\n❌ DB FETCH BY AD IDs FAILED: {str(e)}\n")
        import traceback
        traceback.print_exc()


@app.post("/api/db-fetch")
async def fetch_from_db(request: DBFetchRequest):
    """
    Fetch truck data from the database API (WITHOUT automatic annotation)
    
    This endpoint:
    1. Gets an access token from the authentication API
    2. Fetches truck data from the database API with pagination
    3. Processes the data to extract photos and categories
    4. Returns the data for preview
    5. User can then start annotation separately
    
    Credentials can be provided in the request or will be loaded from config.ini
    """
    print("\n" + "="*80)
    print("🗄️ DB FETCH - FETCHING DATA FROM DATABASE API (PRODUCTION)")
    print("="*80)
    
    # If min and max timestamps are the same, expand to full day (24 hours)
    min_timestamp = request.min_last_update
    max_timestamp = request.max_last_update
    
    if min_timestamp == max_timestamp:
        # Expand to full day: from 00:00:00 to 23:59:59
        max_timestamp = min_timestamp + 86399  # 86399 seconds = 23 hours, 59 minutes, 59 seconds
        print(f"   ℹ️  Same timestamp detected - expanding to full day")
        print(f"   📅 Original: {request.min_last_update}")
        print(f"   📅 Expanded: {min_timestamp} → {max_timestamp} (full 24 hours)")
    else:
        print(f"   📅 Date Range: {min_timestamp} → {max_timestamp}")
    
    print(f"   📊 Listing Range: {request.listing_start} → {request.listing_end}")
    print("="*80 + "\n")
    
    try:
        # Use provided credentials or fallback to config.ini
        # IMPORTANT: Strip whitespace to avoid authentication issues
        client_id = (request.client_id or config.db_api_client_id).strip()
        client_secret = (request.client_secret or config.db_api_client_secret).strip()
        grant_type = (request.grant_type or config.db_api_grant_type).strip()
        
        # Validate credentials
        if not client_id or client_id == 'your_client_id_here':
            raise HTTPException(
                status_code=400, 
                detail="DB API Client ID not configured. Please add credentials to config.ini or provide them in the request."
            )
        
        if not client_secret or client_secret == 'your_client_secret_here':
            raise HTTPException(
                status_code=400, 
                detail="DB API Client Secret not configured. Please add credentials to config.ini or provide them in the request."
            )
        
        print(f"   🔑 Using credentials from: {'Request' if request.client_id else 'config.ini'}")
        print(f"   🔑 Client ID: {client_id[:10]}...")
        print(f"   🔑 Client Secret length: {len(client_secret)} chars")
        print(f"   🔑 Client Secret first 10 chars: {client_secret[:10]}...")
        print(f"   🔑 Grant Type: {grant_type}\n")
        
        # Step 1: Get access token
        token_data = get_access_token(
            client_id,
            client_secret,
            grant_type
        )
        access_token = token_data['access_token']
        
        # Step 2: Calculate how many trucks to fetch
        total_listings_needed = request.listing_end - request.listing_start
        
        print(f"\n   📊 Need to fetch {total_listings_needed} listings")
        print(f"   📦 Will use pagination with max 500 per request\n")
        
        # Step 3: Fetch trucks with pagination
        all_trucks = []
        current_offset = request.listing_start
        limit_per_request = 500  # Max allowed by API
        
        print("="*80)
        print("📦 FETCHING TRUCKS DATA WITH PAGINATION")
        print("="*80)
        
        # Track total available for pagination guidance
        total_available_in_db = 0
        
        while len(all_trucks) < total_listings_needed:
            # Calculate how many more we need
            remaining = total_listings_needed - len(all_trucks)
            current_limit = min(limit_per_request, remaining)
            
            print(f"\n   🔄 Request {len(all_trucks) // 500 + 1}: Offset={current_offset}, Limit={current_limit}")
            
            # Fetch batch (use expanded timestamps)
            batch_data = fetch_trucks_from_db(
                access_token,
                min_timestamp,
                max_timestamp,
                current_limit,
                current_offset
            )
            
            batch_trucks = batch_data.get('result', [])
            
            if not batch_trucks:
                print(f"   ⚠️ No more trucks available")
                break
            
            all_trucks.extend(batch_trucks)
            current_offset += len(batch_trucks)
            
            print(f"   ✅ Batch complete. Total fetched so far: {len(all_trucks)}")
            
            # Check if we've reached the total available
            pagination = batch_data.get('pagination', {})
            total_available_in_db = pagination.get('total', 0)
            
            if current_offset >= total_available_in_db:
                print(f"   ℹ️  Reached end of available data (total: {total_available_in_db})")
                break
        
        print("\n" + "="*80)
        print(f"✅ FETCHING COMPLETE - Retrieved {len(all_trucks)} trucks")
        print(f"   Total available in DB for this date range: {total_available_in_db}")
        print("="*80 + "\n")
        
        # Step 4: Process truck data
        print("="*80)
        print("🔄 PROCESSING TRUCK DATA")
        print("="*80)
        
        processed_trucks = []
        for i, truck in enumerate(all_trucks, 1):
            # Enable debug for first truck to see what we're getting
            debug = (i == 1)
            processed = process_truck_data(truck, debug=debug)
            processed_trucks.append(processed)
            
            if i % 100 == 0:
                print(f"   ✅ Processed {i}/{len(all_trucks)} trucks")
        
        print(f"   ✅ Processed all {len(processed_trucks)} trucks")
        print("="*80 + "\n")
        
        # Step 5: Apply category filters if provided
        if request.category_filters and len(request.category_filters) > 0:
            print("="*80)
            print("🔍 APPLYING CATEGORY FILTERS")
            print("="*80)
            print(f"   Filters: {request.category_filters}")
            
            # Convert filters to lowercase for case-insensitive matching
            filters_lower = [f.lower().strip() for f in request.category_filters]
            
            filtered_trucks = []
            for truck in processed_trucks:
                # Get all breadcrumb categories
                categories = [
                    truck.get('Breadcrumb_Top1', '').lower(),
                    truck.get('Breadcrumb_Top2', '').lower(),
                    truck.get('Breadcrumb_Top3', '').lower()
                ]
                
                # Check if any category matches any filter (partial match)
                matches = False
                for cat in categories:
                    if cat:
                        for f in filters_lower:
                            if f in cat or cat in f:
                                matches = True
                                break
                    if matches:
                        break
                
                if matches:
                    filtered_trucks.append(truck)
            
            print(f"   Before filtering: {len(processed_trucks)} trucks")
            print(f"   After filtering: {len(filtered_trucks)} trucks")
            print(f"   Filtered out: {len(processed_trucks) - len(filtered_trucks)} trucks")
            print("="*80 + "\n")
            
            processed_trucks = filtered_trucks
        
        # Check if any data was fetched
        if len(processed_trucks) == 0:
            print("="*80)
            print("⚠️ NO DATA FOUND")
            print("="*80)
            print("   No trucks found for the specified date range and listing range.")
            print("   Please try:")
            print("   1. Different date range")
            print("   2. Different listing range")
            print("   3. Check if data exists in the database for this period")
            print("="*80 + "\n")
            raise HTTPException(
                status_code=404,
                detail=f"No trucks found for date range {min_timestamp} to {max_timestamp}. Please try a different date range or check if data exists for this period."
            )
        
        # Step 5: Create DataFrame and save intermediate file
        print("="*80)
        print("📄 CREATING INTERMEDIATE EXCEL FILE")
        print("="*80)
        
        df = pd.DataFrame(processed_trucks)
        df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # Save to file (read back by start_db_annotation + download — must always be saved)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        fetch_id = str(uuid.uuid4())[:8]
        output_filename = f"DB_Fetch_{timestamp}.xlsx"
        if getattr(config, 'enable_scrapper_output', True):
            output_dir = os.path.join(config.project_root, "Scrapper output")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, output_filename)
        else:
            output_path = _temp_path(output_filename)
            _register_temp_file(output_path)

        df.to_excel(output_path, index=False)
        
        print(f"   ✅ Excel file created: {output_filename}")
        print(f"   📁 Path: {output_path}")
        print("="*80 + "\n")
        
        jobs[fetch_id] = {
            "id": fetch_id,
            "filename": output_filename,
            "file_path": output_path,
            "status": "fetched",  # New status: data fetched, ready for annotation
            "total_ads": int(len(df)),
            "created_at": datetime.now().isoformat(),
            "is_db_fetch": True,
            "preview_data": processed_trucks[:10]  # First 10 rows for preview
        }
        
        # Calculate pagination guidance
        first_ad_id = processed_trucks[0]['Ad ID'] if processed_trucks else None
        last_ad_id = processed_trucks[-1]['Ad ID'] if processed_trucks else None
        next_start = request.listing_end
        has_more_data = next_start < total_available_in_db
        remaining_listings = max(0, total_available_in_db - next_start)
        
        # Check if filters were applied
        filters_applied = request.category_filters and len(request.category_filters) > 0
        fetched_before_filter = len(all_trucks)  # Count before filtering
        matched_after_filter = len(processed_trucks)  # Count after filtering
        
        print("="*80)
        print("🎉 DB FETCH COMPLETE - READY FOR PREVIEW!")
        print("="*80)
        print(f"   Total Trucks: {len(processed_trucks)}")
        print(f"   File: {output_filename}")
        print(f"   Fetch ID: {fetch_id}")
        print(f"   Status: Ready for annotation")
        print(f"\n   📊 PAGINATION INFO:")
        print(f"   First Ad ID: {first_ad_id}")
        print(f"   Last Ad ID: {last_ad_id}")
        print(f"   Total Available in DB: {total_available_in_db}")
        if filters_applied:
            print(f"   🔍 Category Filters Applied: {request.category_filters}")
            print(f"   Fetched (before filter): {fetched_before_filter}")
            print(f"   Matched (after filter): {matched_after_filter}")
        print(f"   Has More Data: {has_more_data}")
        if has_more_data:
            print(f"   Next Start: {next_start} (remaining: {remaining_listings})")
        else:
            print(f"   ✅ This was the last batch!")
        print("="*80 + "\n")
        
        return {
            "success": True,
            "fetch_id": fetch_id,
            "total_trucks": len(processed_trucks),
            "filename": output_filename,
            "file_path": output_path,
            "preview_data": processed_trucks[:10],  # First 10 rows for preview
            "message": f"Successfully fetched {len(processed_trucks)} trucks from database. Ready for annotation.",
            # Pagination guidance
            "pagination": {
                "first_ad_id": first_ad_id,
                "last_ad_id": last_ad_id,
                "listing_start": request.listing_start,
                "listing_end": request.listing_end,
                "total_available": total_available_in_db,
                "has_more_data": has_more_data,
                "next_suggested_start": next_start if has_more_data else None,
                "next_suggested_end": min(next_start + 1000, total_available_in_db) if has_more_data else None,
                "remaining_listings": remaining_listings,
                # Filter-specific info
                "filters_applied": filters_applied,
                "category_filters": request.category_filters if filters_applied else None,
                "fetched_before_filter": fetched_before_filter if filters_applied else None,
                "matched_after_filter": matched_after_filter if filters_applied else None
            }
        }
        
    except Exception as e:
        print(f"\n❌ DB FETCH FAILED: {str(e)}\n")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to fetch data from database: {str(e)}")


@app.get("/api/db-fetch-by-ids/{fetch_id}/status")
async def get_fetch_by_ids_status(fetch_id: str):
    """
    Get the status of a DB fetch by IDs job
    
    Returns fetch status and preview data when ready
    """
    if fetch_id not in jobs:
        raise HTTPException(status_code=404, detail="Fetch ID not found")
    
    fetch_job = jobs[fetch_id]
    
    return {
        "fetch_id": fetch_id,
        "status": fetch_job.get('status'),
        "total_trucks": fetch_job.get('total_ads', 0),
        "filename": fetch_job.get('output_filename', fetch_job.get('filename')),
        "preview_data": fetch_job.get('preview_data', []),
        "error": fetch_job.get('error')
    }


@app.post("/api/db-fetch/{fetch_id}/start-annotation")
async def start_db_annotation(fetch_id: str, background_tasks: BackgroundTasks):
    """
    Start AI annotation on already-fetched database data
    
    This endpoint:
    1. Loads the fetched data from the previous fetch
    2. Runs AI annotation pipeline
    3. Returns job ID for tracking
    """
    if fetch_id not in jobs:
        raise HTTPException(status_code=404, detail="Fetch ID not found")
    
    fetch_job = jobs[fetch_id]
    
    if fetch_job.get('status') != 'fetched':
        raise HTTPException(status_code=400, detail="This fetch has already been processed or is invalid")
    
    file_path = fetch_job.get('file_path')
    
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Fetched data file not found")
    
    # Create annotation job
    job_id = str(uuid.uuid4())[:8]
    
    jobs[job_id] = {
        "id": job_id,
        "filename": fetch_job.get('filename'),
        "file_path": file_path,
        "status": JobStatus.PROCESSING,
        "total_ads": fetch_job.get('total_ads'),
        "created_at": datetime.now().isoformat(),
        "is_db_fetch": True,
        "parent_fetch_id": fetch_id
    }
    
    # Update fetch job status
    fetch_job['status'] = 'annotating'
    fetch_job['annotation_job_id'] = job_id
    
    # Start annotation pipeline in background
    background_tasks.add_task(run_db_annotation_pipeline_sync, job_id, file_path)
    
    print(f"\n🤖 Starting AI annotation for fetch {fetch_id} (job {job_id})")
    
    return {
        "job_id": job_id,
        "status": JobStatus.PROCESSING,
        "message": f"AI annotation started for {fetch_job.get('total_ads')} trucks"
    }


@app.get("/api/db-fetch/{fetch_id}/download")
async def download_db_fetch_data(fetch_id: str):
    """Download the fetched database Excel file (before annotation)"""
    if fetch_id not in jobs:
        raise HTTPException(status_code=404, detail="Fetch ID not found")
    
    fetch_job = jobs[fetch_id]
    file_path = fetch_job.get('file_path')
    
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Fetched data file not found")
    
    filename = fetch_job.get('output_filename', fetch_job.get('filename', 'db_fetch_data.xlsx'))
    
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@app.post("/api/db-fetch-by-ids")
async def fetch_by_ad_ids(
    file: UploadFile = File(..., description="Excel file with Ad IDs"),
    background_tasks: BackgroundTasks = None
):
    """
    NEW FEATURE: Fetch trucks by Ad IDs from uploaded Excel file (PRODUCTION)
    
    This is a faster alternative to scraping:
    1. User uploads Excel file with Ad IDs (just like scraping feature)
    2. System fetches data directly from database API (1-2 seconds per truck)
    3. Returns formatted data ready for annotation
    
    Much faster than scraping (~10-15 seconds per truck)!
    """
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are supported")
    
    job_id = str(uuid.uuid4())[:8]

    # Save uploaded file
    upload_filename = f"upload_{job_id}_db_fetch_ids_{file.filename}"
    if getattr(config, 'enable_uploads', True):
        upload_dir = os.path.join(config.project_root, "uploads")
        os.makedirs(upload_dir, exist_ok=True)
        file_path = os.path.join(upload_dir, upload_filename)
    else:
        file_path = _temp_path(upload_filename)
        _register_temp_file(file_path)
    
    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)
    
    # Validate file has Ad ID column
    try:
        df = pd.read_excel(file_path)
        if "Ad ID" not in df.columns:
            os.remove(file_path)
            raise HTTPException(status_code=400, detail="Excel file must have an 'Ad ID' column")
        ad_count = df["Ad ID"].nunique()  # Count unique IDs, not raw rows (input may have duplicates)
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(status_code=400, detail=f"Invalid Excel file: {str(e)}")

    # Use credentials from config.ini (strip whitespace)
    client_id = config.db_api_client_id.strip()
    client_secret = config.db_api_client_secret.strip()
    grant_type = config.db_api_grant_type.strip()
    
    # Validate credentials
    if not client_id or client_id == 'your_client_id_here':
        raise HTTPException(
            status_code=400,
            detail="DB API credentials not configured. Please add credentials to config.ini"
        )
    
    if not client_secret or client_secret == 'your_client_secret_here':
        raise HTTPException(
            status_code=400,
            detail="DB API credentials not configured. Please add credentials to config.ini"
        )
    
    # Create job
    jobs[job_id] = {
        "id": job_id,
        "filename": file.filename,
        "file_path": file_path,
        "status": "fetching",  # New status for fetching phase
        "total_ads": ad_count,
        "created_at": datetime.now().isoformat(),
        "is_db_fetch_by_ids": True
    }
    
    # Start fetching in background (will update job to 'fetched' when done)
    background_tasks.add_task(
        run_db_fetch_by_ids_sync,
        job_id,
        file_path,
        client_id,
        client_secret,
        grant_type
    )
    
    return {
        "fetch_id": job_id,  # Return as fetch_id (not job_id) for preview modal
        "job_id": job_id,
        "filename": file.filename,
        "total_trucks": ad_count,
        "status": "fetching",
        "message": f"Fetching {ad_count} trucks from database API. This is much faster than scraping!"
    }


# ==================== DB CATEGORY UPDATE FEATURE ====================

# Category name -> ID mapping for the Trader API
# Categories without a real dev ID use placeholder values (will be replaced with prod IDs later)
CATEGORY_ID_MAP = {
    "Flatbed Truck": "2000617",
    "Pickup Truck": "2000635",
    "Mechanics Truck": "644245525",
    "Utility Truck - Service Truck": "2002561",
    "Dump Truck": "2000609",
    "Flatbed Dump": "2011212",
    "Landscape Truck": "2000625",
    "Contractor Truck": "644247521",
    "Stake Bed": "2014892",
    "Hauler": "2005520",
    "Cab Chassis": "2000881",
    "Stepvan": "2013294",
    "Selfloader": "2007240",
    "Bucket Truck - Boom Truck": "2005161",
    "Cabover Truck - COE": "2000559",
    "Box Truck - Straight Truck": "2002281",
    "Moving Van": "2012012",
    "Refrigerated Truck": "2000641",
    "Cutaway-Cube Van": "644245665",
    "Van": "2000523",
    "Cargo Van": "2011732",
    "Passanger Van": "644245480",
    "Rollback - Tow Truck": "2009720",
    "Conventional Day Cab": "2000601",
    "Dually": "644245588",
    "Dry Van": "644245645",
}

def _normalize_category_key(name: str) -> str:
    """Normalize a category name for fuzzy matching: lowercase, collapse
    hyphens/underscores/extra spaces into a single space, strip edges."""
    import re
    return re.sub(r'[\s\-_]+', ' ', name).strip().lower()

# Build a normalized lookup: normalized_key -> (original_name, id)
_CATEGORY_NORMALIZED_MAP = {
    _normalize_category_key(k): (k, v) for k, v in CATEGORY_ID_MAP.items()
}

def lookup_category(raw_name: str):
    """Look up a category by name, tolerant of hyphens/spaces/case differences.
    Returns (canonical_name, category_id) or (None, None) if not found."""
    # Try exact match first (fast path)
    cat_id = CATEGORY_ID_MAP.get(raw_name)
    if cat_id:
        return raw_name, cat_id
    # Fall back to normalized match
    normalized = _normalize_category_key(raw_name)
    match = _CATEGORY_NORMALIZED_MAP.get(normalized)
    if match:
        return match  # (canonical_name, id)
    return None, None

# Statuses that should be SKIPPED during DB update iteration
DB_UPDATE_SKIP_STATUSES = [
    "no images present",
    "inactive",
    "inactive ad",
    "image not clear",
    "no change",
    "exclusion rule conflict",
]


def get_db_update_bearer_token(token_url: str, client_id: str, client_secret: str, grant_type: str) -> dict:
    """
    Fetches a bearer token from the Trader API token endpoint.
    Returns a dict with 'access_token', 'expires_at' (unix timestamp), and 'expires_in'.
    Raises an exception on failure.
    """
    response = requests.post(
        token_url,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": grant_type,
        },
        timeout=30,
    )
    if response.status_code != 200:
        raise Exception(f"Token API returned status {response.status_code}: {response.text}")
    
    token_data = response.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise Exception(f"No access_token in token response: {token_data}")
    
    expires_in = token_data.get("expires_in", 3600)
    # Refresh 5 minutes before actual expiry as a safety buffer
    expires_at = time.time() + max(expires_in - 300, 60)
    
    print(f"   ✅ Bearer token obtained successfully (expires in {expires_in}s, will refresh at {expires_in - 300}s)")
    return {
        "access_token": access_token,
        "expires_at": expires_at,
        "expires_in": expires_in,
    }


def update_ad_categories_in_db(
    base_url: str, ad_id: str, categories: list, bearer_token: str
) -> dict:
    """
    Calls PUT {base_url}/{ad_id} to update the categories for a single ad.
    
    Args:
        base_url: e.g. https://api-dev.traderonline.com/vLatest/trucks
        ad_id: The ad ID string
        categories: List of {"id": "...", "name": "..."} dicts
        bearer_token: The bearer token string
    
    Returns:
        dict with "success" bool and optional "error" message
    """
    url = f"{base_url}/{ad_id}"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    body = {
        "ad": {
            "categories": categories
        }
    }
    
    try:
        response = requests.put(url, json=body, headers=headers, timeout=30)
        if response.status_code in (200, 201, 204):
            return {"success": True}
        else:
            return {
                "success": False,
                "error": f"HTTP {response.status_code}: {response.text[:200]}"
            }
    except Exception as e:
        return {"success": False, "error": str(e)}


def _derive_patch_base_url(update_base_url: str) -> str:
    """
    Derives the ad-patches API base URL from the existing update base URL.
    e.g. 'https://nebulous-prod.traderonline.com/vLatest/trucks'
      -> 'https://nebulous-prod.traderonline.com/v1/ad-patches'
    """
    from urllib.parse import urlparse
    parsed = urlparse(update_base_url)
    return f"{parsed.scheme}://{parsed.netloc}/v1/ad-patches"


def check_ad_patch(patch_base_url: str, ad_id: str, bearer_token: str) -> dict:
    """
    Checks if an ad has a patch by calling GET {patch_base_url}/{ad_id}.
    
    Returns:
        dict with keys:
            - "has_patch": bool (True if 200, False if 404)
            - "has_categories": bool (True if patch contains a non-empty 'categories' list)
            - "categories": list of category dicts from the patch (empty if no patch / no categories)
            - "error": optional error string if the request itself failed
    """
    url = f"{patch_base_url}/{ad_id}"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            result = data.get("result", {})
            categories = result.get("categories", [])
            has_categories = isinstance(categories, list) and len(categories) > 0
            print(f"      🔍 Patch check for Ad {ad_id}: PATCH FOUND | categories in patch: {has_categories}")
            if has_categories:
                cat_names = [c.get("name", c.get("id", "?")) for c in categories]
                print(f"         Patched categories: {cat_names}")
            return {
                "has_patch": True,
                "has_categories": has_categories,
                "categories": categories,
            }
        elif response.status_code == 404:
            print(f"      🔍 Patch check for Ad {ad_id}: NO PATCH (404)")
            return {
                "has_patch": False,
                "has_categories": False,
                "categories": [],
            }
        else:
            error_msg = f"Patch check returned HTTP {response.status_code}: {response.text[:200]}"
            print(f"      ⚠️ Patch check for Ad {ad_id}: {error_msg}")
            return {
                "has_patch": False,
                "has_categories": False,
                "categories": [],
                "error": error_msg,
            }
    except Exception as e:
        error_msg = f"Patch check request failed: {str(e)}"
        print(f"      ⚠️ Patch check for Ad {ad_id}: {error_msg}")
        return {
            "has_patch": False,
            "has_categories": False,
            "categories": [],
            "error": error_msg,
        }


def delete_patch_categories(patch_base_url: str, ad_id: str, bearer_token: str) -> dict:
    """
    Deletes the categories field from an ad's patch by calling
    DELETE {patch_base_url}/{ad_id}?field=categories
    
    Expects 204 on success.
    
    Returns:
        dict with "success" bool and optional "error" message
    """
    url = f"{patch_base_url}/{ad_id}?field=categories"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    try:
        response = requests.delete(url, headers=headers, timeout=30)
        if response.status_code == 204:
            print(f"      🗑️ Successfully deleted categories from patch for Ad {ad_id} (204)")
            return {"success": True}
        else:
            error_msg = f"Patch category delete returned HTTP {response.status_code}: {response.text[:200]}"
            print(f"      ❌ Failed to delete patch categories for Ad {ad_id}: {error_msg}")
            return {"success": False, "error": error_msg}
    except Exception as e:
        error_msg = f"Patch category delete request failed: {str(e)}"
        print(f"      ❌ Failed to delete patch categories for Ad {ad_id}: {error_msg}")
        return {"success": False, "error": error_msg}


def create_or_update_ad_patch_categories(
    patch_base_url: str, ad_id: str, categories: list, had_patch: bool, bearer_token: str
) -> dict:
    """
    After a successful ad category update, creates or updates the ad-patch
    so it includes the newly AI-annotated categories.

    - If had_patch is True:  GET current patch fields → PUT with existing fields + new categories
    - If had_patch is False: PUT with only categories

    Args:
        patch_base_url: e.g. https://nebulous-prod.traderonline.com/v1/ad-patches
        ad_id:          The ad ID string
        categories:     List of {"id": "...", "name": "..."} dicts (AI-annotated)
        had_patch:      Whether the ad already had a patch before the update
        bearer_token:   The bearer token string

    Returns:
        dict with "success" bool, optional "error" message, and "action" ("created" | "updated")
    """
    url = f"{patch_base_url}/{ad_id}"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }

    patch_body = {}
    action = "created"  # default: no prior patch

    if had_patch:
        action = "updated"
        # ── GET existing patch fields so we can preserve them ──
        print(f"      📥 [Patch Step] Ad {ad_id}: Patch exists — fetching current patch fields...")
        try:
            get_resp = requests.get(url, headers=headers, timeout=30)
            if get_resp.status_code == 200:
                result = get_resp.json().get("result", {})
                skipped_fields = []
                # Copy every field except 'id' and 'categories' (we'll add new categories)
                for key, value in result.items():
                    if key in ("id", "categories"):
                        continue
                    # Skip None/null values — API rejects them
                    if value is None:
                        skipped_fields.append(f"{key}(null)")
                        continue
                    # Skip nested dicts/lists — only simple scalar fields belong in patch
                    if isinstance(value, (dict, list)):
                        skipped_fields.append(f"{key}(complex)")
                        continue
                    # Convert everything to string (API expects strings in PUT body)
                    patch_body[key] = str(value)
                
                copied_keys = list(patch_body.keys())
                print(f"      📥 [Patch Step] Ad {ad_id}: Copied {len(patch_body)} existing patch fields: {copied_keys}")
                if skipped_fields:
                    print(f"      ⏭️ [Patch Step] Ad {ad_id}: Skipped fields: {skipped_fields}")
            else:
                print(f"      ⚠️ [Patch Step] Ad {ad_id}: GET patch returned HTTP {get_resp.status_code} — will PUT with categories only")
        except Exception as e:
            print(f"      ⚠️ [Patch Step] Ad {ad_id}: Failed to GET existing patch: {e} — will PUT with categories only")
    else:
        print(f"      📥 [Patch Step] Ad {ad_id}: No prior patch — will create new patch with categories only")

    # Append the new AI-annotated categories
    patch_body["categories"] = categories

    # ── PUT the patch ──
    body = {"patch": patch_body}
    cat_names = [c.get("name", c.get("id", "?")) for c in categories]
    print(f"      📤 [Patch Step] Ad {ad_id}: Sending PUT to {action} patch with categories: {cat_names}")

    try:
        put_resp = requests.put(url, json=body, headers=headers, timeout=30)
        if put_resp.status_code in (200, 201, 204):
            print(f"      ✅ [Patch Step] Ad {ad_id}: Patch {action} successfully (HTTP {put_resp.status_code})")
            return {"success": True, "action": action}
        else:
            error_msg = f"Patch PUT returned HTTP {put_resp.status_code}: {put_resp.text[:200]}"
            print(f"      ❌ [Patch Step] Ad {ad_id}: Failed to {action[:-1]}e patch — {error_msg}")
            return {"success": False, "error": error_msg, "action": action}
    except Exception as e:
        error_msg = f"Patch PUT request failed: {str(e)}"
        print(f"      ❌ [Patch Step] Ad {ad_id}: Failed to {action[:-1]}e patch — {error_msg}")
        return {"success": False, "error": error_msg, "action": action}


# Global storage for patch summary reports (keyed by report ID)
db_update_patch_reports: Dict[str, dict] = {}


@app.get("/api/db-update-config")
async def get_db_update_config():
    """
    Returns the DB Update API configuration (without the secret) 
    so the frontend can display pre-filled values.
    """
    return {
        "token_url": getattr(config, 'db_update_token_url', ''),
        "update_base_url": getattr(config, 'db_update_base_url', ''),
        "client_id": getattr(config, 'db_update_client_id', ''),
        "grant_type": getattr(config, 'db_update_grant_type', 'client_credentials'),
        "has_secret": bool(getattr(config, 'db_update_client_secret', '')),
    }


@app.post("/api/db-update")
async def db_update_categories(
    file: UploadFile = File(..., description="AI Output Excel file with annotated categories"),
    client_secret: str = Form(None),
):
    """
    DB Category Update Feature:
    1. Accepts an AI output Excel file
    2. Filters rows where Status == "Require Update"
    3. Skips: No Images Present, Inactive, Image not clear, No change
    4. Gets bearer token from Trader API
    5. Calls PUT for each qualifying ad to update categories
    6. Returns summary of results
    """
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are supported")
    
    # Save uploaded file temporarily
    upload_dir = os.path.join(config.project_root, "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    temp_id = str(uuid.uuid4())[:8]
    file_path = os.path.join(upload_dir, f"{temp_id}_db_update_{file.filename}")
    
    try:
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        # Read Excel
        df = pd.read_excel(file_path, dtype={"Ad ID": str})
        df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # Validate required columns
        required_cols = ["Ad ID", "Status", "Annotated_Top1"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise HTTPException(
                status_code=400,
                detail=f"Excel file is missing required columns: {', '.join(missing_cols)}"
            )
        
        total_rows = len(df)
        
        # Filter rows: only "Require Update"
        update_rows = []
        skipped_rows = []
        
        for idx, row in df.iterrows():
            status = str(row.get("Status", "")).strip()
            ad_id = str(row.get("Ad ID", "")).strip()
            
            if not ad_id or ad_id.lower() == "nan":
                skipped_rows.append({"ad_id": "EMPTY", "reason": "Empty Ad ID"})
                continue
            
            status_lower = status.lower()
            
            # Check if this status should be skipped
            should_skip = False
            for skip_status in DB_UPDATE_SKIP_STATUSES:
                if skip_status in status_lower:
                    should_skip = True
                    skipped_rows.append({"ad_id": ad_id, "reason": status})
                    break
            
            if should_skip:
                continue
            
            # Only process "Require Update" rows
            if status_lower == "require update":
                update_rows.append(row)
            else:
                skipped_rows.append({"ad_id": ad_id, "reason": f"Unknown status: {status}"})
        
        if not update_rows:
            return {
                "status": "completed",
                "message": "No rows with 'Require Update' status found in the uploaded file.",
                "total_rows": total_rows,
                "update_count": 0,
                "skipped_count": len(skipped_rows),
                "success_count": 0,
                "failed_count": 0,
                "results": [],
                "skipped": skipped_rows[:50],  # Limit for response size
            }
        
        # Get credentials
        token_url = getattr(config, 'db_update_token_url', '').strip()
        base_url = getattr(config, 'db_update_base_url', '').strip()
        c_id = getattr(config, 'db_update_client_id', '').strip()
        c_secret = (client_secret or getattr(config, 'db_update_client_secret', '')).strip()
        c_grant = getattr(config, 'db_update_grant_type', 'client_credentials').strip()
        
        if not token_url:
            raise HTTPException(status_code=400, detail="DB Update API TokenUrl not configured in config.ini")
        if not base_url:
            raise HTTPException(status_code=400, detail="DB Update API UpdateBaseUrl not configured in config.ini")
        if not c_id:
            raise HTTPException(status_code=400, detail="DB Update API ClientId not configured in config.ini")
        if not c_secret:
            raise HTTPException(status_code=400, detail="DB Update API ClientSecret not configured. Please enter it manually or add it to config.ini")
        
        # Step 1: Get bearer token
        print("\n" + "=" * 80)
        print("🔑 DB CATEGORY UPDATE: OBTAINING BEARER TOKEN")
        print("=" * 80)
        
        try:
            token_info = get_db_update_bearer_token(token_url, c_id, c_secret, c_grant)
            bearer_token = token_info["access_token"]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to obtain bearer token: {str(e)}")
        
        # Derive the ad-patches base URL from the update base URL
        patch_base_url = _derive_patch_base_url(base_url)
        print(f"   📌 Patch API base URL: {patch_base_url}")
        
        # ── Pre-validate all rows (single-threaded, fast) ──
        # This separates category-lookup / validation from the network-bound work
        prepared_ads = []   # List of (ad_id, categories_list, cat_names) ready for API calls
        pre_skip_results = []  # Results for rows skipped during validation
        
        for i, row in enumerate(update_rows, 1):
            ad_id = str(row.get("Ad ID", "")).strip()
            
            categories = []
            unmapped_cats = []
            for col in ["Annotated_Top1", "Annotated_Top2", "Annotated_Top3"]:
                cat_name = str(row.get(col, "")).strip() if pd.notna(row.get(col)) else ""
                if cat_name and cat_name.lower() not in ["", "nan", "none"]:
                    canonical_name, cat_id = lookup_category(cat_name)
                    if cat_id:
                        categories.append({"id": cat_id, "name": canonical_name})
                    else:
                        unmapped_cats.append(cat_name)
            
            if not categories and not unmapped_cats:
                pre_skip_results.append({
                    "ad_id": ad_id,
                    "success": False,
                    "error": "No annotated categories found",
                    "categories": []
                })
                print(f"   [pre-check] ❌ Ad {ad_id}: No annotated categories found")
                continue
            
            if unmapped_cats:
                error_msg = f"Skipped — unmapped category: {', '.join(unmapped_cats)}"
                pre_skip_results.append({
                    "ad_id": ad_id,
                    "success": False,
                    "error": error_msg,
                    "categories": [c["name"] for c in categories] + unmapped_cats
                })
                print(f"   [pre-check] ⚠️ Ad {ad_id}: {error_msg}")
                continue
            
            cat_names = [c["name"] for c in categories]
            prepared_ads.append((ad_id, categories, cat_names))
        
        # Step 2: Process prepared ads with MULTITHREADING
        max_workers = 5
        print("\n" + "=" * 80)
        print(f"📝 DB CATEGORY UPDATE: PROCESSING {len(prepared_ads)} ADS (MULTITHREADED, {max_workers} workers)")
        print(f"   ⏭️ Pre-skipped: {len(pre_skip_results)} ads (validation failures)")
        print(f"   🚀 Using {max_workers} concurrent workers for ~{max_workers}x speedup!")
        print("=" * 80)
        
        # Thread-safe token holder with lock for refresh
        token_lock = threading.Lock()
        token_holder = {"access_token": bearer_token, "expires_at": token_info["expires_at"]}
        
        def _get_valid_token():
            """Get current bearer token, refreshing if expired (thread-safe)."""
            with token_lock:
                if time.time() >= token_holder["expires_at"]:
                    print("   🔄 Bearer token expiring soon, refreshing...")
                    try:
                        new_info = get_db_update_bearer_token(token_url, c_id, c_secret, c_grant)
                        token_holder["access_token"] = new_info["access_token"]
                        token_holder["expires_at"] = new_info["expires_at"]
                    except Exception as e:
                        print(f"   ⚠️ Token refresh failed: {e} — continuing with old token")
                return token_holder["access_token"]
        
        def _process_single_ad(ad_data, idx, total):
            """
            Worker function: check patch → delete patch categories if needed → PUT update.
            Returns (result_dict, patch_summary_dict_or_None).
            """
            ad_id, categories, cat_names = ad_data
            current_token = _get_valid_token()
            
            print(f"\n   [{idx}/{total}] 🔎 Processing Ad {ad_id} ...")
            patch_info = check_ad_patch(patch_base_url, ad_id, current_token)
            
            patch_deleted = False
            old_patch_categories = []
            patch_summary = None
            
            if patch_info.get("has_patch") and patch_info.get("has_categories"):
                old_patch_categories = patch_info["categories"]
                old_cat_names = [c.get("name", c.get("id", "?")) for c in old_patch_categories]
                print(f"      ⚡ Ad {ad_id} has PATCHED categories: {old_cat_names}")
                print(f"      🗑️ Deleting categories from patch before PUT update...")
                
                # Re-fetch token in case it expired during previous calls
                current_token = _get_valid_token()
                delete_result = delete_patch_categories(patch_base_url, ad_id, current_token)
                
                if not delete_result["success"]:
                    error_msg = f"Failed to delete patch categories: {delete_result.get('error', 'Unknown error')}"
                    print(f"   [{idx}/{total}] ❌ Ad {ad_id}: {error_msg}")
                    return (
                        {"ad_id": ad_id, "success": False, "error": error_msg, "categories": cat_names},
                        {"ad_id": ad_id, "old_patched_categories": ", ".join(old_cat_names),
                         "new_updated_categories": ", ".join(cat_names),
                         "patch_deleted": "FAILED", "update_success": "No (patch delete failed)"},
                    )
                
                patch_deleted = True
                print(f"      ✅ Patch categories deleted successfully for Ad {ad_id}")
            elif patch_info.get("has_patch") and not patch_info.get("has_categories"):
                print(f"      ℹ️ Ad {ad_id} has a patch but NO categories in it — proceeding with direct PUT")
            else:
                print(f"      ℹ️ Ad {ad_id} has no patch — proceeding with direct PUT")
            
            # ── PUT UPDATE ──
            current_token = _get_valid_token()
            print(f"      📤 Sending PUT to update Ad {ad_id} with categories: {cat_names}")
            result = update_ad_categories_in_db(base_url, ad_id, categories, current_token)
            
            if result["success"]:
                print(f"   [{idx}/{total}] ✅ Ad {ad_id}: Updated -> {cat_names}")
            else:
                print(f"   [{idx}/{total}] ❌ Ad {ad_id}: {result.get('error', 'Unknown error')}")
            
            # ── PATCH STEP: Create or update ad-patch with new categories ──
            patch_action = None       # "created" | "updated" | None
            patch_step_success = False
            patch_step_error = ""
            
            if result["success"]:
                print(f"\n      🩹 [Patch Step] Ad {ad_id}: Ad update succeeded — now creating/updating patch with categories...")
                current_token = _get_valid_token()
                patch_result = create_or_update_ad_patch_categories(
                    patch_base_url, ad_id, categories,
                    had_patch=patch_info.get("has_patch", False),
                    bearer_token=current_token,
                )
                patch_step_success = patch_result["success"]
                patch_action = patch_result.get("action")
                patch_step_error = patch_result.get("error", "")
                if patch_step_success:
                    print(f"   [{idx}/{total}] 🩹 Ad {ad_id}: Patch {patch_action} successfully with categories: {cat_names}")
                else:
                    print(f"   [{idx}/{total}] ⚠️ Ad {ad_id}: Ad updated OK but patch step failed — {patch_step_error}")
            else:
                print(f"      ⏭️ [Patch Step] Ad {ad_id}: Skipping patch step because ad update failed")
            
            result_dict = {
                "ad_id": ad_id,
                "success": result["success"],
                "error": result.get("error", ""),
                "categories": cat_names,
                "patch_action": patch_action,           # "created" | "updated" | None
                "patch_step_success": patch_step_success,
                "patch_step_error": patch_step_error,
            }
            
            # Build patch summary if a patch action was taken OR this ad had patch categories
            if patch_action or patch_deleted or (patch_info.get("has_patch") and patch_info.get("has_categories")):
                old_cat_names = [c.get("name", c.get("id", "?")) for c in old_patch_categories]
                patch_summary = {
                    "ad_id": ad_id,
                    "old_patched_categories": ", ".join(old_cat_names),
                    "new_updated_categories": ", ".join(cat_names),
                    "patch_deleted": "Yes" if patch_deleted else "No",
                    "update_success": "Yes" if result["success"] else f"No ({result.get('error', 'Unknown')})",
                    "patch_step": f"{patch_action} ({'OK' if patch_step_success else 'FAILED'})" if patch_action else "N/A",
                }
            
            return (result_dict, patch_summary)
        
        # ── Run with ThreadPoolExecutor ──
        results = list(pre_skip_results)  # Start with pre-skipped results
        patched_ads_summary = []
        update_start = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_ad = {
                executor.submit(_process_single_ad, ad_data, i, len(prepared_ads)): ad_data
                for i, ad_data in enumerate(prepared_ads, 1)
            }
            
            for future in as_completed(future_to_ad):
                try:
                    result_dict, patch_summary = future.result()
                    results.append(result_dict)
                    if patch_summary:
                        patched_ads_summary.append(patch_summary)
                except Exception as e:
                    ad_data = future_to_ad[future]
                    ad_id = ad_data[0]
                    print(f"   ❌ Worker exception for Ad {ad_id}: {e}")
                    results.append({
                        "ad_id": ad_id,
                        "success": False,
                        "error": f"Worker exception: {str(e)}",
                        "categories": ad_data[2],
                    })
        
        update_elapsed = time.time() - update_start
        
        # Count successes/failures from all results
        success_count = sum(1 for r in results if r["success"])
        failed_count = sum(1 for r in results if not r["success"])
        
        # Count patch step outcomes
        patches_created = sum(1 for r in results if r.get("patch_action") == "created" and r.get("patch_step_success"))
        patches_updated = sum(1 for r in results if r.get("patch_action") == "updated" and r.get("patch_step_success"))
        patches_failed  = sum(1 for r in results if r.get("patch_action") and not r.get("patch_step_success"))
        
        # Store patch report if any patched ads were found
        patch_report_id = None
        if patched_ads_summary:
            patch_report_id = str(uuid.uuid4())[:8]
            db_update_patch_reports[patch_report_id] = {
                "created_at": datetime.now().isoformat(),
                "summary": patched_ads_summary,
            }
            print(f"\n   📋 Patch summary report stored with ID: {patch_report_id} ({len(patched_ads_summary)} patched ads)")
        
        # Summary
        print("\n" + "=" * 80)
        print(f"🎉 DB CATEGORY UPDATE COMPLETE ({update_elapsed:.1f}s with {max_workers} workers)")
        print(f"   Total rows in file: {total_rows}")
        print(f"   Rows to update: {len(update_rows)}")
        print(f"   ✅ Ad updates successful: {success_count}")
        print(f"   ❌ Ad updates failed: {failed_count}")
        print(f"   ⏭️ Skipped: {len(skipped_rows)}")
        print(f"   🩹 Patched ads (had categories in patch): {len(patched_ads_summary)}")
        print(f"   🩹 Patch step — created: {patches_created}, updated: {patches_updated}, failed: {patches_failed}")
        print(f"   ⚡ Speed: {max_workers} concurrent workers")
        print("=" * 80 + "\n")
        
        return {
            "status": "completed",
            "message": f"DB update completed in {update_elapsed:.1f}s. {success_count} successful, {failed_count} failed, {len(skipped_rows)} skipped. Patch step: {patches_created} created, {patches_updated} updated, {patches_failed} failed.",
            "total_rows": total_rows,
            "update_count": len(update_rows),
            "skipped_count": len(skipped_rows),
            "success_count": success_count,
            "failed_count": failed_count,
            "patched_count": len(patched_ads_summary),
            "patches_created": patches_created,
            "patches_updated": patches_updated,
            "patches_failed": patches_failed,
            "patch_report_id": patch_report_id,
            "results": results,
            "skipped": skipped_rows[:50],  # Limit for response size
        }
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ DB Update error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"DB Update failed: {str(e)}")
    finally:
        # Clean up temp file
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass


@app.get("/api/db-update/patch-report/{report_id}/download")
async def download_patch_report(report_id: str):
    """
    Downloads an Excel summary of all ads that had patched categories
    during a DB update run. Shows old patched categories, new updated
    categories, whether the patch was deleted, and update success.
    """
    if report_id not in db_update_patch_reports:
        raise HTTPException(status_code=404, detail="Patch report not found. It may have expired or the server was restarted.")
    
    report_data = db_update_patch_reports[report_id]
    summary = report_data["summary"]
    
    if not summary:
        raise HTTPException(status_code=404, detail="Patch report is empty — no patched ads were recorded.")
    
    # Build DataFrame from summary
    df = pd.DataFrame(summary)
    # Rename columns to human-readable headers (order matches dict keys in patch_summary)
    column_map = {
        "ad_id": "Ad ID",
        "old_patched_categories": "Old Patched Categories",
        "new_updated_categories": "New Updated Categories",
        "patch_deleted": "Patch Deleted",
        "update_success": "Update Success",
        "patch_step": "Patch Create/Update Step",
    }
    df.rename(columns=column_map, inplace=True)
    
    # Write patch summary Excel (served immediately, not read back — always goes to temp)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"patch_summary_{report_id}_{timestamp}.xlsx"
    if getattr(config, 'enable_uploads', True):
        report_dir = os.path.join(config.project_root, "uploads")
        os.makedirs(report_dir, exist_ok=True)
        file_path = os.path.join(report_dir, filename)
    else:
        file_path = _temp_path(filename)
        _register_temp_file(file_path)

    df.to_excel(file_path, index=False, engine="openpyxl")
    print(f"📥 Patch report Excel generated: {file_path} ({len(summary)} rows)")
    
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    freeze_support()  # Required for Windows multiprocessing
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
