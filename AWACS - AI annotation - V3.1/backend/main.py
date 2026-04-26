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
from fastapi.responses import FileResponse, RedirectResponse
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

from b2_storage import (
    init_b2, shutdown_b2, upload_to_b2_async, get_download_url,
    b2_key_for_file, list_b2_files, delete_b2_files, setup_lifecycle_rule,
    is_enabled as b2_is_enabled, get_b2_key_for_cdc_file, flush_uploads,
)
from ai_tool.config_loader import config, load_config
from ai_tool.rate_limiter import Yoda
from ai_tool.main_processor import save_checkpoint, merge_all_session_reports
from ai_tool.data_processing import load_rules, normalize_text
from ai_tool import web_utils, classification, ad_tracker, cdc_audit_logger
from ai_tool.awacs_logger import setup_logger
from ai_tool.time_utils import now_ist
import ai_module

logger = setup_logger("awacs.backend")

# Initialize config
load_config()

# Initialize Ad Annotation Limit Tracker (Turso DB)
if config.enable_ad_annotation_limit:
    logger.info("=" * 60)
    logger.info("📊 AD ANNOTATION LIMIT TRACKER: ✅ ENABLED")
    logger.info("   Max annotation runs per ad: %d", config.max_annotation_runs)
    logger.info("   Turso DB URL: %s...", config.turso_db_url[:40])
    try:
        ad_tracker.init_tracker(config.turso_db_url, config.turso_auth_token)
    except Exception as e:
        logger.error("   ❌ Ad Tracker initialization failed: %s", e)
        logger.warning("   ⚠️ Disabling Ad Annotation Limit for this session")
        config.enable_ad_annotation_limit = False
    logger.info("=" * 60)
else:
    logger.info("📊 AD ANNOTATION LIMIT TRACKER: ❌ DISABLED (bypassed)")

# Initialize CDC Audit Logger (Grafana Cloud Loki)
if config.enable_cdc_audit_log:
    logger.info("=" * 60)
    logger.info("📋 CDC AUDIT LOGGER: ✅ ENABLED (Grafana Loki)")
    logger.info("   Push URL: %s...", config.loki_push_url[:50])
    try:
        cdc_audit_logger.init_audit_logger(
            config.loki_push_url, config.loki_query_url,
            config.loki_user_id, config.loki_api_key,
        )
    except Exception as e:
        logger.error("   ❌ Audit Logger initialization failed: %s", e)
        logger.warning("   ⚠️ Disabling CDC Audit Logging for this session")
        config.enable_cdc_audit_log = False
    logger.info("=" * 60)
else:
    logger.info("📋 CDC AUDIT LOGGER: ❌ DISABLED")

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
# When B2 is enabled, files are backed up to cloud so local copies expire faster.
_TEMP_FILE_MAX_AGE_HOURS = 1 if getattr(config, 'b2_enabled', False) else 24

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
    logger.info("   [TEMP] Registered for cleanup: %s", os.path.basename(path))


def _upload_and_track(local_path: str, file_type: str, filename: str,
                      delete_local: bool = True) -> str:
    """Upload file to B2 (async) and return the B2 object key."""
    b2_key = b2_key_for_file(file_type, filename)
    upload_to_b2_async(local_path, b2_key, delete_local=delete_local)
    return b2_key


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
                logger.warning("   [TEMP] Could not delete %s: %s", os.path.basename(path), e)
    if deleted:
        logger.info("[TEMP CLEANUP] Deleted %d file(s) older than %dh:", len(deleted), _TEMP_FILE_MAX_AGE_HOURS)
        for name in deleted:
            logger.info("   - %s", name)
    else:
        logger.info("[TEMP CLEANUP] No files older than %dh — nothing to delete.", _TEMP_FILE_MAX_AGE_HOURS)


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
                logger.warning("   [TEMP] Could not delete leftover %s: %s", fname, e)
    if deleted:
        logger.info("[TEMP CLEANUP] Removed %d leftover file(s) from previous session:", len(deleted))
        for name in deleted:
            logger.info("   - %s", name)
    else:
        logger.info("[TEMP CLEANUP] No leftover files from previous session.")


def _temp_cleanup_loop():
    """Background thread: runs cleanup every _TEMP_CLEANUP_INTERVAL_SECONDS."""
    while True:
        time.sleep(_TEMP_CLEANUP_INTERVAL_SECONDS)
        logger.info("[TEMP CLEANUP] Background cleanup running (interval: %dh)...", _TEMP_CLEANUP_INTERVAL_SECONDS // 3600)
        _cleanup_old_temp_files()


@app.on_event("startup")
def on_startup():
    logger.info("[TEMP CLEANUP] Checking for leftover temp files from previous session...")
    _cleanup_leftover_awacs_temp_files()
    t = threading.Thread(target=_temp_cleanup_loop, daemon=True)
    t.start()
    logger.info("[TEMP CLEANUP] Background cleanup thread started — runs every %dh, deletes files older than %dh", _TEMP_CLEANUP_INTERVAL_SECONDS // 3600, _TEMP_FILE_MAX_AGE_HOURS)

    # Initialize Backblaze B2 cloud storage
    if init_b2(config):
        setup_lifecycle_rule()
        logger.info("[B2] Backblaze B2 cloud storage initialized")
    else:
        logger.info("[B2] Backblaze B2 disabled — using local storage only")


@app.on_event("shutdown")
def on_shutdown():
    logger.info("[TEMP CLEANUP] Server shutting down — running final cleanup...")
    _cleanup_old_temp_files()
    shutdown_b2()


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
    
    logger.info("=" * 80)
    logger.info("🤖 AI ANNOTATION PHASE STARTED (ULTRA-OPTIMIZED)")
    logger.info("=" * 80)
    logger.info("   Total Ads: %d", total)
    logger.info("   Workers: %d", num_workers)
    logger.info("   Models:")
    logger.info("      📋 Promo Check:         %s", config.gemini_model_promo_check)
    logger.info("      📋 Classification:      %s", config.gemini_model_classification)
    logger.info("      📋 Dually Verification: %s", config.gemini_model_dually_verification)
    logger.info("   API Keys: %d", len(config.gemini_api_keys))
    logger.info("   📋 [Rules.json] Will be loaded by each worker from: %s", config.rules_json)
    logger.info("   🔧 DUALLY DETECTION SETTINGS:")
    logger.info("      🌑 Darth CV2 (OpenCV) Detection: %s", '✅ ENABLED' if config.enable_darth_cv2_dually else '❌ DISABLED')
    if config.enable_darth_cv2_dually:
        logger.info("         └─ Threshold: %s (tire-like contours required)", config.darth_cv2_dually_threshold)
    logger.info("      🔍 Post-Processing LLM Verification: %s", '✅ ENABLED' if config.enable_dually_llm_verification else '❌ DISABLED')
    logger.info("=" * 80)
    
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
    logger.info("🧙 Initializing Yoda (Rate Limiter)...")
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
    logger.info("   ✅ Status queue drainer started")
    
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
        logger.info("   Started Worker-%d (PID: %d)", i, p.pid)
        time.sleep(0.3)  # ULTRA-OPTIMIZED: Reduced from 0.5s to 0.3s
    
    logger.info("   All %d workers started. Processing...", num_workers)
    logger.info("   📊 Real-time results will appear below:")
    
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
                
                logger.info("   ✅ [%d/%d] %s: %s | Status: %s | ⏱️ Avg: %.2fs/ad", len(results), total, ad_id, top1, status, avg_time_per_listing)
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
                logger.info("   ⏳ Progress: %d/%d done | %d workers active | ⏱️ %ds elapsed | ETA: %.0fs", len(results), total, alive, elapsed, eta)
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
    
    logger.info("=" * 80)
    logger.info("✅ AI ANNOTATION PHASE COMPLETE")
    logger.info("=" * 80)
    logger.info("   Processed: %d/%d ads", len(results), total)
    logger.info("   ⏱️ Total Time: %.2fs (%.2f minutes)", elapsed, elapsed / 60)
    logger.info("   ⏱️ Average Time per Ad: %.2fs", avg_time)
    logger.info("   📊 Processing Rate: %.1f ads/minute", len(results) / elapsed * 60)
    logger.info("=" * 80)
    
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
    
    logger.info("=" * 80)
    logger.info("🔍 DUALLY VERIFICATION PHASE - Multi-Threaded with 5 Workers (ULTRA-OPTIMIZED)")
    logger.info("=" * 80)
    
    # Find all rows that have "Dually" in any annotation column
    annotation_cols = ["Annotated_Top1", "Annotated_Top2", "Annotated_Top3"]
    
    dually_mask = result_df[annotation_cols].apply(
        lambda row: any("dually" in str(val).lower() for val in row), 
        axis=1
    )
    
    dually_listings = result_df[dually_mask].copy()
    
    if len(dually_listings) == 0:
        logger.info("   No listings marked as Dually. Skipping verification.")
        return result_df, 0  # Return 0 cost when no verification needed
    
    total_dually = len(dually_listings)
    num_workers = 5  # Fixed to 5 workers as requested
    
    logger.info("   Found %d listings marked as Dually", total_dually)
    logger.info("   🧵 Using %d workers for parallel verification", num_workers)
    logger.info("   📊 Expected speedup: ~%dx faster than sequential processing", num_workers)
    
    # Update job status for frontend
    if job_id in jobs:
        jobs[job_id]['status'] = JobStatus.VERIFYING_DUALLY
        jobs[job_id]['dually_total'] = int(total_dually)
        jobs[job_id]['dually_verified'] = 0
    
    # STEP 1: Pre-fetch all images first (PARALLEL for speed)
    if getattr(config, 'verbose_cache_logging', True):
        logger.info("   📥 STEP 1: Pre-fetching ALL images from cache...")
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
                    logger.info("   📸 Ad %s: Pre-fetched %d image(s)", ad_id, len(valid_images))
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
    logger.info("   ✅ Pre-fetched images for %d listings in %.2fs (PARALLEL)", len(prefetched_images), prefetch_time)

    if len(prefetched_images) == 0:
        logger.warning("   ⚠️ No images available for any dually listings. Skipping verification.")
        return result_df, 0
    
    # STEP 2: Setup multiprocessing resources
    logger.info("   🔧 STEP 2: Setting up %d workers...", num_workers)
    m = Manager()
    job_q = m.Queue()
    res_q = m.Queue()
    stat_q = m.Queue()
    key_q = m.Queue()
    
    # Load Keys
    for k in config.gemini_api_keys_info:
        key_q.put(k)
    logger.info("   🔑 Loaded %d API keys into queue", len(config.gemini_api_keys_info))

    # Load verification jobs into queue (skip rule-based dually that shouldn't be verified)
    logger.info("   📋 Loading verification jobs into queue...")
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
            logger.info("   ⏭️ Ad %s: Skipping verification — Landscape + Cabover COE (always dually by rule)", ad_id)
            continue

        verification_job = {
            'idx': idx,
            'ad_id': ad_id,
            'images': img_bytes_list,
            'row_data': row.to_dict()
        }
        job_q.put(verification_job)
        logger.info("   ➕ Added verification job for Ad %s to queue", ad_id)

    if skipped_rule_based > 0:
        logger.info("   ⏭️ Skipped %d listings (rule-based dually, no verification needed)", skipped_rule_based)
    
    jobs_loaded = len(prefetched_images) - skipped_rule_based
    logger.info("   ✅ %d jobs loaded into queue (%d skipped as rule-based dually)", jobs_loaded, skipped_rule_based)

    if jobs_loaded == 0:
        logger.warning("   ⚠️ No listings need dually verification after rule-based filtering. Skipping.")
        return result_df, 0

    # STEP 3: Start worker processes
    logger.info("   🚀 STEP 3: Starting %d worker processes...", num_workers)
    procs = []
    for i in range(1, num_workers + 1):
        p = Process(
            target=ai_module.start_dually_verification_worker,
            args=(i, job_q, res_q, stat_q, key_q, yoda_instance)
        )
        p.start()
        procs.append(p)
        logger.info("   ✅ Started Dually Verification Worker-%d (PID: %d)", i, p.pid)
        time.sleep(0.3)  # Small delay to avoid race conditions
    
    logger.info("   🏃 All %d workers started. Beginning parallel verification...", num_workers)
    logger.info("   📊 Real-time results will appear below:")
    
    # STEP 4: Collect results from workers
    verification_loop_start = time.time()
    verified_count = 0
    removed_count = 0
    error_count = 0
    total_cost = 0
    results_collected = 0
    
    logger.info("=" * 80)
    logger.info("📊 REAL-TIME VERIFICATION RESULTS")
    logger.info("=" * 80)
    
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
                logger.warning("   [%d/%d] ⚠️ %s: ERROR - %s | ⏱️ %.2fs | ETA: %.0fs", current_num, total_dually, ad_id, error_msg[:40], listing_time, eta)
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
                        logger.info("   [%d/%d] ✅ %s: CONFIRMED | Cost: %.4f¢ → Total: %.4f¢ | Time: %.2fs | Avg: %.2fs | Rate: %.1f/min | ETA: %.0fs", current_num, total_dually, ad_id, cost, new_cost, listing_time, avg_time, rate * 60, eta)
                    
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
                        logger.info("   [%d/%d] ❌ %s: FALSE POSITIVE - REMOVING | Cost: %.4f¢ → Total: %.4f¢ | Time: %.2fs | Avg: %.2fs | Rate: %.1f/min | ETA: %.0fs", current_num, total_dually, ad_id, cost, new_cost, listing_time, avg_time, rate * 60, eta)
                    
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
                logger.info("   ⏳ All workers finished. Breaking out of result collection loop.")
                break
            continue
    
    # Wait for all workers to finish
    logger.info("   ⏳ Waiting for all workers to complete...")
    for i, p in enumerate(procs, 1):
        p.join(timeout=30)
        if p.is_alive():
            logger.warning("   ⚠️ Worker-%d did not finish in time, terminating...", i)
            p.terminate()
            p.join()
        else:
            logger.info("   ✅ Worker-%d finished successfully", i)
    
    verification_loop_elapsed = time.time() - verification_loop_start
    total_verification_elapsed = time.time() - verification_start
    avg_verify_time = verification_loop_elapsed / total_dually if total_dually > 0 else 0
    
    # NO SLEEP HERE - Workers handle everything!
    
    verification_loop_elapsed = time.time() - verification_loop_start
    total_verification_elapsed = time.time() - verification_start
    avg_verify_time = verification_loop_elapsed / results_collected if results_collected > 0 else 0
    
    # VERIFICATION: Check that costs were actually added to the DataFrame
    if getattr(config, 'verbose_cache_logging', True):
        logger.info("=" * 80)
        logger.info("📊 COST VERIFICATION: Checking Cost_Cents Updates")
        logger.info("=" * 80)
        cost_sum_in_df = result_df['Cost_Cents'].sum() if 'Cost_Cents' in result_df.columns else 0
        logger.info("   Total Cost_Cents in DataFrame: %.4f¢", cost_sum_in_df)
        logger.info("   Dually verification costs calculated: %.4f¢", total_cost)
        logger.info("=" * 80)
    
    logger.info("=" * 80)
    logger.info("🔍 DUALLY VERIFICATION PHASE COMPLETE - MULTI-THREADED RESULTS")
    logger.info("=" * 80)
    logger.info("   🧵 Workers Used: %d", num_workers)
    logger.info("   📋 Total Checked: %d", total_dually)
    logger.info("   ✅ Confirmed: %d", verified_count)
    logger.info("   ❌ Removed (False Positives): %d", removed_count)
    logger.warning("   ⚠️ Errors: %d", error_count)
    logger.info("   💰 Dually Verification Cost: %.4f¢", total_cost)
    logger.info("   ⏱️ PERFORMANCE METRICS:")
    logger.info("      Total Time: %.2fs (%.2f minutes)", total_verification_elapsed, total_verification_elapsed / 60)
    logger.info("      Verification Loop Time: %.2fs", verification_loop_elapsed)
    logger.info("      Average Time per Listing: %.2fs", avg_verify_time)
    logger.info("      Verification Rate: %.1f listings/minute", results_collected / verification_loop_elapsed * 60)
    logger.info("      Speedup vs Sequential: ~%dx faster", num_workers)
    logger.info("=" * 80)
    
    # Update job status back to processing for final save
    if job_id in jobs:
        jobs[job_id]['status'] = JobStatus.PROCESSING
        jobs[job_id]['dually_verified'] = int(results_collected)
        jobs[job_id]['dually_removed'] = int(removed_count)
        jobs[job_id]['dually_verification_cost'] = float(total_cost)
    
    # FINAL STEP: Recalculate ALL statuses after verification with proper normalization
    logger.info("   🔄 STEP 5: Recalculating all statuses after verification...")
    
    # Load normalization rules
    from ai_tool.data_processing import load_rules, normalize_text
    rules = load_rules(config.rules_json)
    norm_map = rules.get('normalize_map', {})
    
    status_updated_count = 0
    for idx, row in result_df.iterrows():
        # Skip inactive ads, errors, and no-image cases
        current_status = str(row.get("Status", "")).strip()
        if any(x in current_status.lower() for x in ["inactive", "error", "image not clear", "no images present", "non-ctt platform"]):
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
    
    logger.info("   ✅ Status recalculation complete: %d status(es) corrected", status_updated_count)
    
    return result_df, total_cost  # Return both dataframe and verification cost


def run_job_pipeline_sync(job_id: str, file_path: str):
    """Main pipeline: Scraping -> Parallel AI Processing (runs synchronously)"""
    job = jobs[job_id]
    run_ts = now_ist().strftime("%Y-%m-%d_%H-%M-%S")
    
    try:
        # Load input file
        df = pd.read_excel(file_path, dtype={"Ad ID": str})
        df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # ✅ FIX: Deduplicate Ad IDs BEFORE processing to prevent exponential growth
        duplicates_before = df[df.duplicated(subset=["Ad ID"], keep=False)]
        if not duplicates_before.empty:
            logger.warning("   ⚠️ WARNING: Found %d duplicate rows in input Excel!", len(duplicates_before))
            logger.warning("   ⚠️ Duplicate Ad IDs: %s", duplicates_before['Ad ID'].unique().tolist())
            logger.info("   ✅ Removing duplicates, keeping first occurrence...")
            df = df.drop_duplicates(subset=["Ad ID"], keep='first').reset_index(drop=True)
            logger.info("   ✅ After deduplication: %d unique Ad IDs", len(df))
        
        # Add required columns
        for col in ["Breadcrumb_Top1", "Breadcrumb_Top2", "Breadcrumb_Top3", "Image_URLs"]:
            if col not in df.columns:
                df[col] = ""
        
        job['total_ads'] = int(len(df))  # Convert numpy.int64 to native int
        job['status'] = JobStatus.SCRAPING
        
        logger.info("=" * 60)
        logger.info("JOB %s STARTED", job_id)
        logger.info("File: %s", job.get('filename'))
        logger.info("Total Ads: %d", len(df))
        logger.info("=" * 60)
        
        # ── Ad Annotation Limit: Pre-filter (before scraping) ──
        if config.enable_ad_annotation_limit:
            over_limit = ad_tracker.filter_over_limit_ads(df["Ad ID"].tolist(), config.max_annotation_runs)
            if over_limit:
                logger.info("   🚫 Ad Tracker: Skipping %d ads (already annotated %d+ times)", len(over_limit), config.max_annotation_runs)
                logger.info("   🚫 Skipped Ad IDs: %s", str(sorted(over_limit)[:10]) + ('...' if len(over_limit) > 10 else ''))
                df = df[~df["Ad ID"].isin(over_limit)].reset_index(drop=True)
                job['total_ads'] = int(len(df))
                logger.info("   ✅ Ad Tracker: %d ads remaining after filter", len(df))
            else:
                logger.info("   ✅ Ad Tracker: All %d ads are within annotation limit", len(df))

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
        logger.info("🤖 Using %d parallel workers for faster processing", num_workers)
        result_df = run_parallel_ai(df, run_ts, job_id, num_workers)

        # Phase 3: Dually Verification - LLM double-check for false positives
        # Controlled by config.enable_dually_llm_verification flag
        dually_verification_cost = 0
        if not result_df.empty:
            if config.enable_dually_llm_verification:
                logger.info("=" * 60)
                logger.info("🔍 DUALLY LLM VERIFICATION: ✅ ENABLED")
                logger.info("   Starting post-processing verification for Dually annotations...")
                logger.info("=" * 60)
                # Create a new Yoda instance for verification
                m_verify = Manager()
                yoda_verify = Yoda(config.gemini_api_keys_info, config.rate_limit_rpm, m_verify)
                result_df, dually_verification_cost = verify_dually_listings(result_df, job_id, yoda_verify)
            else:
                logger.info("=" * 60)
                logger.info("🔍 DUALLY LLM VERIFICATION: ❌ DISABLED (Skipping)")
                logger.info("   Post-processing verification is turned OFF in config.ini")
                logger.info("   Set 'EnableDuallyLLMVerification = True' to enable")
                logger.info("=" * 60)
        
        # ── Ad Annotation Limit: Post-increment for successful ads ──
        if config.enable_ad_annotation_limit and not result_df.empty:
            if "Status" in result_df.columns:
                successful_ids = result_df[result_df["Status"] != "AI Error"]["Ad ID"].tolist()
            else:
                successful_ids = result_df["Ad ID"].tolist()
            if successful_ids:
                ad_tracker.increment_annotation_counts(successful_ids)
                logger.info("   📊 Ad Tracker: Updated annotation counts for %d successfully processed ads in Turso DB", len(successful_ids))

        # ✅ FIX: Deduplicate result DataFrame before saving to prevent duplicate entries
        if not result_df.empty:
            duplicates_final = result_df[result_df.duplicated(subset=["Ad ID"], keep=False)]
            if not duplicates_final.empty:
                logger.warning("   ⚠️ WARNING: Found %d duplicate rows in final result!", len(duplicates_final))
                logger.warning("   ⚠️ Duplicate Ad IDs: %s", duplicates_final['Ad ID'].unique().tolist())
                logger.info("   ✅ Removing duplicates, keeping first occurrence...")
                result_df = result_df.drop_duplicates(subset=["Ad ID"], keep='first').reset_index(drop=True)
                logger.info("   ✅ After deduplication: %d unique rows", len(result_df))

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
        job['b2_key'] = _upload_and_track(output_path, 'annotated', output_filename)

        # Calculate summary - Cost_Cents already includes dually verification costs (added in line 737)
        total_cost_in_df = result_df['Cost_Cents'].sum() if 'Cost_Cents' in result_df.columns else 0
        
        # For reporting: separate annotation cost (before dually) and dually cost
        annotation_cost_only = total_cost_in_df - dually_verification_cost  # Back-calculate annotation-only cost
        
        job['total_cost'] = float(total_cost_in_df)  # Total cost (includes dually costs already)
        job['annotation_cost'] = float(annotation_cost_only)  # Annotation cost without dually
        job['dually_verification_cost'] = float(dually_verification_cost)  # Separate dually cost for reporting
        
        # Merge session reports
        merge_all_session_reports(run_ts)
        
        logger.info("=" * 60)
        logger.info("🎉 JOB %s COMPLETE!", job_id)
        logger.info("Output: %s", output_filename)
        logger.info("💰 Annotation Cost: %s¢", annotation_cost_only)
        logger.info("💰 Dually Verification Cost: %s¢", dually_verification_cost)
        logger.info("💰 TOTAL COST: %s¢", total_cost_in_df)
        logger.info("=" * 60)

    except Exception as e:
        job['status'] = JobStatus.FAILED
        job['error'] = str(e)
        logger.error("❌ JOB %s FAILED: %s", job_id, str(e))
        import traceback
        traceback.print_exc()


async def run_job_pipeline(job_id: str, file_path: str):
    """Async wrapper for the pipeline"""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, run_job_pipeline_sync, job_id, file_path)


def run_reannotation_pipeline_sync(job_id: str, file_path: str):
    """Reannotation pipeline: Skip scraping, go directly to AI annotation"""
    job = jobs[job_id]
    run_ts = now_ist().strftime("%Y-%m-%d_%H-%M-%S")
    
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
        
        logger.info("=" * 60)
        logger.info("🔄 RE-ANNOTATION JOB %s STARTED", job_id)
        logger.info("File: %s", job.get('filename'))
        logger.info("Total Ads: %d", len(df))
        logger.info("Skipping scraping - using existing data")
        logger.info("=" * 60)
        
        # ── Ad Annotation Limit: Pre-filter (before AI) ──
        if config.enable_ad_annotation_limit:
            over_limit = ad_tracker.filter_over_limit_ads(df["Ad ID"].tolist(), config.max_annotation_runs)
            if over_limit:
                logger.info("   🚫 Ad Tracker: Skipping %d ads (already annotated %d+ times)", len(over_limit), config.max_annotation_runs)
                logger.info("   🚫 Skipped Ad IDs: %s", str(sorted(over_limit)[:10]) + ('...' if len(over_limit) > 10 else ''))
                df = df[~df["Ad ID"].isin(over_limit)].reset_index(drop=True)
                job['total_ads'] = int(len(df))
                logger.info("   ✅ Ad Tracker: %d ads remaining after filter", len(df))
            else:
                logger.info("   ✅ Ad Tracker: All %d ads are within annotation limit", len(df))

        # Phase 1: Parallel AI Processing (no scraping) - ULTRA-OPTIMIZED
        num_workers = min(10, max(1, len(config.gemini_api_keys)))
        logger.info("🤖 Using %d parallel workers for faster processing", num_workers)
        result_df = run_parallel_ai(df, run_ts, job_id, num_workers)

        # Phase 2: Dually Verification (if enabled)
        dually_verification_cost = 0
        if not result_df.empty:
            if config.enable_dually_llm_verification:
                logger.info("=" * 60)
                logger.info("🔍 DUALLY LLM VERIFICATION: ✅ ENABLED")
                logger.info("   Starting post-processing verification for Dually annotations...")
                logger.info("=" * 60)
                m_verify = Manager()
                yoda_verify = Yoda(config.gemini_api_keys_info, config.rate_limit_rpm, m_verify)
                result_df, dually_verification_cost = verify_dually_listings(result_df, job_id, yoda_verify)
            else:
                logger.info("=" * 60)
                logger.info("🔍 DUALLY LLM VERIFICATION: ❌ DISABLED (Skipping)")
                logger.info("=" * 60)

        # ── Ad Annotation Limit: Post-increment for successful ads ──
        if config.enable_ad_annotation_limit and not result_df.empty:
            if "Status" in result_df.columns:
                successful_ids = result_df[result_df["Status"] != "AI Error"]["Ad ID"].tolist()
            else:
                successful_ids = result_df["Ad ID"].tolist()
            if successful_ids:
                ad_tracker.increment_annotation_counts(successful_ids)
                logger.info("   📊 Ad Tracker: Updated annotation counts for %d successfully processed ads in Turso DB", len(successful_ids))
        
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
        job['b2_key'] = _upload_and_track(output_path, 'reannotated', output_filename)

        # Calculate summary
        # Calculate summary - Cost_Cents already includes dually verification costs (added in line 737)
        total_cost_in_df = result_df['Cost_Cents'].sum() if 'Cost_Cents' in result_df.columns else 0
        annotation_cost_only = total_cost_in_df - dually_verification_cost  # Back-calculate annotation-only cost
        
        job['total_cost'] = float(total_cost_in_df)  # Total cost (includes dually costs already)
        job['annotation_cost'] = float(annotation_cost_only)  # Annotation cost without dually
        job['dually_verification_cost'] = float(dually_verification_cost)  # Separate dually cost for reporting
        
        # Merge session reports
        merge_all_session_reports(run_ts)
        
        logger.info("=" * 60)
        logger.info("🎉 RE-ANNOTATION JOB %s COMPLETE!", job_id)
        logger.info("Output: %s", output_filename)
        logger.info("💰 Annotation Cost: %s¢", annotation_cost_only)
        logger.info("💰 Dually Verification Cost: %s¢", dually_verification_cost)
        logger.info("💰 TOTAL COST: %s¢", total_cost_in_df)
        logger.info("=" * 60)

    except Exception as e:
        job['status'] = JobStatus.FAILED
        job['error'] = str(e)
        logger.error("❌ RE-ANNOTATION JOB %s FAILED: %s", job_id, str(e))
        import traceback
        traceback.print_exc()


async def run_reannotation_pipeline(job_id: str, file_path: str):
    """Async wrapper for the reannotation pipeline"""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, run_reannotation_pipeline_sync, job_id, file_path)


def run_db_annotation_pipeline_sync(job_id: str, file_path: str, b2_folder_override: str = None):
    """
    AI Annotation pipeline for already-fetched database data — BATCH MODE (500 per batch)

    Processes in batches of 500 listings. Each batch runs the COMPLETE pipeline:
    1. AI annotation (promo check, refinement, classification)
    2. Dually verification (if enabled)
    3. Save batch Excel (downloadable immediately)

    After all batches: combines into one final complete file.
    Handles any size: 700→[500,200], 300→[300], 1500→[500,500,500]

    b2_folder_override: If set, batch files and final output upload to this B2 folder
                        instead of the default 'batches'/'db-annotated'. Used by the CDC
                        pipeline to route files directly to 'cdc/annotated'.
    """
    BATCH_SIZE = 500
    job = jobs[job_id]
    run_ts = now_ist().strftime("%Y-%m-%d_%H-%M-%S")
    
    try:
        logger.info("=" * 80)
        logger.info("🤖 AI ANNOTATION JOB %s STARTED — BATCH MODE (DB Fetched Data)", job_id)
        logger.info("=" * 80)
        logger.info("   Source File: %s", os.path.basename(file_path))
        logger.info("   Batch Size: %d", BATCH_SIZE)
        logger.info("=" * 80)
        
        # Load already-fetched data
        df = pd.read_excel(file_path, dtype={"Ad ID": str})
        df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

        # Fetch file fully loaded into DataFrame — delete local tmp copy (already on B2)
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info("   [CLEANUP] Deleted local fetch file: %s", os.path.basename(file_path))
            except OSError:
                pass

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
                logger.info("   🚫 Ad Tracker: Skipping %d ads (already annotated %d+ times)", len(over_limit), config.max_annotation_runs)
                logger.info("   🚫 Skipped Ad IDs: %s", str(sorted(over_limit)[:10]) + ('...' if len(over_limit) > 10 else ''))
                df = df[~df["Ad ID"].isin(over_limit)].reset_index(drop=True)
                logger.info("   ✅ Ad Tracker: %d ads remaining after filter", len(df))
            else:
                logger.info("   ✅ Ad Tracker: All %d ads are within annotation limit", len(df))

        total_listings = len(df)
        job['total_ads'] = int(total_listings)
        
        # Calculate batch plan
        num_batches = (total_listings + BATCH_SIZE - 1) // BATCH_SIZE  # ceil division
        job['batches'] = []
        job['total_batches'] = num_batches
        job['current_batch'] = 0
        
        logger.info("   ✅ Loaded %d ads from fetched data", total_listings)
        batch_ranges = " ".join(f"[{b * BATCH_SIZE + 1}-{min((b + 1) * BATCH_SIZE, total_listings)}]" for b in range(num_batches))
        logger.info("   📦 Will process in %d batch(es): %s", num_batches, batch_ranges)
        
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
            
            logger.info("#" * 80)
            logger.info("📦 BATCH %d/%d — Listings %d to %d (%d ads)", batch_num, num_batches, batch_start + 1, batch_end, batch_count)
            logger.info("#" * 80)
            
            # Use a unique run_ts per batch so session reports don't collide
            batch_run_ts = f"{run_ts}_batch{batch_num}"
            
            # --- PHASE A: AI ANNOTATION for this batch ---
            logger.info("=" * 80)
            logger.info("🤖 BATCH %d — PHASE A: AI ANNOTATION", batch_num)
            logger.info("=" * 80)
            logger.info("   Ads in this batch: %d", batch_count)
            logger.info("   Using %d parallel workers", num_workers)
            logger.info("=" * 80)
            
            batch_result_df = run_parallel_ai(batch_df, batch_run_ts, job_id, num_workers)
            
            # --- PHASE B: DUALLY VERIFICATION for this batch ---
            batch_dually_cost = 0
            if not batch_result_df.empty and config.enable_dually_llm_verification:
                logger.info("=" * 60)
                logger.info("🔍 BATCH %d — PHASE B: DUALLY LLM VERIFICATION", batch_num)
                logger.info("=" * 60)
                m_verify = Manager()
                yoda_verify = Yoda(config.gemini_api_keys_info, config.rate_limit_rpm, m_verify)
                batch_result_df, batch_dually_cost = verify_dually_listings(batch_result_df, job_id, yoda_verify)
            elif not batch_result_df.empty:
                logger.info("   🔍 BATCH %d — Dually verification: ❌ DISABLED (Skipping)", batch_num)
            
            # ── Ad Annotation Limit: Post-increment for successful ads in this batch ──
            if config.enable_ad_annotation_limit and not batch_result_df.empty:
                if "Status" in batch_result_df.columns:
                    successful_ids = batch_result_df[batch_result_df["Status"] != "AI Error"]["Ad ID"].tolist()
                else:
                    successful_ids = batch_result_df["Ad ID"].tolist()
                if successful_ids:
                    ad_tracker.increment_annotation_counts(successful_ids)
                    logger.info("   📊 Ad Tracker: Updated annotation counts for %d ads (batch %d) in Turso DB", len(successful_ids), batch_num)

            # --- PHASE C: SAVE BATCH OUTPUT (atomic write) ---
            logger.info("=" * 80)
            logger.info("💾 BATCH %d — PHASE C: SAVING BATCH OUTPUT", batch_num)
            logger.info("=" * 80)
            
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
                logger.warning("   ⚠️ Batch %d returned no results!", batch_num)
            batch_total_cost = batch_result_df['Cost_Cents'].sum() if 'Cost_Cents' in batch_result_df.columns else 0
            batch_annotation_cost = batch_total_cost - batch_dually_cost
            
            total_annotation_cost += batch_annotation_cost
            total_dually_cost += batch_dually_cost
            
            # Store batch metadata (thread-safe)
            # Upload batch to B2 (keep local — combine step reads it later)
            batch_b2_folder = b2_folder_override or 'batches'
            batch_b2_key = _upload_and_track(batch_path, batch_b2_folder, batch_filename, delete_local=False)

            batch_info = {
                'batch_index': batch_idx,
                'batch_num': batch_num,
                'start': batch_start + 1,
                'end': batch_end,
                'row_count': int(len(batch_result_df)),
                'filename': batch_filename,
                'file_path': batch_path,
                'b2_key': batch_b2_key,
                'status': 'completed',
                'annotation_cost': float(batch_annotation_cost),
                'dually_cost': float(batch_dually_cost),
                'total_cost': float(batch_total_cost)
            }
            with jobs_lock:
                job['batches'].append(batch_info)
            
            # No need to keep batch DataFrame in memory — read from disk when combining
            del batch_result_df
            
            logger.info("   ✅ Batch %d saved: %s", batch_num, batch_filename)
            logger.info("   📊 Rows: %d | Cost: %.2f¢", batch_info['row_count'], batch_total_cost)
            logger.info("   📥 Available for download immediately!")
            logger.info("=" * 80)
            
            # Merge session reports for this batch
            merge_all_session_reports(batch_run_ts)
        
        # ========== COMBINE ALL BATCHES INTO FINAL FILE ==========
        logger.info("=" * 80)
        logger.info("💾 COMBINING ALL %d BATCHES INTO FINAL OUTPUT", num_batches)
        logger.info("=" * 80)
        
        # Read from saved batch files instead of keeping in memory (memory-efficient)
        with jobs_lock:
            batch_files = [b['file_path'] for b in job.get('batches', [])]
        
        if batch_files:
            batch_dfs = [pd.read_excel(f, dtype={"Ad ID": str}) for f in batch_files]
            final_result_df = pd.concat(batch_dfs, ignore_index=True)
            del batch_dfs  # Free memory

            # Batch files combined into final output — delete local tmp copies (already on B2)
            for bp in batch_files:
                if os.path.exists(bp):
                    try:
                        os.remove(bp)
                    except OSError:
                        pass
            logger.info("   [CLEANUP] Deleted %d local batch file(s)", len(batch_files))

            # ✅ FIX: Deduplicate when combining batches to prevent duplicate entries
            if not final_result_df.empty:
                final_result_df["Ad ID"] = final_result_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                duplicates_combined = final_result_df[final_result_df.duplicated(subset=["Ad ID"], keep=False)]
                if not duplicates_combined.empty:
                    logger.warning("   ⚠️ WARNING: Found %d duplicate rows when combining batches!", len(duplicates_combined))
                    logger.warning("   ⚠️ Duplicate Ad IDs: %s", duplicates_combined['Ad ID'].unique().tolist())
                    logger.info("   ✅ Removing duplicates, keeping first occurrence...")
                    final_result_df = final_result_df.drop_duplicates(subset=["Ad ID"], keep='first').reset_index(drop=True)
                    logger.info("   ✅ After deduplication: %d unique rows", len(final_result_df))
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
        output_b2_folder = b2_folder_override or 'db-annotated'
        job['b2_key'] = _upload_and_track(output_path, output_b2_folder, output_filename)
        job['total_cost'] = float(total_cost)
        job['annotation_cost'] = float(total_annotation_cost)
        job['dually_verification_cost'] = float(total_dually_cost)

        logger.info("   ✅ Final combined file saved: %s", output_filename)
        logger.info("   📊 Total rows: %d", len(final_result_df))
        logger.info("   📁 Path: %s", output_path)
        logger.info("=" * 80)

        logger.info("=" * 80)
        logger.info("🎉 AI ANNOTATION JOB %s COMPLETE! (%d batches)", job_id, num_batches)
        logger.info("=" * 80)
        logger.info("   🤖 Total Ads Annotated: %d", len(final_result_df))
        logger.info("   📦 Batches Completed: %d", num_batches)
        logger.info("   📄 Final Output File: %s", output_filename)
        logger.info("   💰 Annotation Cost: %.2f¢", total_annotation_cost)
        logger.info("   💰 Dually Verification Cost: %.2f¢", total_dually_cost)
        logger.info("   💰 TOTAL COST: %.2f¢", total_cost)
        logger.info("=" * 80)

    except Exception as e:
        job['status'] = JobStatus.FAILED
        job['error'] = str(e)
        job['partial_completion'] = True
        job['completed_batches'] = len(job.get('batches', []))
        # Already-completed batches remain in job['batches'] and are still downloadable
        logger.error("❌ AI ANNOTATION JOB %s FAILED: %s", job_id, str(e))
        if job.get('batches'):
            logger.info("   📦 %d batch(es) completed before failure and are still downloadable.", len(job['batches']))
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
    run_ts = now_ist().strftime("%Y-%m-%d_%H-%M-%S")
    
    try:
        logger.info("=" * 80)
        logger.info("🗄️ DB FETCH + AI ANNOTATION JOB %s STARTED (PRODUCTION)", job_id)
        logger.info("=" * 80)
        logger.info("   Date Range: %s → %s", min_last_update, max_last_update)
        logger.info("   Listing Range: %s → %s", listing_start, listing_end)
        logger.info("=" * 80)

        # ========== PHASE 1: FETCH DATA FROM DATABASE API (PRODUCTION) ==========
        logger.info("=" * 80)
        logger.info("🗄️ PHASE 1: FETCHING DATA FROM DATABASE API (PRODUCTION)")
        logger.info("=" * 80)

        # Step 1: Get access token
        logger.info("   🔑 Using credentials from: %s", 'Request' if client_id != config.db_api_client_id else 'config.ini')
        logger.info("   🔑 Client ID: %s...", client_id[:10])
        logger.info("   🔑 Grant Type: %s", grant_type)
        
        token_data = get_access_token(client_id, client_secret, grant_type)
        access_token = token_data['access_token']
        
        # Step 2: Calculate how many trucks to fetch
        total_listings_needed = listing_end - listing_start
        logger.info("   📊 Need to fetch %d listings", total_listings_needed)
        logger.info("   📦 Will use pagination with max 500 per request")

        # Step 3: Fetch trucks with pagination
        all_trucks = []
        current_offset = listing_start
        limit_per_request = 500

        logger.info("=" * 80)
        logger.info("📦 FETCHING TRUCKS DATA WITH PAGINATION")
        logger.info("=" * 80)
        
        while len(all_trucks) < total_listings_needed:
            remaining = total_listings_needed - len(all_trucks)
            current_limit = min(limit_per_request, remaining)
            
            logger.info("   🔄 Request %d: Offset=%d, Limit=%d", len(all_trucks) // 500 + 1, current_offset, current_limit)
            
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
                logger.warning("   ⚠️ No more trucks available")
                break

            # Debug: Check for duplicates before extending
            existing_ids = set(truck.get('id') for truck in all_trucks)
            new_ids = [truck.get('id') for truck in batch_trucks]
            duplicate_count = sum(1 for tid in new_ids if tid in existing_ids)

            if duplicate_count > 0:
                logger.warning("   ⚠️ WARNING: %d duplicate Ad IDs detected in this batch!", duplicate_count)

            all_trucks.extend(batch_trucks)
            current_offset += len(batch_trucks)

            logger.info("   ✅ Batch complete. Total fetched so far: %d", len(all_trucks))
            logger.info("   📊 API reports total available: %d", total_available)
            logger.info("   📊 Unique Ad IDs so far: %d", len(set(truck.get('id') for truck in all_trucks)))

            if current_offset >= total_available:
                logger.info("   ℹ️  Reached end of available data (total: %d)", total_available)
                break

        logger.info("=" * 80)
        logger.info("✅ FETCHING COMPLETE - Retrieved %d trucks", len(all_trucks))
        logger.info("=" * 80)

        # Step 4: Process truck data into DataFrame
        logger.info("=" * 80)
        logger.info("🔄 PROCESSING TRUCK DATA INTO DATAFRAME")
        logger.info("=" * 80)

        logger.info("   📊 Input: %d trucks from API", len(all_trucks))
        logger.info("   📊 Unique IDs in raw data: %d", len(set(truck.get('id') for truck in all_trucks)))

        processed_trucks = []
        for i, truck in enumerate(all_trucks, 1):
            processed = process_truck_data(truck)
            processed_trucks.append(processed)

            if i % 100 == 0:
                logger.info("   ✅ Processed %d/%d trucks", i, len(all_trucks))

        logger.info("   ✅ Processed all %d trucks", len(processed_trucks))
        logger.info("=" * 80)

        # Create DataFrame
        df = pd.DataFrame(processed_trucks)

        logger.info("   📊 Before deduplication: %d rows", len(df))

        df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

        # Check for duplicates after processing
        duplicates = df[df.duplicated(subset=['Ad ID'], keep=False)]
        if not duplicates.empty:
            logger.warning("   ⚠️ Found %d duplicate rows!", len(duplicates))
            logger.warning("   ⚠️ Duplicate Ad IDs: %s", duplicates['Ad ID'].unique().tolist())
            # Remove duplicates, keeping first occurrence
            df = df.drop_duplicates(subset=['Ad ID'], keep='first')
            logger.info("   ✅ After deduplication: %d rows", len(df))
        else:
            logger.info("   ✅ No duplicates found")
        
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

        # Upload to B2 (keep local — annotation pipeline reads it next)
        _upload_and_track(db_fetch_path, 'db-fetch', db_fetch_filename, delete_local=False)

        logger.info("   ✅ Intermediate DB Fetch file saved: %s", db_fetch_filename)
        logger.info("   📁 Path: %s", db_fetch_path)

        job['total_ads'] = int(len(df))
        job['status'] = JobStatus.PROCESSING

        # ── Ad Annotation Limit: Pre-filter (before AI annotation) ──
        if config.enable_ad_annotation_limit:
            over_limit = ad_tracker.filter_over_limit_ads(df["Ad ID"].tolist(), config.max_annotation_runs)
            if over_limit:
                logger.info("   🚫 Ad Tracker: Skipping %d ads (already annotated %d+ times)", len(over_limit), config.max_annotation_runs)
                logger.info("   🚫 Skipped Ad IDs: %s", str(sorted(over_limit)[:10]) + ('...' if len(over_limit) > 10 else ''))
                df = df[~df["Ad ID"].isin(over_limit)].reset_index(drop=True)
                job['total_ads'] = int(len(df))
                logger.info("   ✅ Ad Tracker: %d ads remaining after filter", len(df))
            else:
                logger.info("   ✅ Ad Tracker: All %d ads are within annotation limit", len(df))

        # ========== PHASE 2: AI ANNOTATION ==========
        logger.info("=" * 80)
        logger.info("🤖 PHASE 2: AI ANNOTATION (Same as existing feature)")
        logger.info("=" * 80)
        logger.info("   Total Ads to Annotate: %d", len(df))
        logger.info("=" * 80)

        # Run parallel AI annotation (same as existing feature)
        num_workers = 5
        logger.info("🤖 Using %d parallel workers for AI annotation", num_workers)
        result_df = run_parallel_ai(df, run_ts, job_id, num_workers)

        # ========== PHASE 3: DUALLY VERIFICATION ==========
        dually_verification_cost = 0
        if not result_df.empty:
            if config.enable_dually_llm_verification:
                logger.info("=" * 60)
                logger.info("🔍 DUALLY LLM VERIFICATION: ✅ ENABLED")
                logger.info("   Starting post-processing verification for Dually annotations...")
                logger.info("=" * 60)
                m_verify = Manager()
                yoda_verify = Yoda(config.gemini_api_keys_info, config.rate_limit_rpm, m_verify)
                result_df, dually_verification_cost = verify_dually_listings(result_df, job_id, yoda_verify)
            else:
                logger.info("=" * 60)
                logger.info("🔍 DUALLY LLM VERIFICATION: ❌ DISABLED (Skipping)")
                logger.info("=" * 60)

        # ── Ad Annotation Limit: Post-increment for successful ads ──
        if config.enable_ad_annotation_limit and not result_df.empty:
            if "Status" in result_df.columns:
                successful_ids = result_df[result_df["Status"] != "AI Error"]["Ad ID"].tolist()
            else:
                successful_ids = result_df["Ad ID"].tolist()
            if successful_ids:
                ad_tracker.increment_annotation_counts(successful_ids)
                logger.info("   📊 Ad Tracker: Updated annotation counts for %d successfully processed ads in Turso DB", len(successful_ids))

        # ========== PHASE 4: SAVE FINAL OUTPUT ==========
        logger.info("=" * 80)
        logger.info("💾 PHASE 4: SAVING FINAL ANNOTATED OUTPUT")
        logger.info("=" * 80)

        output_filename = f"output_db_annotated_{run_ts}.xlsx"
        if getattr(config, 'enable_ai_output', True):
            os.makedirs(config.output_dir, exist_ok=True)
            output_path = os.path.join(config.output_dir, output_filename)
        else:
            output_path = _temp_path(output_filename)
            _register_temp_file(output_path)
        result_df.to_excel(output_path, index=False)

        logger.info("   ✅ Final annotated file saved: %s", output_filename)
        logger.info("   📁 Path: %s", output_path)
        logger.info("=" * 80)

        job['status'] = JobStatus.COMPLETED
        job['output_file'] = output_path
        job['output_filename'] = output_filename
        job['b2_key'] = _upload_and_track(output_path, 'db-annotated', output_filename)

        # Calculate summary costs - Cost_Cents already includes dually verification costs (added in line 737)
        total_cost_in_df = result_df['Cost_Cents'].sum() if 'Cost_Cents' in result_df.columns else 0
        annotation_cost_only = total_cost_in_df - dually_verification_cost  # Back-calculate annotation-only cost
        
        job['total_cost'] = float(total_cost_in_df)  # Total cost (includes dually costs already)
        job['annotation_cost'] = float(annotation_cost_only)  # Annotation cost without dually
        job['dually_verification_cost'] = float(dually_verification_cost)  # Separate dually cost for reporting
        
        # Merge session reports
        merge_all_session_reports(run_ts)
        
        logger.info("=" * 80)
        logger.info("🎉 DB FETCH + AI ANNOTATION JOB %s COMPLETE!", job_id)
        logger.info("=" * 80)
        logger.info("   📊 Total Trucks Fetched: %d", len(df))
        logger.info("   🤖 Total Ads Annotated: %d", len(result_df))
        logger.info("   📄 Output File: %s", output_filename)
        logger.info("   💰 Annotation Cost: %s¢", annotation_cost_only)
        logger.info("   💰 Dually Verification Cost: %s¢", dually_verification_cost)
        logger.info("   💰 TOTAL COST: %s¢", total_cost_in_df)
        logger.info("=" * 80)

    except Exception as e:
        job['status'] = JobStatus.FAILED
        job['error'] = str(e)
        logger.error("❌ DB FETCH + AI ANNOTATION JOB %s FAILED: %s", job_id, str(e))
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

    # Archive upload to B2 (keep local — pipeline needs it)
    _upload_and_track(file_path, 'uploads', upload_filename, delete_local=False)

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

    # Archive upload to B2 (keep local — pipeline needs it)
    _upload_and_track(file_path, 'uploads', upload_filename, delete_local=False)

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

    # Try B2 pre-signed URL redirect first
    b2_key = job.get('b2_key')
    if b2_key and b2_is_enabled():
        url = get_download_url(b2_key)
        if url:
            return RedirectResponse(url=url, status_code=302)

    # Fallback to local file
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

    # Try B2 pre-signed URL redirect first
    b2_key = batch.get('b2_key')
    if b2_key and b2_is_enabled():
        url = get_download_url(b2_key)
        if url:
            return RedirectResponse(url=url, status_code=302)

    # Fallback to local file
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
        logger.info("📋 [Rules.json] Loading for AUDIT comparison...")
        rules = load_rules(config.rules_json)
        norm_map = rules['normalize_map']
        logger.info("📋 [Rules.json] AUDIT using normalize_map with %d entries", len(norm_map))
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
            elif "non-ctt platform" in ai_status:
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
    
    # Identify Inactive and Non-CTT Rows
    is_inactive = final_output['Status'].astype(str).str.contains('inactive|non-ctt platform', case=False, na=False) if 'Status' in final_output.columns else pd.Series([False] * total)
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
    timestamp = now_ist().strftime("%Y-%m-%d_%H-%M-%S")
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
        
        logger.info("✅ Audit Complete!")
        logger.info("   Global Accuracy: %.2f%%", global_acc_pct)
        logger.info("   Active Accuracy: %.2f%%", active_acc_pct)
        logger.info("   Report Saved: %s", report_filename)
        
    except Exception as e:
        return {"error": f"Error saving audit report: {str(e)}"}
    
    b2_key = _upload_and_track(report_path, 'audit-reports', report_filename)

    return {
        "audit_id": audit_id,
        "report_path": report_path,
        "report_filename": report_filename,
        "b2_key": b2_key,
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
            "b2_key": result.get("b2_key"),
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

    # Try B2 pre-signed URL redirect first
    b2_key = audit.get('b2_key')
    if b2_key and b2_is_enabled():
        url = get_download_url(b2_key)
        if url:
            return RedirectResponse(url=url, status_code=302)

    # Fallback to local file
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
    logger.info("=" * 80)
    logger.info("🔑 FETCHING ACCESS TOKEN FROM DB API (PRODUCTION)")
    logger.info("=" * 80)

    # Token URL from config.ini [DB_API] section
    token_url = config.db_api_token_url
    
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
        logger.info("   📤 POST %s", token_url)
        logger.info("   📝 Form Data: client_id=%s, grant_type=%s", client_id, grant_type)
        logger.info("   📝 Client Secret: %d chars", len(client_secret))
        logger.info("   📋 Headers: %s", headers)

        response = requests.post(token_url, data=form_data, headers=headers)

        # Print detailed error information if request fails
        if not response.ok:
            logger.error("   ❌ HTTP %d: %s", response.status_code, response.reason)
            logger.error("   📋 Response Headers: %s", dict(response.headers))
            try:
                error_detail = response.json()
                logger.error("   📋 Error JSON: %s", error_detail)
            except:
                logger.error("   📋 Response Text: %s", response.text)

        response.raise_for_status()

        token_data = response.json()
        logger.info("   ✅ Access token received successfully")
        logger.info("   ⏱️  Expires in: %s seconds", token_data['expires_in'])
        logger.info("=" * 80)

        return token_data
    except requests.exceptions.HTTPError as e:
        logger.error("   ❌ HTTP Error: %s", str(e))
        logger.info("=" * 80)
        raise
    except Exception as e:
        logger.error("   ❌ Error fetching access token: %s", str(e))
        logger.info("=" * 80)
        raise


def fetch_trucks_from_db(access_token: str, min_last_update: int, max_last_update: int, limit: int = 500, offset: int = 0) -> dict:
    """
    Fetch truck data from the DB API with pagination (PRODUCTION)
    """
    # Trucks URL from config.ini [DB_API] section
    trucks_url = config.db_api_trucks_url
    
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
        logger.info("   📤 GET %s", trucks_url)
        logger.info("   📝 Params: minLastUpdate=%s, maxLastUpdate=%s, limit=%d, offset=%d", min_last_update, max_last_update, limit, offset)

        response = requests.get(trucks_url, params=params, headers=headers)
        response.raise_for_status()

        data = response.json()
        total = data.get('pagination', {}).get('total', 0)
        returned = len(data.get('result', []))

        logger.info("   ✅ Fetched %d trucks (Total available: %d)", returned, total)

        return data
    except Exception as e:
        logger.error("   ❌ Error fetching trucks: %s", str(e))
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
        logger.debug("   🔍 DEBUG - Processing truck %s:", ad_id)
        logger.debug("      Photos array exists: %s", photos is not None)
        logger.debug("      Photos count: %d", len(photos) if photos else 0)
        if photos:
            logger.debug("      First photo data: %s", photos[0])
    
    if photos:
        cdn_urls = []
        for photo in photos:
            # Extract URL directly from the photo object (API provides full CDN URL)
            photo_url = photo.get('url', '')
            if photo_url:
                cdn_urls.append(photo_url)
            elif debug:
                logger.debug("      ⚠️ Photo missing url field: %s", photo)

        processed['Image_URLs'] = ','.join(cdn_urls)

        if debug:
            urls_preview = processed['Image_URLs'][:100] + '...' if len(processed['Image_URLs']) > 100 else processed['Image_URLs']
            logger.debug("      Final Image_URLs: %s", urls_preview)
    elif debug:
        logger.debug("      ⚠️ No photos array in truck data")
    
    return processed


def filter_ctt_platform_trucks(fetched_trucks: list) -> tuple:
    """
    Filter trucks to only include those present on the CTT platform.

    Checks the 'adFeatures' field (comma-separated string) for the CTT feature ID.
    If EnableCTTPlatformFilter is False, all trucks pass through unchanged.

    Args:
        fetched_trucks: List of raw truck dicts from DB API

    Returns:
        tuple: (ctt_trucks, non_ctt_ids)
            - ctt_trucks: List of truck dicts that ARE on CTT
            - non_ctt_ids: List of ad IDs that are NOT on CTT
    """
    if not config.enable_ctt_platform_filter:
        return fetched_trucks, []

    ctt_feature_id = config.ctt_feature_id
    ctt_trucks = []
    non_ctt_ids = []

    for truck in fetched_trucks:
        ad_features = str(truck.get('adFeatures', '') or '')
        feature_list = [f.strip() for f in ad_features.split(',') if f.strip()]

        if ctt_feature_id in feature_list:
            ctt_trucks.append(truck)
        else:
            ad_id = truck.get('id', 'unknown')
            non_ctt_ids.append(str(ad_id))

    logger.info("   %s", "=" * 60)
    logger.info("   CTT PLATFORM FILTER")
    logger.info("   %s", "=" * 60)
    logger.info("   Feature ID checked: %s", ctt_feature_id)
    logger.info("   Total trucks checked: %d", len(fetched_trucks))
    logger.info("   CTT platform (pass): %d", len(ctt_trucks))
    logger.info("   Non-CTT platform (filtered out): %d", len(non_ctt_ids))
    if non_ctt_ids:
        if len(non_ctt_ids) <= 20:
            logger.info("   Filtered Ad IDs: %s", non_ctt_ids)
        else:
            logger.info("   Filtered Ad IDs (first 20): %s...", non_ctt_ids[:20])
    logger.info("   %s", "=" * 60)

    return ctt_trucks, non_ctt_ids


def fetch_single_truck_by_id(access_token: str, ad_id: str,
                             base_url: str = None) -> dict:
    """
    Fetch a single truck by Ad ID from the DB API.
    
    Args:
        access_token: Bearer token for authentication
        ad_id: The truck Ad ID to fetch
        base_url: Base URL for trucks endpoint (e.g. https://host/vLatest/trucks)
        
    Returns:
        dict: Truck data from API
    """
    if base_url is None:
        base_url = config.db_api_base_url
    truck_url = f"{base_url.rstrip('/')}/{ad_id}"

    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {'bypassCache': 'true'}

    try:
        response = requests.get(truck_url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        # The API returns data in format: {"url": "...", "result": {...}}
        # We need the "result" object which contains the truck data
        return data.get('result', {})
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning("   ⚠️ Truck %s not found (404)", ad_id)
            return None
        else:
            logger.error("   ❌ Error fetching truck %s: HTTP %s", ad_id, e.response.status_code)
            raise
    except Exception as e:
        logger.error("   ❌ Error fetching truck %s: %s", ad_id, str(e))
        raise


def _fetch_single_truck_worker(ad_id: str, access_token: str, index: int, total: int,
                               base_url: str = None):
    """
    Worker function to fetch a single truck by Ad ID (for multithreading)

    Args:
        ad_id: The truck Ad ID to fetch
        access_token: Bearer token for authentication
        index: Current index (for progress display)
        total: Total number of trucks to fetch
        base_url: Base URL for trucks endpoint

    Returns:
        tuple: (status, ad_id, truck_data_or_error)
            status: 'success', 'not_found', or 'error'
            ad_id: The Ad ID that was fetched
            truck_data_or_error: Truck data dict if success, None if not found, error message if error
    """
    try:
        logger.info("   🔄 [%d/%d] Fetching truck %s...", index, total, ad_id)
        truck_data = fetch_single_truck_by_id(access_token, ad_id, base_url=base_url)

        if truck_data:
            logger.info("   ✅ [%d/%d] Truck %s: OK", index, total, ad_id)
            return ('success', ad_id, truck_data)
        else:
            logger.info("   [%d/%d] Truck %s: Not Found", index, total, ad_id)
            return ('not_found', ad_id, None)

    except Exception as e:
        error_msg = str(e)[:50]
        logger.error("   [%d/%d] Truck %s: Error: %s", index, total, ad_id, error_msg)
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
    run_ts = now_ist().strftime("%Y-%m-%d_%H-%M-%S")
    
    try:
        logger.info("=" * 80)
        logger.info("🗄️ DB FETCH BY AD IDs JOB %s STARTED (PRODUCTION)", job_id)
        logger.info("   Source File: %s", os.path.basename(file_path))
        logger.info("=" * 80)

        # ========== STEP 1: Load Ad IDs from Excel ==========
        logger.info("=" * 80)
        logger.info("📄 STEP 1: LOADING AD IDs FROM EXCEL")
        logger.info("=" * 80)

        df = pd.read_excel(file_path, dtype={"Ad ID": str})

        # Standardize Ad ID column
        if "Ad ID" not in df.columns:
            raise ValueError("Excel file must have an 'Ad ID' column")

        df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

        # Upload file fully loaded into DataFrame — delete local tmp copy (already on B2)
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info("   [CLEANUP] Deleted local upload file: %s", os.path.basename(file_path))
            except OSError:
                pass

        # ✅ FIX: Deduplicate Ad IDs BEFORE processing to prevent exponential growth
        duplicates_before = df[df.duplicated(subset=["Ad ID"], keep=False)]
        if not duplicates_before.empty:
            logger.warning("   ⚠️ WARNING: Found %d duplicate rows in input Excel!", len(duplicates_before))
            logger.warning("   ⚠️ Duplicate Ad IDs: %s", duplicates_before['Ad ID'].unique().tolist())
            logger.info("   ✅ Removing duplicates, keeping first occurrence...")
            df = df.drop_duplicates(subset=["Ad ID"], keep='first').reset_index(drop=True)
            logger.info("   ✅ After deduplication: %d unique Ad IDs", len(df))

        ad_ids = df["Ad ID"].tolist()

        logger.info("   ✅ Loaded %d unique Ad IDs from Excel", len(ad_ids))
        logger.info("   📊 First 5 Ad IDs: %s", ad_ids[:5])
        logger.info("=" * 80)

        # ── Ad Annotation Limit: Pre-filter (BEFORE DB API calls — saves API bandwidth) ──
        if config.enable_ad_annotation_limit:
            over_limit = ad_tracker.filter_over_limit_ads(ad_ids, config.max_annotation_runs)
            if over_limit:
                skipped_preview = sorted(over_limit)[:10]
                ellipsis = '...' if len(over_limit) > 10 else ''
                logger.info("   🚫 Ad Tracker: Skipping %d ads (already annotated %d+ times)", len(over_limit), config.max_annotation_runs)
                logger.info("   🚫 Skipped Ad IDs: %s%s", skipped_preview, ellipsis)
                df = df[~df["Ad ID"].isin(over_limit)].reset_index(drop=True)
                ad_ids = df["Ad ID"].tolist()
                logger.info("   ✅ Ad Tracker: %d ads remaining — only these will be fetched from DB API", len(ad_ids))
            else:
                logger.info("   ✅ Ad Tracker: All %d ads are within annotation limit", len(ad_ids))
        
        job['total_ads'] = int(len(ad_ids))
        job['status'] = JobStatus.PROCESSING
        
        # ========== STEP 2: Get Access Token ==========
        logger.info("=" * 80)
        logger.info("🔑 STEP 2: AUTHENTICATING WITH DB API")
        logger.info("=" * 80)

        token_data = get_access_token(client_id, client_secret, grant_type)
        access_token = token_data['access_token']
        logger.info("=" * 80)

        # ========== STEP 3: Fetch Trucks by Ad ID (MULTITHREADED) ==========
        logger.info("=" * 80)
        logger.info("📦 STEP 3: FETCHING %d TRUCKS FROM DB API (MULTITHREADED)", len(ad_ids))
        logger.info("=" * 80)
        logger.info("   Using endpoint: %s/{ad_id}", config.db_api_base_url)
        logger.info("   🚀 Using 5 concurrent workers for SUPER FAST fetching!")
        
        fetched_trucks = []
        not_found_ids = []
        error_ids = []
        
        # Use ThreadPoolExecutor with 5 workers for concurrent fetching
        max_workers = 5
        logger.info("⚡ Starting %d concurrent fetch workers...", max_workers)
        
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
        
        logger.info("=" * 80)
        logger.info("✅ FETCHING COMPLETE")
        logger.info("   Successfully fetched: %d/%d trucks", len(fetched_trucks), len(ad_ids))
        if not_found_ids:
            logger.warning("   ⚠️ Not found: %d trucks - %s", len(not_found_ids), not_found_ids[:10])
        if error_ids:
            logger.error("   ❌ Errors: %d trucks - %s", len(error_ids), error_ids[:10])
        logger.info("=" * 80)
        
        # Apply CTT Platform Filter
        fetched_trucks, non_ctt_ids = filter_ctt_platform_trucks(fetched_trucks)

        if len(fetched_trucks) == 0 and len(non_ctt_ids) == 0:
            raise ValueError("No trucks were successfully fetched from the database")

        # ========== STEP 4: Process Truck Data ==========
        logger.info("=" * 80)
        logger.info("🔄 STEP 4: PROCESSING TRUCK DATA")
        logger.info("=" * 80)

        processed_trucks = []
        for i, truck in enumerate(fetched_trucks, 1):
            debug = (i == 1)  # Debug first truck only
            processed = process_truck_data(truck, debug=debug)
            processed_trucks.append(processed)

            if i % 50 == 0:
                logger.info("   ✅ Processed %d/%d trucks", i, len(fetched_trucks))

        # Add not_found and error trucks with "Inactive ad" status
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

        # Add non-CTT platform trucks
        for ad_id in non_ctt_ids:
            processed_trucks.append({
                'Ad ID': ad_id,
                'Breadcrumb_Top1': 'Non-CTT Platform',
                'Breadcrumb_Top2': '',
                'Breadcrumb_Top3': '',
                'Image_URLs': ''
            })

        logger.info("   ✅ Processed %d successful trucks", len(fetched_trucks))
        if not_found_ids:
            logger.warning("   ⚠️ Added %d inactive (not found) trucks", len(not_found_ids))
        if error_ids:
            logger.warning("   ⚠️ Added %d inactive (error) trucks", len(error_ids))
        if non_ctt_ids:
            logger.info("   ℹ️  Added %d Non-CTT Platform entries", len(non_ctt_ids))
        logger.info("   ✅ Total processed: %d trucks", len(processed_trucks))
        logger.info("=" * 80)
        
        # ========== STEP 5: Save to Excel ==========
        logger.info("=" * 80)
        logger.info("💾 STEP 5: SAVING TO EXCEL")
        logger.info("=" * 80)
        
        result_df = pd.DataFrame(processed_trucks)
        result_df["Ad ID"] = result_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # ✅ FIX: Deduplicate result DataFrame to prevent duplicate entries
        duplicates_after = result_df[result_df.duplicated(subset=["Ad ID"], keep=False)]
        if not duplicates_after.empty:
            logger.warning("   ⚠️ WARNING: Found %d duplicate rows in processed data!", len(duplicates_after))
            logger.warning("   ⚠️ Duplicate Ad IDs: %s", duplicates_after['Ad ID'].unique().tolist())
            logger.info("   ✅ Removing duplicates, keeping first occurrence...")
            result_df = result_df.drop_duplicates(subset=["Ad ID"], keep='first').reset_index(drop=True)
            logger.info("   ✅ After deduplication: %d unique rows", len(result_df))
        
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

        # Upload to B2 (keep local — annotation pipeline may read it)
        b2_key = _upload_and_track(db_fetch_path, 'db-fetch', db_fetch_filename, delete_local=False)

        logger.info("   ✅ Excel file saved: %s", db_fetch_filename)
        logger.info("   📁 Path: %s", db_fetch_path)
        logger.info("=" * 80)

        # Update job status
        job['status'] = 'fetched'
        job['file_path'] = db_fetch_path
        job['output_file'] = db_fetch_path
        job['output_filename'] = db_fetch_filename
        job['b2_key'] = b2_key
        job['total_ads'] = int(len(result_df))
        job['preview_data'] = processed_trucks[:10]
        job['is_db_fetch_by_ids'] = True
        
        logger.info("=" * 80)
        logger.info("🎉 DB FETCH BY AD IDs COMPLETE!")
        logger.info("   📊 Successfully fetched: %d trucks", len(fetched_trucks))
        logger.info("   📄 Excel file: %s", db_fetch_filename)
        logger.info("   ✅ Ready for annotation!")
        logger.info("   📊 Preview available: First %d trucks", len(processed_trucks[:10]))
        logger.info("=" * 80)

    except Exception as e:
        job['status'] = JobStatus.FAILED
        job['error'] = str(e)
        logger.error("❌ DB FETCH BY AD IDs FAILED: %s", str(e))
        import traceback
        traceback.print_exc()


# ==================== CDC PIPELINE TRIGGER ====================

CDC_OUTPUT_DIR = os.path.join(str(PROJECT_ROOT), "cdc_ai_output_excels")


def _cdc_get_access_token(base_url: str, client_id: str, client_secret: str, grant_type: str) -> dict:
    """Get access token from the CDC dev DB API."""
    import requests as _requests
    token_url = f"{base_url}/token"
    logger.info("   POST %s", token_url)
    resp = _requests.post(token_url, data={
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': grant_type,
    }, headers={'Content-Type': 'application/x-www-form-urlencoded'})
    resp.raise_for_status()
    token_data = resp.json()
    logger.info("   Access token received successfully")
    return token_data


def _cdc_fetch_single_truck(base_url: str, access_token: str, ad_id: str) -> dict | None:
    """Fetch a single truck from the CDC dev DB API."""
    import requests as _requests
    truck_url = f"{base_url}/trucks/{ad_id}?bypassCache=true"
    resp = _requests.get(truck_url, headers={'Authorization': f'Bearer {access_token}'})
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    data = resp.json()
    return data.get('result', {})


def _cdc_fetch_worker(base_url: str, ad_id: str, access_token: str, index: int, total: int):
    """Worker for concurrent CDC truck fetching."""
    try:
        logger.info("   [%d/%d] Fetching truck %s...", index, total, ad_id)
        truck_data = _cdc_fetch_single_truck(base_url, access_token, ad_id)
        if truck_data:
            logger.info("   [%d/%d] Truck %s: OK", index, total, ad_id)
            return ('success', ad_id, truck_data)
        else:
            logger.info("   [%d/%d] Truck %s: Not Found", index, total, ad_id)
            return ('not_found', ad_id, None)
    except Exception as e:
        logger.error("   [%d/%d] Truck %s: Error: %s", index, total, ad_id, str(e)[:50])
        return ('error', ad_id, str(e))


def _cdc_dev_db_update(db_api_base_url: str, client_id: str, client_secret: str,
                       grant_type: str, output_excel_path: str, job_id: str) -> dict:
    """
    Dev DB Update: Reads the annotated CDC output Excel and PUTs category updates
    to the dev DB API for ads with Status == "Require Update".

    Simplified version of the prod /api/db-update — no patch workflow, direct PUT only.
    Uses dev credentials (same as CDC fetch).

    Returns a summary dict with counts and per-ad results.
    """
    logger.info("=" * 80)
    logger.info("🔄 CDC DEV DB UPDATE — Job %s", job_id)
    logger.info("   Excel: %s", os.path.basename(output_excel_path))
    logger.info("   Dev API: %s", db_api_base_url)
    logger.info("=" * 80)

    # 1. Read the annotated output Excel
    df = pd.read_excel(output_excel_path, dtype={"Ad ID": str})
    df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    total_rows = len(df)

    if "Status" not in df.columns or "Annotated_Top1" not in df.columns:
        logger.warning("   ⚠️ Missing required columns (Status, Annotated_Top1) — skipping db update")
        return {"total_rows": total_rows, "update_count": 0, "success_count": 0,
                "failed_count": 0, "skipped_count": total_rows, "results": []}

    # 2. Filter rows — only "Require Update"
    prepared_ads = []
    skipped_count = 0

    for _, row in df.iterrows():
        ad_id = str(row.get("Ad ID", "")).strip()
        status = str(row.get("Status", "")).strip()
        status_lower = status.lower()

        if not ad_id or ad_id.lower() == "nan":
            skipped_count += 1
            continue

        # Skip known non-update statuses
        should_skip = False
        for skip_status in DB_UPDATE_SKIP_STATUSES:
            if skip_status in status_lower:
                should_skip = True
                break
        if should_skip:
            skipped_count += 1
            continue

        # Only process "Require Update"
        if status_lower != "require update":
            skipped_count += 1
            continue

        # 3. Resolve categories from Annotated_Top1/2/3
        categories = []
        unmapped = []
        for col in ["Annotated_Top1", "Annotated_Top2", "Annotated_Top3"]:
            cat_name = str(row.get(col, "")).strip() if pd.notna(row.get(col)) else ""
            if cat_name and cat_name.lower() not in ("", "nan", "none"):
                canonical_name, cat_id = lookup_category(cat_name)
                if cat_id:
                    categories.append({"id": cat_id, "name": canonical_name})
                else:
                    unmapped.append(cat_name)

        if not categories or unmapped:
            reason = f"unmapped: {unmapped}" if unmapped else "no categories"
            logger.info("   ⏭️ Ad %s: Skipped — %s", ad_id, reason)
            skipped_count += 1
            continue

        cat_names = [c["name"] for c in categories]
        prepared_ads.append((ad_id, categories, cat_names))

    if not prepared_ads:
        logger.info("   ℹ️ No ads with 'Require Update' status to process")
        logger.info("   Total: %d | Skipped: %d", total_rows, skipped_count)
        return {"total_rows": total_rows, "update_count": 0, "success_count": 0,
                "failed_count": 0, "skipped_count": skipped_count, "results": []}

    # 4. Get fresh dev token
    logger.info("   🔑 Getting fresh dev access token...")
    token_data = _cdc_get_access_token(db_api_base_url, client_id, client_secret, grant_type)
    token_holder = {"access_token": token_data["access_token"]}
    token_lock = threading.Lock()

    # Token refresh helper (re-fetch if needed during long runs)
    token_expiry = time.time() + token_data.get("expires_in", 3600) - 300

    def _get_valid_token():
        nonlocal token_expiry
        with token_lock:
            if time.time() >= token_expiry:
                logger.info("   🔄 Refreshing dev access token...")
                new_data = _cdc_get_access_token(db_api_base_url, client_id, client_secret, grant_type)
                token_holder["access_token"] = new_data["access_token"]
                token_expiry = time.time() + new_data.get("expires_in", 3600) - 300
            return token_holder["access_token"]

    # 5. Multithreaded PUT updates
    update_base_url = f"{db_api_base_url}/trucks"
    max_workers = 5
    logger.info("   📝 Updating %d ads in dev DB (%d workers)", len(prepared_ads), max_workers)

    results = []
    update_start = time.time()

    def _update_worker(ad_data, idx, total):
        ad_id, categories, cat_names = ad_data
        current_token = _get_valid_token()
        logger.info("   [%d/%d] 📤 Ad %s → %s", idx, total, ad_id, cat_names)
        result = update_ad_categories_in_db(update_base_url, ad_id, categories, current_token)
        if result["success"]:
            logger.info("   [%d/%d] ✅ Ad %s: Updated", idx, total, ad_id)
        else:
            logger.error("   [%d/%d] ❌ Ad %s: %s", idx, total, ad_id, result.get('error', 'Unknown'))

        # ── CDC Audit Log: Record old vs new categories to Loki ──
        if config.enable_cdc_audit_log:
            try:
                row_match = df[df["Ad ID"] == ad_id]
                breadcrumbs = ["", "", ""]
                if not row_match.empty:
                    row_data = row_match.iloc[0]
                    for i, col in enumerate(["Breadcrumb_Top1", "Breadcrumb_Top2", "Breadcrumb_Top3"]):
                        val = row_data.get(col, "")
                        breadcrumbs[i] = str(val).strip() if pd.notna(val) else ""
                cdc_audit_logger.log_category_change(
                    ad_id=ad_id, job_id=job_id, environment="dev",
                    old_breadcrumbs=breadcrumbs, new_annotated=cat_names,
                    old_patch_categories=[],
                    success=result["success"], error=result.get("error", ""),
                )
            except Exception as e:
                logger.warning("      [Audit] Warning: failed to log audit for Ad %s: %s", ad_id, e)

        return {
            "ad_id": ad_id,
            "success": result["success"],
            "error": result.get("error", ""),
            "categories": cat_names,
        }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ad = {
            executor.submit(_update_worker, ad_data, i, len(prepared_ads)): ad_data
            for i, ad_data in enumerate(prepared_ads, 1)
        }
        for future in as_completed(future_to_ad):
            try:
                results.append(future.result())
            except Exception as e:
                ad_data = future_to_ad[future]
                logger.error("   ❌ Worker exception for Ad %s: %s", ad_data[0], e)
                results.append({
                    "ad_id": ad_data[0], "success": False,
                    "error": f"Worker exception: {str(e)}", "categories": ad_data[2],
                })

    elapsed = time.time() - update_start
    success_count = sum(1 for r in results if r["success"])
    failed_count = sum(1 for r in results if not r["success"])

    logger.info("=" * 80)
    logger.info("🔄 CDC DEV DB UPDATE COMPLETE (%.1fs)", elapsed)
    logger.info("   Total rows: %d | To update: %d | Skipped: %d", total_rows, len(prepared_ads), skipped_count)
    logger.info("   ✅ Success: %d | ❌ Failed: %d", success_count, failed_count)
    logger.info("=" * 80)

    # Save patch summary report as Excel to cdc_ai_output_excels/
    patch_report_filename = None
    if results:
        try:
            report_df = pd.DataFrame([
                {
                    "Ad ID": r["ad_id"],
                    "Categories": ", ".join(r.get("categories", [])),
                    "Update Status": "Success" if r["success"] else "Failed",
                    "Error": r.get("error", ""),
                }
                for r in results
            ])
            run_ts = now_ist().strftime("%Y-%m-%d_%H-%M-%S")
            patch_report_filename = f"CDC_Patch_Summary_{run_ts}.xlsx"
            report_path = os.path.join(CDC_OUTPUT_DIR, patch_report_filename)
            os.makedirs(CDC_OUTPUT_DIR, exist_ok=True)
            report_df.to_excel(report_path, index=False)
            _upload_and_track(report_path, 'cdc/patch-summaries', patch_report_filename)
            logger.info("   📋 Patch Summary report saved: %s", patch_report_filename)
        except Exception as e:
            logger.warning("   ⚠️ Failed to save Patch Summary report: %s", e)

    # Flush any remaining audit log records to Loki
    if config.enable_cdc_audit_log:
        cdc_audit_logger.flush()

    return {
        "total_rows": total_rows,
        "update_count": len(prepared_ads),
        "success_count": success_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count,
        "elapsed_seconds": round(elapsed, 1),
        "results": results,
        "patch_report_filename": patch_report_filename,
    }


def _cdc_prod_db_update(token_url: str, client_id: str, client_secret: str,
                        grant_type: str, update_base_url: str,
                        output_excel_path: str, job_id: str) -> dict:
    """
    Prod DB Update: Full patch lifecycle for CDC pipeline.

    Replicates the complete /api/db-update workflow:
    1. Get bearer token from prod token endpoint
    2. For each ad with Status == "Require Update":
       a. check_ad_patch() — check if ad has existing patch
       b. delete_patch_categories() — remove old patch categories if present
       c. update_ad_categories_in_db() — PUT new categories
       d. create_or_update_ad_patch_categories() — create/update the patch
    3. Generate patch summary Excel
    4. Generate DB update report Excel

    Returns a summary dict with counts and per-ad results.
    """
    logger.info("=" * 80)
    logger.info("🔄 CDC PROD DB UPDATE — Job %s", job_id)
    logger.info("   Excel: %s", os.path.basename(output_excel_path))
    logger.info("   Token URL: %s", token_url)
    logger.info("   Update URL: %s", update_base_url)
    logger.info("=" * 80)

    # 1. Read the annotated output Excel
    df = pd.read_excel(output_excel_path, dtype={"Ad ID": str})
    df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    total_rows = len(df)

    if "Status" not in df.columns or "Annotated_Top1" not in df.columns:
        logger.warning("   ⚠️ Missing required columns (Status, Annotated_Top1) — skipping db update")
        return {"total_rows": total_rows, "update_count": 0, "success_count": 0,
                "failed_count": 0, "skipped_count": total_rows, "results": [],
                "patch_report_filename": None}

    # 2. Filter rows — only "Require Update"
    prepared_ads = []
    skipped_count = 0

    for _, row in df.iterrows():
        ad_id = str(row.get("Ad ID", "")).strip()
        status = str(row.get("Status", "")).strip()
        status_lower = status.lower()

        if not ad_id or ad_id.lower() == "nan":
            skipped_count += 1
            continue

        # Skip known non-update statuses
        should_skip = False
        for skip_status in DB_UPDATE_SKIP_STATUSES:
            if skip_status in status_lower:
                should_skip = True
                break
        if should_skip:
            skipped_count += 1
            continue

        # Only process "Require Update"
        if status_lower != "require update":
            skipped_count += 1
            continue

        # 3. Resolve categories from Annotated_Top1/2/3
        categories = []
        unmapped = []
        for col in ["Annotated_Top1", "Annotated_Top2", "Annotated_Top3"]:
            cat_name = str(row.get(col, "")).strip() if pd.notna(row.get(col)) else ""
            if cat_name and cat_name.lower() not in ("", "nan", "none"):
                canonical_name, cat_id = lookup_category(cat_name)
                if cat_id:
                    categories.append({"id": cat_id, "name": canonical_name})
                else:
                    unmapped.append(cat_name)

        if not categories or unmapped:
            reason = f"unmapped: {unmapped}" if unmapped else "no categories"
            logger.info("   ⏭️ Ad %s: Skipped — %s", ad_id, reason)
            skipped_count += 1
            continue

        cat_names = [c["name"] for c in categories]
        prepared_ads.append((ad_id, categories, cat_names))

    if not prepared_ads:
        logger.info("   ℹ️ No ads with 'Require Update' status to process")
        logger.info("   Total: %d | Skipped: %d", total_rows, skipped_count)
        return {"total_rows": total_rows, "update_count": 0, "success_count": 0,
                "failed_count": 0, "skipped_count": skipped_count, "results": [],
                "patch_report_filename": None}

    # 4. Get fresh prod bearer token
    logger.info("   🔑 Getting fresh PROD access token from %s...", token_url)
    token_info = get_db_update_bearer_token(token_url, client_id, client_secret, grant_type)
    token_holder = {"access_token": token_info["access_token"], "expires_at": token_info["expires_at"]}
    token_lock = threading.Lock()

    def _get_valid_token():
        with token_lock:
            if time.time() >= token_holder["expires_at"]:
                logger.info("   🔄 Refreshing PROD access token...")
                new_info = get_db_update_bearer_token(token_url, client_id, client_secret, grant_type)
                token_holder["access_token"] = new_info["access_token"]
                token_holder["expires_at"] = new_info["expires_at"]
            return token_holder["access_token"]

    # Derive the ad-patches base URL
    patch_base_url = _derive_patch_base_url(update_base_url)
    logger.info("   📌 Patch API base URL: %s", patch_base_url)

    # 5. Multithreaded update with full patch lifecycle
    max_workers = 5
    logger.info("   📝 Updating %d ads in PROD DB (%d workers)", len(prepared_ads), max_workers)
    logger.info("   🩹 Using full patch lifecycle (check → delete → PUT → patch)")

    results = []
    patched_ads_summary = []
    update_start = time.time()

    def _prod_update_worker(ad_data, idx, total):
        """Worker: full patch lifecycle for a single ad (mirrors /api/db-update logic)."""
        ad_id, categories, cat_names = ad_data
        current_token = _get_valid_token()

        logger.info("   [%d/%d] 🔎 Processing Ad %s ...", idx, total, ad_id)

        # Step A: Check for existing patch
        patch_info = check_ad_patch(patch_base_url, ad_id, current_token)

        patch_deleted = False
        old_patch_categories = []
        patch_summary = None

        # Step B: Delete patch categories if present
        if patch_info.get("has_patch") and patch_info.get("has_categories"):
            old_patch_categories = patch_info["categories"]
            old_cat_names = [c.get("name", c.get("id", "?")) for c in old_patch_categories]
            logger.info("      ⚡ Ad %s has PATCHED categories: %s", ad_id, old_cat_names)
            logger.info("      🗑️ Deleting categories from patch before PUT update...")

            current_token = _get_valid_token()
            delete_result = delete_patch_categories(patch_base_url, ad_id, current_token)

            if not delete_result["success"]:
                error_msg = f"Failed to delete patch categories: {delete_result.get('error', 'Unknown error')}"
                logger.error("   [%d/%d] ❌ Ad %s: %s", idx, total, ad_id, error_msg)
                return (
                    {"ad_id": ad_id, "success": False, "error": error_msg, "categories": cat_names},
                    {"ad_id": ad_id, "old_patched_categories": ", ".join(old_cat_names),
                     "new_updated_categories": ", ".join(cat_names),
                     "patch_deleted": "FAILED", "update_success": "No (patch delete failed)",
                     "patch_step": "N/A"},
                )

            patch_deleted = True
            logger.info("      ✅ Patch categories deleted successfully for Ad %s", ad_id)
        elif patch_info.get("has_patch") and not patch_info.get("has_categories"):
            logger.info("      ℹ️ Ad %s has a patch but NO categories in it — proceeding with direct PUT", ad_id)
        else:
            logger.info("      ℹ️ Ad %s has no patch — proceeding with direct PUT", ad_id)

        # Step C: PUT update
        current_token = _get_valid_token()
        logger.info("      📤 Sending PUT to update Ad %s with categories: %s", ad_id, cat_names)
        result = update_ad_categories_in_db(update_base_url, ad_id, categories, current_token)

        if result["success"]:
            logger.info("   [%d/%d] ✅ Ad %s: Updated -> %s", idx, total, ad_id, cat_names)
        else:
            logger.error("   [%d/%d] ❌ Ad %s: %s", idx, total, ad_id, result.get('error', 'Unknown error'))

        # Step D: Create or update ad-patch with new categories
        patch_action = None
        patch_step_success = False
        patch_step_error = ""

        if result["success"]:
            logger.info("      🩹 [Patch Step] Ad %s: Ad update succeeded — now creating/updating patch...", ad_id)
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
                logger.info("   [%d/%d] 🩹 Ad %s: Patch %s successfully with categories: %s", idx, total, ad_id, patch_action, cat_names)
            else:
                logger.warning("   [%d/%d] ⚠️ Ad %s: Ad updated OK but patch step failed — %s", idx, total, ad_id, patch_step_error)
        else:
            logger.info("      ⏭️ [Patch Step] Ad %s: Skipping patch step because ad update failed", ad_id)

        result_dict = {
            "ad_id": ad_id,
            "success": result["success"],
            "error": result.get("error", ""),
            "categories": cat_names,
            "patch_action": patch_action,
            "patch_step_success": patch_step_success,
            "patch_step_error": patch_step_error,
        }

        # Build patch summary if applicable
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

        # ── CDC Audit Log: Record old vs new categories to Loki ──
        if config.enable_cdc_audit_log:
            try:
                row_match = df[df["Ad ID"] == ad_id]
                breadcrumbs = ["", "", ""]
                if not row_match.empty:
                    row_data = row_match.iloc[0]
                    for i, col in enumerate(["Breadcrumb_Top1", "Breadcrumb_Top2", "Breadcrumb_Top3"]):
                        val = row_data.get(col, "")
                        breadcrumbs[i] = str(val).strip() if pd.notna(val) else ""
                old_patch_cat_names = [c.get("name", c.get("id", "")) for c in old_patch_categories]
                cdc_audit_logger.log_category_change(
                    ad_id=ad_id, job_id=job_id, environment="prod",
                    old_breadcrumbs=breadcrumbs, new_annotated=cat_names,
                    old_patch_categories=old_patch_cat_names,
                    success=result["success"], error=result.get("error", ""),
                    patch_action=patch_action or "", patch_deleted=patch_deleted,
                )
            except Exception as e:
                logger.warning("      [Audit] Warning: failed to log audit for Ad %s: %s", ad_id, e)

        return (result_dict, patch_summary)

    # Run with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ad = {
            executor.submit(_prod_update_worker, ad_data, i, len(prepared_ads)): ad_data
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
                logger.error("   ❌ Worker exception for Ad %s: %s", ad_data[0], e)
                results.append({
                    "ad_id": ad_data[0], "success": False,
                    "error": f"Worker exception: {str(e)}", "categories": ad_data[2],
                })

    elapsed = time.time() - update_start
    success_count = sum(1 for r in results if r["success"])
    failed_count = sum(1 for r in results if not r["success"])

    # Count patch step outcomes
    patches_created = sum(1 for r in results if r.get("patch_action") == "created" and r.get("patch_step_success"))
    patches_updated = sum(1 for r in results if r.get("patch_action") == "updated" and r.get("patch_step_success"))
    patches_failed = sum(1 for r in results if r.get("patch_action") and not r.get("patch_step_success"))

    logger.info("=" * 80)
    logger.info("🔄 CDC PROD DB UPDATE COMPLETE (%.1fs)", elapsed)
    logger.info("   Total rows: %d | To update: %d | Skipped: %d", total_rows, len(prepared_ads), skipped_count)
    logger.info("   ✅ Success: %d | ❌ Failed: %d", success_count, failed_count)
    logger.info("   🩹 Patches — created: %d, updated: %d, failed: %d", patches_created, patches_updated, patches_failed)
    logger.info("=" * 80)

    # 6. Save patch summary Excel to cdc_ai_output_excels/
    patch_report_filename = None
    if patched_ads_summary:
        try:
            patch_df = pd.DataFrame(patched_ads_summary)
            column_map = {
                "ad_id": "Ad ID",
                "old_patched_categories": "Old Patched Categories",
                "new_updated_categories": "New Updated Categories",
                "patch_deleted": "Patch Deleted",
                "update_success": "Update Success",
                "patch_step": "Patch Create/Update Step",
            }
            patch_df.rename(columns=column_map, inplace=True)

            run_ts = now_ist().strftime("%Y-%m-%d_%H-%M-%S")
            patch_report_filename = f"CDC_Patch_Summary_{run_ts}.xlsx"
            patch_report_path = os.path.join(CDC_OUTPUT_DIR, patch_report_filename)
            patch_df.to_excel(patch_report_path, index=False, engine="openpyxl")
            _upload_and_track(patch_report_path, 'cdc/patch-summaries', patch_report_filename)
            logger.info("   📋 Patch Summary report saved: %s (%d patched ads)", patch_report_filename, len(patched_ads_summary))
        except Exception as e:
            logger.warning("   ⚠️ Failed to save Patch Summary report: %s", e)

    # Flush any remaining audit log records to Loki
    if config.enable_cdc_audit_log:
        cdc_audit_logger.flush()

    return {
        "total_rows": total_rows,
        "update_count": len(prepared_ads),
        "success_count": success_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count,
        "elapsed_seconds": round(elapsed, 1),
        "patches_created": patches_created,
        "patches_updated": patches_updated,
        "patches_failed": patches_failed,
        "patched_count": len(patched_ads_summary),
        "results": results,
        "patch_report_filename": patch_report_filename,
    }


def run_cdc_pipeline_sync(job_id: str, ad_ids: list, client_id: str, client_secret: str,
                          grant_type: str, db_api_base_url: str,
                          cdc_env: str = "dev", token_url: str = "",
                          update_base_url: str = ""):
    """
    CDC Pipeline: DB Fetch + AI Annotation for CDC-filtered truck ads.

    Supports both dev and prod environments:
    - dev: Uses dev DB API for fetch + simple PUT for update
    - prod: Uses prod DB API (nebulous-prod) for fetch + full patch lifecycle for update

    1. Fetches truck data from DB API by ad IDs
    2. Saves intermediate fetch Excel to cdc_ai_output_excels/
    3. Runs AI annotation pipeline
    4. Saves annotated output to cdc_ai_output_excels/
    5. Updates DB (dev: simple PUT, prod: patch lifecycle)
    """
    is_prod = (cdc_env == "prod")
    env_label = "PROD ⚠️" if is_prod else "DEV"
    job = jobs[job_id]
    run_ts = now_ist().strftime("%Y-%m-%d_%H-%M-%S")

    try:
        logger.info("=" * 80)
        logger.info("CDC PIPELINE JOB %s STARTED [%s]", job_id, env_label)
        logger.info("   Environment: %s", env_label)
        logger.info("   Ad IDs: %d", len(ad_ids))
        logger.info("   Output Dir: %s", CDC_OUTPUT_DIR)
        if is_prod:
            logger.info("   Token URL: %s", token_url)
            logger.info("   Update URL: %s", update_base_url)
        logger.info("=" * 80)

        os.makedirs(CDC_OUTPUT_DIR, exist_ok=True)

        # ── Ad Annotation Limit: Pre-filter ──
        if config.enable_ad_annotation_limit:
            over_limit = ad_tracker.filter_over_limit_ads(ad_ids, config.max_annotation_runs)
            if over_limit:
                logger.info("   Ad Tracker: Skipping %d ads (already annotated %d+ times)", len(over_limit), config.max_annotation_runs)
                ad_ids = [aid for aid in ad_ids if aid not in over_limit]
                logger.info("   Ad Tracker: %d ads remaining", len(ad_ids))

        if not ad_ids:
            raise ValueError("No ads to process after filtering")

        job['total_ads'] = len(ad_ids)
        job['status'] = "fetching"

        if is_prod:
            # ==================== PROD: Fetch via prod API ====================

            # ========== STEP 1: Get Access Token (Prod) ==========
            logger.info("=" * 80)
            logger.info("STEP 1: AUTHENTICATING WITH PROD DB API (%s)", db_api_base_url)
            logger.info("=" * 80)

            token_data = _cdc_get_access_token(db_api_base_url, client_id, client_secret, grant_type)
            access_token = token_data['access_token']
            logger.info("=" * 80)

            # ========== STEP 2: Fetch Trucks via Prod API (Multithreaded) ==========
            # Derive the trucks endpoint from the base URL
            prod_trucks_url = f"{db_api_base_url.rstrip('/')}/trucks"
            logger.info("=" * 80)
            logger.info("STEP 2: FETCHING %d TRUCKS FROM PROD DB API", len(ad_ids))
            logger.info("=" * 80)
            logger.info("   Using endpoint: %s/{ad_id}", prod_trucks_url)

            fetched_trucks = []
            not_found_ids = []
            error_ids = []

            max_workers = 5
            logger.info("   Starting %d concurrent fetch workers...", max_workers)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_ad_id = {
                    executor.submit(_fetch_single_truck_worker, ad_id, access_token, i, len(ad_ids),
                                    base_url=prod_trucks_url): ad_id
                    for i, ad_id in enumerate(ad_ids, 1)
                }
                for future in as_completed(future_to_ad_id):
                    status, ad_id, result = future.result()
                    if status == 'success':
                        fetched_trucks.append(result)
                    elif status == 'not_found':
                        not_found_ids.append(ad_id)
                    elif status == 'error':
                        error_ids.append(ad_id)

        else:
            # ==================== DEV: Fetch via dev API ====================

            # ========== STEP 1: Get Access Token (Dev DB API) ==========
            logger.info("=" * 80)
            logger.info("STEP 1: AUTHENTICATING WITH DEV DB API (%s)", db_api_base_url)
            logger.info("=" * 80)

            token_data = _cdc_get_access_token(db_api_base_url, client_id, client_secret, grant_type)
            access_token = token_data['access_token']
            logger.info("=" * 80)

            # ========== STEP 2: Fetch Trucks (Multithreaded) ==========
            logger.info("=" * 80)
            logger.info("STEP 2: FETCHING %d TRUCKS FROM DEV DB API", len(ad_ids))
            logger.info("=" * 80)

            fetched_trucks = []
            not_found_ids = []
            error_ids = []

            max_workers = 5
            logger.info("   Starting %d concurrent fetch workers...", max_workers)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_ad_id = {
                    executor.submit(_cdc_fetch_worker, db_api_base_url, ad_id, access_token, i, len(ad_ids)): ad_id
                    for i, ad_id in enumerate(ad_ids, 1)
                }
                for future in as_completed(future_to_ad_id):
                    status, ad_id, result = future.result()
                    if status == 'success':
                        fetched_trucks.append(result)
                    elif status == 'not_found':
                        not_found_ids.append(ad_id)
                    elif status == 'error':
                        error_ids.append(ad_id)

        # ── Common steps from here ──

        logger.info("   Fetched: %d/%d trucks", len(fetched_trucks), len(ad_ids))
        if not_found_ids:
            logger.info("   Not found: %d - %s", len(not_found_ids), not_found_ids[:10])
        if error_ids:
            logger.error("   Errors: %d - %s", len(error_ids), error_ids[:10])
        logger.info("=" * 80)

        # Apply CTT Platform Filter
        fetched_trucks, non_ctt_ids = filter_ctt_platform_trucks(fetched_trucks)

        if len(fetched_trucks) == 0 and len(non_ctt_ids) == 0:
            raise ValueError("No trucks were successfully fetched from the database")

        # ========== STEP 3: Process Truck Data ==========
        logger.info("=" * 80)
        logger.info("STEP 3: PROCESSING TRUCK DATA")
        logger.info("=" * 80)

        processed_trucks = []
        for i, truck in enumerate(fetched_trucks, 1):
            processed = process_truck_data(truck, debug=(i == 1))
            processed_trucks.append(processed)

        for ad_id in not_found_ids + error_ids:
            processed_trucks.append({
                'Ad ID': ad_id,
                'Breadcrumb_Top1': 'Inactive ad',
                'Breadcrumb_Top2': '',
                'Breadcrumb_Top3': '',
                'Image_URLs': ''
            })

        # Add non-CTT platform trucks
        for ad_id in non_ctt_ids:
            processed_trucks.append({
                'Ad ID': ad_id,
                'Breadcrumb_Top1': 'Non-CTT Platform',
                'Breadcrumb_Top2': '',
                'Breadcrumb_Top3': '',
                'Image_URLs': ''
            })

        logger.info("   Processed: %d trucks total", len(processed_trucks))
        if non_ctt_ids:
            logger.info("   Non-CTT Platform: %d ads filtered out", len(non_ctt_ids))
        logger.info("=" * 80)

        # ========== STEP 4: Save Fetch Excel ==========
        result_df = pd.DataFrame(processed_trucks)
        result_df["Ad ID"] = result_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        result_df = result_df.drop_duplicates(subset=["Ad ID"], keep='first').reset_index(drop=True)

        fetch_filename = f"CDC_Fetch_{run_ts}.xlsx"
        fetch_path = os.path.join(CDC_OUTPUT_DIR, fetch_filename)
        result_df.to_excel(fetch_path, index=False)

        # Upload to B2 (keep local — annotation pipeline reads it next)
        _upload_and_track(fetch_path, 'cdc/fetch', fetch_filename, delete_local=False)

        logger.info("   Fetch Excel saved: %s", fetch_path)

        job['status'] = 'fetched'
        job['file_path'] = fetch_path

        # ========== STEP 5: Run AI Annotation ==========
        logger.info("=" * 80)
        logger.info("STEP 5: RUNNING AI ANNOTATION PIPELINE")
        logger.info("=" * 80)

        # Temporarily redirect output_dir to CDC folder
        original_output_dir = config.output_dir
        config.output_dir = CDC_OUTPUT_DIR

        try:
            run_db_annotation_pipeline_sync(job_id, fetch_path, b2_folder_override='cdc/annotated')
        finally:
            config.output_dir = original_output_dir

        # If enable_ai_output=False, the annotated output landed in temp instead of
        # CDC_OUTPUT_DIR (because load_config() re-reads config.ini mid-pipeline).
        # Move it to CDC_OUTPUT_DIR locally so the DB update step can read it.
        # B2 upload is already handled by the annotation function via b2_folder_override='cdc/annotated'.
        output_excel = job.get('output_file', '')
        if output_excel and os.path.exists(output_excel):
            if CDC_OUTPUT_DIR not in os.path.dirname(os.path.abspath(output_excel)):
                dest = os.path.join(CDC_OUTPUT_DIR, os.path.basename(output_excel))
                import shutil
                shutil.move(output_excel, dest)
                job['output_file'] = dest
                logger.info("   📁 Moved annotated output to CDC folder: %s", os.path.basename(dest))

            # Also move any batch files that ended up in temp
            for batch_info in job.get('batches', []):
                bp = batch_info.get('file_path', '')
                if bp and os.path.exists(bp) and CDC_OUTPUT_DIR not in os.path.dirname(os.path.abspath(bp)):
                    batch_dest = os.path.join(CDC_OUTPUT_DIR, os.path.basename(bp))
                    shutil.move(bp, batch_dest)
                    batch_info['file_path'] = batch_dest

        # ========== STEP 6: DB Update ==========
        output_excel = job.get('output_file')
        if output_excel and os.path.exists(output_excel):
            try:
                if is_prod:
                    # ── PROD: Full patch lifecycle ──
                    db_update_result = _cdc_prod_db_update(
                        token_url, client_id, client_secret, grant_type,
                        update_base_url, output_excel, job_id
                    )
                else:
                    # ── DEV: Simple PUT (existing behavior) ──
                    db_update_result = _cdc_dev_db_update(
                        db_api_base_url, client_id, client_secret, grant_type,
                        output_excel, job_id
                    )
                job['db_update_result'] = db_update_result
            except Exception as e:
                update_type = "Prod" if is_prod else "Dev"
                logger.warning("   ⚠️ STEP 6 %s DB Update failed (non-fatal): %s", update_type, e)
                import traceback
                traceback.print_exc()
                job['db_update_result'] = {"error": str(e)}
        else:
            logger.info("   ⏭️ STEP 6: Skipped — no output file found for db update")

        # Clean up local CDC files — they're already on B2
        # Flush pending uploads first so local files aren't deleted mid-upload
        if b2_is_enabled():
            flush_uploads()
            for f in os.listdir(CDC_OUTPUT_DIR):
                fpath = os.path.join(CDC_OUTPUT_DIR, f)
                if os.path.isfile(fpath):
                    try:
                        os.remove(fpath)
                    except OSError:
                        pass
            logger.info("   [B2] Local CDC files cleaned up (backed up on B2)")

        logger.info("=" * 80)
        logger.info("CDC PIPELINE JOB %s COMPLETE! [%s]", job_id, env_label)
        logger.info("=" * 80)

    except Exception as e:
        job['status'] = JobStatus.FAILED
        job['error'] = str(e)
        logger.error("CDC PIPELINE JOB %s FAILED: %s", job_id, str(e))
        import traceback
        traceback.print_exc()


@app.post("/api/cdc-trigger")
async def cdc_trigger(payload: dict, background_tasks: BackgroundTasks):
    """
    Trigger the CDC pipeline: DB Fetch + AI Annotation for a list of ad IDs.

    Called by the cdc_pipeline/run_annotation.py script after the CDC consumer
    has collected filtered ads in filtered_ads.jsonl.

    Supports both dev and prod environments via the "cdc_env" field.

    Accepts: {
        "ad_ids": ["123", "456", ...],
        "cdc_env": "dev" | "prod",
        "db_api_base_url": "",
        "client_id": "...",
        "client_secret": "...",
        "grant_type": "",
        "token_url": "" (prod only),
        "update_base_url": "" (prod only)
    }
    Returns: {"job_id": "...", "total_ads": N, "status": "fetching"}
    """
    ad_ids = payload.get("ad_ids", [])
    if not ad_ids:
        raise HTTPException(status_code=400, detail="ad_ids list is empty")

    # Deduplicate
    ad_ids = list(dict.fromkeys(str(aid).strip() for aid in ad_ids))

    # Environment flag
    cdc_env = payload.get("cdc_env", "dev").strip().lower()
    is_prod = (cdc_env == "prod")

    # DB API credentials from the request (sent by cdc_pipeline/.env)
    db_api_base_url = payload.get("db_api_base_url", "").strip()
    client_id = payload.get("client_id", "").strip()
    client_secret = payload.get("client_secret", "").strip()
    grant_type = payload.get("grant_type", "client_credentials").strip()

    # Prod-only fields
    token_url = payload.get("token_url", "").strip()
    update_base_url = payload.get("update_base_url", "").strip()

    if not db_api_base_url:
        raise HTTPException(status_code=400, detail="db_api_base_url is required")
    if not client_id:
        raise HTTPException(status_code=400, detail="client_id is required")
    if not client_secret:
        raise HTTPException(status_code=400, detail="client_secret is required")

    if is_prod:
        if not token_url:
            raise HTTPException(status_code=400, detail="token_url is required for prod environment")
        if not update_base_url:
            raise HTTPException(status_code=400, detail="update_base_url is required for prod environment")

    job_id = str(uuid.uuid4())[:8]

    jobs[job_id] = {
        "id": job_id,
        "status": "fetching",
        "total_ads": len(ad_ids),
        "created_at": datetime.now().isoformat(),
        "is_cdc_triggered": True,
        "cdc_env": cdc_env,
    }

    background_tasks.add_task(
        run_cdc_pipeline_sync,
        job_id,
        ad_ids,
        client_id,
        client_secret,
        grant_type,
        db_api_base_url,
        cdc_env,
        token_url,
        update_base_url,
    )

    env_label = "PROD ⚠️" if is_prod else "DEV"
    return {
        "job_id": job_id,
        "total_ads": len(ad_ids),
        "status": "fetching",
        "cdc_env": cdc_env,
        "message": f"CDC pipeline [{env_label}] started for {len(ad_ids)} ads. Output: cdc_ai_output_excels/",
    }


@app.get("/api/cdc-trigger/{job_id}/status")
async def cdc_trigger_status(job_id: str):
    """Check status of a CDC-triggered pipeline job."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job ID not found")

    job = jobs[job_id]
    return {
        "job_id": job_id,
        "status": job.get("status"),
        "total_ads": job.get("total_ads"),
        "output_file": job.get("output_filename"),
        "error": job.get("error"),
        "db_update_result": job.get("db_update_result"),
    }


@app.get("/api/cdc-outputs")
async def list_cdc_outputs():
    """List all files in B2 cloud or local cdc_ai_output_excels/ for the CDC Outputs UI tab."""

    # When B2 is enabled, list from cloud (recursive across date folders)
    if b2_is_enabled():
        return {
            "annotation_files": list_b2_files("awacs-outputs/cdc/annotated/"),
            "db_fetch_files": list_b2_files("awacs-outputs/cdc/fetch/"),
            "patch_summary_files": list_b2_files("awacs-outputs/cdc/patch-summaries/"),
        }

    # Fallback: list from local filesystem
    import glob as _glob

    os.makedirs(CDC_OUTPUT_DIR, exist_ok=True)

    annotation_files = []
    db_fetch_files = []
    patch_summary_files = []

    for filepath in sorted(_glob.glob(os.path.join(CDC_OUTPUT_DIR, "*.xlsx")), key=os.path.getmtime, reverse=True):
        fname = os.path.basename(filepath)
        try:
            stat = os.stat(filepath)
            info = {
                "filename": fname,
                "size_kb": round(stat.st_size / 1024, 1),
                "created_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            }
        except OSError:
            continue

        if fname.startswith("CDC_Patch_Summary_"):
            patch_summary_files.append(info)
        elif fname.startswith("CDC_Fetch_"):
            db_fetch_files.append(info)
        elif "annotated" in fname.lower() or fname.startswith("batch_"):
            annotation_files.append(info)

    return {
        "annotation_files": annotation_files,
        "db_fetch_files": db_fetch_files,
        "patch_summary_files": patch_summary_files,
    }


@app.get("/api/cdc-outputs/download/{filename}")
async def download_cdc_output(filename: str):
    """Download a specific file from B2 cloud or local cdc_ai_output_excels/."""
    # Prevent path traversal
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    # Try B2 pre-signed URL redirect first
    if b2_is_enabled():
        b2_key = get_b2_key_for_cdc_file(filename)
        if b2_key:
            url = get_download_url(b2_key)
            if url:
                return RedirectResponse(url=url, status_code=302)

    # Fallback to local file
    filepath = os.path.join(CDC_OUTPUT_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        path=filepath,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.delete("/api/cdc-outputs/delete")
async def delete_cdc_outputs(type: str):
    """
    Delete CDC output files by type.
    type=annotation      — deletes annotated output and batch files
    type=db_fetch        — deletes CDC_Fetch_*.xlsx files
    type=patch_summary   — deletes CDC_Patch_Summary_*.xlsx files
    """
    if type not in ("annotation", "db_fetch", "patch_summary"):
        raise HTTPException(status_code=400, detail="type must be 'annotation', 'db_fetch', or 'patch_summary'")

    # When B2 is enabled, delete from cloud storage
    if b2_is_enabled():
        prefix_map = {
            "annotation": "awacs-outputs/cdc/annotated/",
            "db_fetch": "awacs-outputs/cdc/fetch/",
            "patch_summary": "awacs-outputs/cdc/patch-summaries/",
        }
        deleted = delete_b2_files(prefix_map[type])
        logger.info("   🗑️ Deleted %d CDC %s file(s) from B2", deleted, type)
        return {"deleted": deleted, "type": type}

    # Fallback: delete from local filesystem
    deleted = 0
    if not os.path.exists(CDC_OUTPUT_DIR):
        return {"deleted": 0, "type": type}

    for fname in os.listdir(CDC_OUTPUT_DIR):
        if not fname.endswith(".xlsx"):
            continue

        should_delete = False
        if type == "db_fetch" and fname.startswith("CDC_Fetch_"):
            should_delete = True
        elif type == "patch_summary" and fname.startswith("CDC_Patch_Summary_"):
            should_delete = True
        elif type == "annotation" and ("annotated" in fname.lower() or fname.startswith("batch_")):
            should_delete = True

        if should_delete:
            try:
                os.remove(os.path.join(CDC_OUTPUT_DIR, fname))
                deleted += 1
            except OSError:
                pass

    logger.info("   🗑️ Deleted %d CDC %s file(s)", deleted, type)
    return {"deleted": deleted, "type": type}


@app.get("/api/cdc-audit/{ad_id}")
async def get_cdc_audit_log(ad_id: str, limit: int = 50):
    """
    Look up CDC category change history for a specific ad ID.

    Use case: Dealer asks 'why were my categories changed?'
    Returns all category changes with old/new values and timestamps from Grafana Loki.
    """
    if not config.enable_cdc_audit_log:
        raise HTTPException(status_code=404, detail="CDC audit logging is not enabled")

    records = cdc_audit_logger.query_ad_history(ad_id, limit=limit)
    return {"ad_id": ad_id, "total_changes": len(records), "history": records}


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
    logger.info("=" * 80)
    logger.info("🗄️ DB FETCH - FETCHING DATA FROM DATABASE API (PRODUCTION)")
    logger.info("=" * 80)

    # If min and max timestamps are the same, expand to full day (24 hours)
    min_timestamp = request.min_last_update
    max_timestamp = request.max_last_update

    if min_timestamp == max_timestamp:
        # Expand to full day: from 00:00:00 to 23:59:59
        max_timestamp = min_timestamp + 86399  # 86399 seconds = 23 hours, 59 minutes, 59 seconds
        logger.info("   ℹ️  Same timestamp detected - expanding to full day")
        logger.info("   📅 Original: %s", request.min_last_update)
        logger.info("   📅 Expanded: %s → %s (full 24 hours)", min_timestamp, max_timestamp)
    else:
        logger.info("   📅 Date Range: %s → %s", min_timestamp, max_timestamp)

    logger.info("   📊 Listing Range: %s → %s", request.listing_start, request.listing_end)
    logger.info("=" * 80)
    
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
        
        logger.info("   🔑 Using credentials from: %s", 'Request' if request.client_id else 'config.ini')
        logger.info("   🔑 Client ID: %s...", client_id[:10])
        logger.info("   🔑 Client Secret length: %d chars", len(client_secret))
        logger.info("   🔑 Grant Type: %s", grant_type)
        
        # Step 1: Get access token
        token_data = get_access_token(
            client_id,
            client_secret,
            grant_type
        )
        access_token = token_data['access_token']
        
        # Step 2: Calculate how many trucks to fetch
        total_listings_needed = request.listing_end - request.listing_start
        
        logger.info("   📊 Need to fetch %d listings", total_listings_needed)
        logger.info("   📦 Will use pagination with max 500 per request")
        
        # Step 3: Fetch trucks with pagination
        all_trucks = []
        current_offset = request.listing_start
        limit_per_request = 500  # Max allowed by API
        
        logger.info("=" * 80)
        logger.info("📦 FETCHING TRUCKS DATA WITH PAGINATION")
        logger.info("=" * 80)

        # Track total available for pagination guidance
        total_available_in_db = 0

        while len(all_trucks) < total_listings_needed:
            # Calculate how many more we need
            remaining = total_listings_needed - len(all_trucks)
            current_limit = min(limit_per_request, remaining)

            logger.info("   🔄 Request %d: Offset=%d, Limit=%d", len(all_trucks) // 500 + 1, current_offset, current_limit)
            
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
                logger.warning("   ⚠️ No more trucks available")
                break

            all_trucks.extend(batch_trucks)
            current_offset += len(batch_trucks)

            logger.info("   ✅ Batch complete. Total fetched so far: %d", len(all_trucks))
            
            # Check if we've reached the total available
            pagination = batch_data.get('pagination', {})
            total_available_in_db = pagination.get('total', 0)
            
            if current_offset >= total_available_in_db:
                logger.info("   ℹ️  Reached end of available data (total: %d)", total_available_in_db)
                break

        logger.info("=" * 80)
        logger.info("✅ FETCHING COMPLETE - Retrieved %d trucks", len(all_trucks))
        logger.info("   Total available in DB for this date range: %d", total_available_in_db)
        logger.info("=" * 80)

        # Step 3.5: Apply CTT Platform Filter
        all_trucks, non_ctt_ids = filter_ctt_platform_trucks(all_trucks)

        # Step 4: Process truck data
        logger.info("=" * 80)
        logger.info("🔄 PROCESSING TRUCK DATA")
        logger.info("=" * 80)

        processed_trucks = []
        for i, truck in enumerate(all_trucks, 1):
            # Enable debug for first truck to see what we're getting
            debug = (i == 1)
            processed = process_truck_data(truck, debug=debug)
            processed_trucks.append(processed)

            if i % 100 == 0:
                logger.info("   ✅ Processed %d/%d trucks", i, len(all_trucks))

        # Add non-CTT platform trucks to output (for tracking)
        for ad_id in non_ctt_ids:
            processed_trucks.append({
                'Ad ID': ad_id,
                'Breadcrumb_Top1': 'Non-CTT Platform',
                'Breadcrumb_Top2': '',
                'Breadcrumb_Top3': '',
                'Image_URLs': ''
            })

        logger.info("   ✅ Processed all %d CTT trucks", len(all_trucks))
        if non_ctt_ids:
            logger.info("   ℹ️  Added %d Non-CTT Platform entries to output", len(non_ctt_ids))
        logger.info("=" * 80)

        # Step 5: Apply category filters if provided
        if request.category_filters and len(request.category_filters) > 0:
            logger.info("=" * 80)
            logger.info("🔍 APPLYING CATEGORY FILTERS")
            logger.info("=" * 80)
            logger.info("   Filters: %s", request.category_filters)
            
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
            
            logger.info("   Before filtering: %d trucks", len(processed_trucks))
            logger.info("   After filtering: %d trucks", len(filtered_trucks))
            logger.info("   Filtered out: %d trucks", len(processed_trucks) - len(filtered_trucks))
            logger.info("=" * 80)
            
            processed_trucks = filtered_trucks
        
        # Check if any data was fetched
        if len(processed_trucks) == 0:
            logger.warning("=" * 80)
            logger.warning("⚠️ NO DATA FOUND")
            logger.warning("   No trucks found for the specified date range and listing range.")
            logger.warning("=" * 80)
            raise HTTPException(
                status_code=404,
                detail=f"No trucks found for date range {min_timestamp} to {max_timestamp}. Please try a different date range or check if data exists for this period."
            )
        
        # Step 5: Create DataFrame and save intermediate file
        logger.info("=" * 80)
        logger.info("📄 CREATING INTERMEDIATE EXCEL FILE")
        logger.info("=" * 80)
        
        df = pd.DataFrame(processed_trucks)
        df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # Save to file (read back by start_db_annotation + download — must always be saved)
        timestamp = now_ist().strftime("%Y-%m-%d_%H-%M-%S")
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

        # Upload to B2 (keep local — may be used for annotation)
        b2_key = _upload_and_track(output_path, 'db-fetch', output_filename, delete_local=False)

        logger.info("   ✅ Excel file created: %s", output_filename)
        logger.info("   📁 Path: %s", output_path)
        logger.info("=" * 80)

        jobs[fetch_id] = {
            "id": fetch_id,
            "filename": output_filename,
            "file_path": output_path,
            "b2_key": b2_key,
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
        
        logger.info("=" * 80)
        logger.info("🎉 DB FETCH COMPLETE - READY FOR PREVIEW!")
        logger.info("   Total Trucks: %d", len(processed_trucks))
        logger.info("   File: %s", output_filename)
        logger.info("   Fetch ID: %s", fetch_id)
        logger.info("   Status: Ready for annotation")
        logger.info("   📊 PAGINATION INFO:")
        logger.info("   First Ad ID: %s", first_ad_id)
        logger.info("   Last Ad ID: %s", last_ad_id)
        logger.info("   Total Available in DB: %d", total_available_in_db)
        if filters_applied:
            logger.info("   🔍 Category Filters Applied: %s", request.category_filters)
            logger.info("   Fetched (before filter): %d", fetched_before_filter)
            logger.info("   Matched (after filter): %d", matched_after_filter)
        logger.info("   Has More Data: %s", has_more_data)
        if has_more_data:
            logger.info("   Next Start: %d (remaining: %d)", next_start, remaining_listings)
        else:
            logger.info("   ✅ This was the last batch!")
        logger.info("=" * 80)
        
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
        logger.error("❌ DB FETCH FAILED: %s", str(e))
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
    
    logger.info("🤖 Starting AI annotation for fetch %s (job %s)", fetch_id, job_id)
    
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

    # Try B2 pre-signed URL redirect first
    b2_key = fetch_job.get('b2_key')
    if b2_key and b2_is_enabled():
        url = get_download_url(b2_key)
        if url:
            return RedirectResponse(url=url, status_code=302)

    # Fallback to local file
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

    # Archive upload to B2 (keep local — pipeline needs it)
    _upload_and_track(file_path, 'uploads', upload_filename, delete_local=False)

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
# CATEGORY_ID_MAP = {
#     "Flatbed Truck": "2000617",
#     "Pickup Truck": "2000635",
#     "Mechanics Truck": "644245525",
#     "Utility Truck - Service Truck": "2002561",
#     "Dump Truck": "2000609",
#     "Flatbed Dump": "2011212",
#     "Landscape Truck": "2000625",
#     "Contractor Truck": "644247521",
#     "Stake Bed": "2014892",
#     "Hauler": "2005520",
#     "Cab Chassis": "2000881",
#     "Stepvan": "2013294",
#     "Selfloader": "2007240",
#     "Bucket Truck - Boom Truck": "2005161",
#     "Cabover Truck - COE": "2000559",
#     "Box Truck - Straight Truck": "2002281",
#     "Moving Van": "2012012",
#     "Refrigerated Truck": "2000641",
#     "Cutaway-Cube Van": "644245665",
#     "Van": "2000523",
#     "Cargo Van": "2011732",
#     "Passanger Van": "644245480",
#     "Rollback - Tow Truck": "2009720",
#     "Conventional Day Cab": "2000601",
#     "Dually": "644245588",
#     "Dry Van": "644245645",
# }

CATEGORY_ID_MAP = {
    # Existing
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
    "Reefer/Refrigerated Truck": "2000641",
    "Cutaway-Cube Van": "644245665",
    "Van": "2000523",
    "Cargo Van": "2011732",
    "Passanger Van": "644245480",  # (keeping your typo for compatibility)
    "Rollback - Tow Truck": "2009720",
    "Conventional Day Cab": "2000601",
    "Dually": "644245588",
    "Dry Van": "644245645",
    # Added
    "SUV": "644250031",
    "Concrete Barricade Truck": "644248721",
    "Food Truck": "644247060",
    "Milk Truck": "644247926",
    "Livestock Truck": "644248521",
    "Frac Truck": "644247561",
    "Stone Spreader Truck": "644247781",
    "Transfer Truck": "644247541",
    "Ambulance": "2000545",
    "Animal Services": "644248102",
    "Armored Truck": "2008200",
    "Asphalt Distributor Truck": "644248845",
    "Attenuator": "2014172",
    "Auger": "2007482",
    "Beverage Truck": "2000547",
    "Bus": "2000551",
    "Cable Dispenser": "2011652",
    "Cable Scrapper - Cable Puller": "2008202",
    "Cabover Truck - Sleeper": "2000563",
    "Car Carrier": "2000683",
    "Catering Truck - Food Truck": "2006526",
    "Chipper Truck": "2008760",
    "Concrete Pump Truck": "644247221",
    "Conventional - Day Cab": "2000601",
    "Conventional - Sleeper Truck": "2000603",
    "Crane Truck": "2000605",
    "Crew Van": "644245705",
    "Curtain Side": "644247225",
    "De-Icer": "644249982",
    "Digger Derrick": "2005121",
    "Expeditor-Hotshot": "2004120",
    "Farm Truck - Grain Truck": "2000611",
    "Fire Truck": "2000613",
    "Fuel Truck - Lube Truck": "2012574",
    "Garbage Truck": "2000623",
    "Glass Truck": "2013172",
    "Glider Kit": "644247323",
    "Grapple Truck": "2007320",
    "Hooklift Truck": "2013772",
    "Hot Oil Truck": "644247389",
    "Insulator Washer": "2007442",
    "Knucklebooms": "644248201",
    "Logging": "2000627",
    "LPG Tank Truck": "644249963",
    "Lugger": "644250006",
    "Military": "2008000",
    "Mini Truck": "2011452",
    "Minibus": "644245706",
    "Mixer Truck - Concrete Truck": "2000633",
    "Mobility Van": "644247161",
    "Oil Tank Truck": "644245685",
    "Other Truck": "2012932",
    "Passenger Van": "644245480",
    "Plow Truck - Spreader Truck": "2000637",
    "Plumber Service Truck": "644245653",
    "Railroad Truck": "644249979",
    "Recycle Truck": "644247390",
    "Refuse": "644247398",
    "Roll Off Truck": "2010814",
    "Rollback Tow Truck": "2009720",
    "Roustabout": "644248062",
    "Salvage Truck": "644247388",
    "Saw Body": "644248241",
    "Septic": "2011932",
    "Sewer Inspection Trucks": "644251143",
    "Sewer Trucks": "2008206",
    "Shredder Truck": "2014252",
    "Sling Truck": "2015012",
    "Spray Truck": "2015216",
    "Street Cleaner": "2008800",
    "Sweeper": "2001720",
    "Tanker Truck": "2014462",
    "Toter": "2010852",
    "Tractor": "2008002",
    "Truck Mounted Stripers": "644248001",
    "Vacuum Truck": "2009082",
    "Waste Oil Trucks": "2014372",
    "Water Tank": "2014412",
    "Water Truck": "2008240",
    "Western Hauler": "644247084",
    "Winch Truck": "2010456",
    "Wrecker Tow Truck": "2006880",
    "Yard Spotter Truck": "2006044",
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
    "non-ctt platform",
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
    
    logger.info("   ✅ Bearer token obtained successfully (expires in %ds, will refresh at %ds)", expires_in, expires_in - 300)
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
            logger.info("      🔍 Patch check for Ad %s: PATCH FOUND | categories in patch: %s", ad_id, has_categories)
            if has_categories:
                cat_names = [c.get("name", c.get("id", "?")) for c in categories]
                logger.info("         Patched categories: %s", cat_names)
            return {
                "has_patch": True,
                "has_categories": has_categories,
                "categories": categories,
            }
        elif response.status_code == 404:
            logger.info("      🔍 Patch check for Ad %s: NO PATCH (404)", ad_id)
            return {
                "has_patch": False,
                "has_categories": False,
                "categories": [],
            }
        else:
            error_msg = f"Patch check returned HTTP {response.status_code}: {response.text[:200]}"
            logger.warning("      ⚠️ Patch check for Ad %s: %s", ad_id, error_msg)
            return {
                "has_patch": False,
                "has_categories": False,
                "categories": [],
                "error": error_msg,
            }
    except Exception as e:
        error_msg = f"Patch check request failed: {str(e)}"
        logger.warning("      ⚠️ Patch check for Ad %s: %s", ad_id, error_msg)
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
            logger.info("      🗑️ Successfully deleted categories from patch for Ad %s (204)", ad_id)
            return {"success": True}
        else:
            error_msg = f"Patch category delete returned HTTP {response.status_code}: {response.text[:200]}"
            logger.error("      ❌ Failed to delete patch categories for Ad %s: %s", ad_id, error_msg)
            return {"success": False, "error": error_msg}
    except Exception as e:
        error_msg = f"Patch category delete request failed: {str(e)}"
        logger.error("      ❌ Failed to delete patch categories for Ad %s: %s", ad_id, error_msg)
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
        logger.info("      📥 [Patch Step] Ad %s: Patch exists — fetching current patch fields...", ad_id)
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
                logger.info("      📥 [Patch Step] Ad %s: Copied %d existing patch fields: %s", ad_id, len(patch_body), copied_keys)
                if skipped_fields:
                    logger.info("      ⏭️ [Patch Step] Ad %s: Skipped fields: %s", ad_id, skipped_fields)
            else:
                logger.warning("      ⚠️ [Patch Step] Ad %s: GET patch returned HTTP %d — will PUT with categories only", ad_id, get_resp.status_code)
        except Exception as e:
            logger.warning("      ⚠️ [Patch Step] Ad %s: Failed to GET existing patch: %s — will PUT with categories only", ad_id, e)
    else:
        logger.info("      📥 [Patch Step] Ad %s: No prior patch — will create new patch with categories only", ad_id)

    # Append the new AI-annotated categories
    patch_body["categories"] = categories

    # ── PUT the patch ──
    body = {"patch": patch_body}
    cat_names = [c.get("name", c.get("id", "?")) for c in categories]
    logger.info("      📤 [Patch Step] Ad %s: Sending PUT to %s patch with categories: %s", ad_id, action, cat_names)

    try:
        put_resp = requests.put(url, json=body, headers=headers, timeout=30)
        if put_resp.status_code in (200, 201, 204):
            logger.info("      ✅ [Patch Step] Ad %s: Patch %s successfully (HTTP %d)", ad_id, action, put_resp.status_code)
            return {"success": True, "action": action}
        else:
            error_msg = f"Patch PUT returned HTTP {put_resp.status_code}: {put_resp.text[:200]}"
            logger.error("      ❌ [Patch Step] Ad %s: Failed to %se patch — %s", ad_id, action[:-1], error_msg)
            return {"success": False, "error": error_msg, "action": action}
    except Exception as e:
        error_msg = f"Patch PUT request failed: {str(e)}"
        logger.error("      ❌ [Patch Step] Ad %s: Failed to %se patch — %s", ad_id, action[:-1], error_msg)
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
        logger.info("=" * 80)
        logger.info("🔑 DB CATEGORY UPDATE: OBTAINING BEARER TOKEN")
        logger.info("=" * 80)
        
        try:
            token_info = get_db_update_bearer_token(token_url, c_id, c_secret, c_grant)
            bearer_token = token_info["access_token"]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to obtain bearer token: {str(e)}")
        
        # Derive the ad-patches base URL from the update base URL
        patch_base_url = _derive_patch_base_url(base_url)
        logger.info("   📌 Patch API base URL: %s", patch_base_url)
        
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
                logger.error("   [pre-check] ❌ Ad %s: No annotated categories found", ad_id)
                continue
            
            if unmapped_cats:
                error_msg = f"Skipped — unmapped category: {', '.join(unmapped_cats)}"
                pre_skip_results.append({
                    "ad_id": ad_id,
                    "success": False,
                    "error": error_msg,
                    "categories": [c["name"] for c in categories] + unmapped_cats
                })
                logger.warning("   [pre-check] ⚠️ Ad %s: %s", ad_id, error_msg)
                continue
            
            cat_names = [c["name"] for c in categories]
            prepared_ads.append((ad_id, categories, cat_names))
        
        # Step 2: Process prepared ads with MULTITHREADING
        max_workers = 5
        logger.info("=" * 80)
        logger.info("📝 DB CATEGORY UPDATE: PROCESSING %d ADS (MULTITHREADED, %d workers)", len(prepared_ads), max_workers)
        logger.info("   ⏭️ Pre-skipped: %d ads (validation failures)", len(pre_skip_results))
        logger.info("   🚀 Using %d concurrent workers for ~%dx speedup!", max_workers, max_workers)
        logger.info("=" * 80)
        
        # Thread-safe token holder with lock for refresh
        token_lock = threading.Lock()
        token_holder = {"access_token": bearer_token, "expires_at": token_info["expires_at"]}
        
        def _get_valid_token():
            """Get current bearer token, refreshing if expired (thread-safe)."""
            with token_lock:
                if time.time() >= token_holder["expires_at"]:
                    logger.info("   🔄 Bearer token expiring soon, refreshing...")
                    try:
                        new_info = get_db_update_bearer_token(token_url, c_id, c_secret, c_grant)
                        token_holder["access_token"] = new_info["access_token"]
                        token_holder["expires_at"] = new_info["expires_at"]
                    except Exception as e:
                        logger.warning("   ⚠️ Token refresh failed: %s — continuing with old token", e)
                return token_holder["access_token"]
        
        def _process_single_ad(ad_data, idx, total):
            """
            Worker function: check patch → delete patch categories if needed → PUT update.
            Returns (result_dict, patch_summary_dict_or_None).
            """
            ad_id, categories, cat_names = ad_data
            current_token = _get_valid_token()
            
            logger.info("   [%d/%d] 🔎 Processing Ad %s ...", idx, total, ad_id)
            patch_info = check_ad_patch(patch_base_url, ad_id, current_token)

            patch_deleted = False
            old_patch_categories = []
            patch_summary = None

            if patch_info.get("has_patch") and patch_info.get("has_categories"):
                old_patch_categories = patch_info["categories"]
                old_cat_names = [c.get("name", c.get("id", "?")) for c in old_patch_categories]
                logger.info("      ⚡ Ad %s has PATCHED categories: %s", ad_id, old_cat_names)
                logger.info("      🗑️ Deleting categories from patch before PUT update...")
                
                # Re-fetch token in case it expired during previous calls
                current_token = _get_valid_token()
                delete_result = delete_patch_categories(patch_base_url, ad_id, current_token)
                
                if not delete_result["success"]:
                    error_msg = f"Failed to delete patch categories: {delete_result.get('error', 'Unknown error')}"
                    logger.error("   [%d/%d] ❌ Ad %s: %s", idx, total, ad_id, error_msg)
                    return (
                        {"ad_id": ad_id, "success": False, "error": error_msg, "categories": cat_names},
                        {"ad_id": ad_id, "old_patched_categories": ", ".join(old_cat_names),
                         "new_updated_categories": ", ".join(cat_names),
                         "patch_deleted": "FAILED", "update_success": "No (patch delete failed)"},
                    )
                
                patch_deleted = True
                logger.info("      ✅ Patch categories deleted successfully for Ad %s", ad_id)
            elif patch_info.get("has_patch") and not patch_info.get("has_categories"):
                logger.info("      ℹ️ Ad %s has a patch but NO categories in it — proceeding with direct PUT", ad_id)
            else:
                logger.info("      ℹ️ Ad %s has no patch — proceeding with direct PUT", ad_id)

            # ── PUT UPDATE ──
            current_token = _get_valid_token()
            logger.info("      📤 Sending PUT to update Ad %s with categories: %s", ad_id, cat_names)
            result = update_ad_categories_in_db(base_url, ad_id, categories, current_token)

            if result["success"]:
                logger.info("   [%d/%d] ✅ Ad %s: Updated -> %s", idx, total, ad_id, cat_names)
            else:
                logger.error("   [%d/%d] ❌ Ad %s: %s", idx, total, ad_id, result.get('error', 'Unknown error'))
            
            # ── PATCH STEP: Create or update ad-patch with new categories ──
            patch_action = None       # "created" | "updated" | None
            patch_step_success = False
            patch_step_error = ""
            
            if result["success"]:
                logger.info("      🩹 [Patch Step] Ad %s: Ad update succeeded — now creating/updating patch with categories...", ad_id)
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
                    logger.info("   [%d/%d] 🩹 Ad %s: Patch %s successfully with categories: %s", idx, total, ad_id, patch_action, cat_names)
                else:
                    logger.warning("   [%d/%d] ⚠️ Ad %s: Ad updated OK but patch step failed — %s", idx, total, ad_id, patch_step_error)
            else:
                logger.info("      ⏭️ [Patch Step] Ad %s: Skipping patch step because ad update failed", ad_id)
            
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

            # ── CDC Audit Log: Record old vs new categories to Loki ──
            if config.enable_cdc_audit_log:
                try:
                    row_match = df[df["Ad ID"] == ad_id]
                    breadcrumbs = ["", "", ""]
                    if not row_match.empty:
                        row_data = row_match.iloc[0]
                        for bi, col in enumerate(["Breadcrumb_Top1", "Breadcrumb_Top2", "Breadcrumb_Top3"]):
                            val = row_data.get(col, "")
                            breadcrumbs[bi] = str(val).strip() if pd.notna(val) else ""
                    old_patch_cat_names = [c.get("name", c.get("id", "")) for c in old_patch_categories]
                    cdc_audit_logger.log_category_change(
                        ad_id=ad_id, job_id=temp_id, environment="prod",
                        old_breadcrumbs=breadcrumbs, new_annotated=cat_names,
                        old_patch_categories=old_patch_cat_names,
                        success=result["success"], error=result.get("error", ""),
                        patch_action=patch_action or "", patch_deleted=patch_deleted,
                    )
                except Exception as e:
                    logger.warning("      [Audit] Warning: failed to log audit for Ad %s: %s", ad_id, e)

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
                    logger.error("   ❌ Worker exception for Ad %s: %s", ad_id, e)
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
            logger.info("   📋 Patch summary report stored with ID: %s (%d patched ads)", patch_report_id, len(patched_ads_summary))

        # Summary
        logger.info("=" * 80)
        logger.info("🎉 DB CATEGORY UPDATE COMPLETE (%.1fs with %d workers)", update_elapsed, max_workers)
        logger.info("   Total rows in file: %d", total_rows)
        logger.info("   Rows to update: %d", len(update_rows))
        logger.info("   ✅ Ad updates successful: %d", success_count)
        logger.info("   ❌ Ad updates failed: %d", failed_count)
        logger.info("   ⏭️ Skipped: %d", len(skipped_rows))
        logger.info("   🩹 Patched ads (had categories in patch): %d", len(patched_ads_summary))
        logger.info("   🩹 Patch step — created: %d, updated: %d, failed: %d", patches_created, patches_updated, patches_failed)
        logger.info("   ⚡ Speed: %d concurrent workers", max_workers)
        logger.info("=" * 80)
        
        # Flush any remaining audit log records to Loki
        if config.enable_cdc_audit_log:
            cdc_audit_logger.flush()

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
        logger.error("❌ DB Update error: %s", e)
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
async def download_patch_report(report_id: str, background_tasks: BackgroundTasks):
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
    timestamp = now_ist().strftime("%Y%m%d_%H%M%S")
    filename = f"patch_summary_{report_id}_{timestamp}.xlsx"
    if getattr(config, 'enable_uploads', True):
        report_dir = os.path.join(config.project_root, "uploads")
        os.makedirs(report_dir, exist_ok=True)
        file_path = os.path.join(report_dir, filename)
    else:
        file_path = _temp_path(filename)
        _register_temp_file(file_path)

    df.to_excel(file_path, index=False, engine="openpyxl")
    logger.info("📥 Patch report Excel generated: %s (%d rows)", file_path, len(summary))

    # Upload to B2 in background for archival (keep local — we serve it directly below)
    _upload_and_track(file_path, 'patch-summaries', filename, delete_local=False)

    # Schedule cleanup after response is sent (file already on B2)
    def _cleanup_patch_file(path: str):
        try:
            if os.path.exists(path):
                os.remove(path)
                logger.info("   [CLEANUP] Deleted local patch report: %s", os.path.basename(path))
        except OSError:
            pass
    background_tasks.add_task(_cleanup_patch_file, file_path)

    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    freeze_support()  # Required for Windows multiprocessing
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
