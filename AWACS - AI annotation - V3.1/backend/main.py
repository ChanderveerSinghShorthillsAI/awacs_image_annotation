# FastAPI Backend for AWACS AI Annotation Tool
import os
import sys
import glob
import uuid
import asyncio
import time
import threading
from datetime import datetime
from typing import Dict
from pathlib import Path
from multiprocessing import Process, Manager, freeze_support
import queue

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
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
from ai_tool import web_utils, classification
import ai_module

# Initialize config
load_config()

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
job_progress: Dict[str, dict] = {}  # Track real-time progress per job
audit_jobs: Dict[str, dict] = {}  # Track audit jobs


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
    
    OPTIMIZED: Reduced wait times for faster scraping while maintaining accuracy.
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
    
    driver = None
    try:
        driver = setup_driver(headless=True)
        print(f"🚀 Started scraping {total} ads (OPTIMIZED)")
        
        for idx, row in df.iterrows():
            ad_id = str(row.get("Ad ID", "")).strip()
            if not ad_id:
                continue
                
            url = f"https://www.commercialtrucktrader.com/listing/{ad_id}"
            
            try:
                driver.set_page_load_timeout(10)  # OPTIMIZED: Reduced from 15s to 10s
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
                    print(f"[{processed}/{total}] ⚠️ {ad_id}: Inactive")
                    continue
                
                lower_title = page_title.lower()
                if "no longer available" in lower_title or "listing not found" in lower_title:
                    df.at[idx, "Breadcrumb_Top1"] = "Inactive ad"
                    processed += 1
                    print(f"[{processed}/{total}] ⚠️ {ad_id}: Inactive")
                    continue
                
                # Extract breadcrumbs (All filtering rules preserved)
                try:
                    nav = WebDriverWait(driver, 6).until(  # OPTIMIZED: Reduced from 8s to 6s
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
                
                # Extract images (OPTIMIZED - Faster with same accuracy)
                try:
                    # OPTIMIZED: Reduced wait from 8s to 5s (still sufficient for lazy loading)
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "img.rsImg"))
                    )
                    time.sleep(0.3)  # OPTIMIZED: Reduced from 1s to 0.3s
                    
                    # Try to interact with gallery to load more images (aim for 3 images)
                    try:
                        arrow = driver.find_element(By.CSS_SELECTOR, ".rsArrowRight .rsArrowIcn")
                        action = ActionChains(driver)
                        # OPTIMIZED: Reduced max clicks from 10 to 4 (usually enough for 3 images)
                        for click_count in range(4):
                            try:
                                action.click(arrow).perform()
                                time.sleep(0.15)  # OPTIMIZED: Reduced from 0.4s to 0.15s
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
                    
                    # OPTIMIZED: Reduced final wait from 0.8s to 0.2s
                    time.sleep(0.2)
                    
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
                    print(f"[{processed}/{total}] ✅ {ad_id}: {df.at[idx, 'Breadcrumb_Top1']} | {len(image_urls)} imgs")
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
                        print(f"[{processed}/{total}] ✅ {ad_id}: {df.at[idx, 'Breadcrumb_Top1']} | {len(image_urls)} imgs (fallback)")
                    except:
                        df.at[idx, "Image_URLs"] = ""
                        processed += 1
                        print(f"[{processed}/{total}] ✅ {ad_id}: {df.at[idx, 'Breadcrumb_Top1']} | 0 imgs")
                    
            except Exception as e:
                df.at[idx, "Breadcrumb_Top1"] = "Inactive ad"
                processed += 1
                print(f"[{processed}/{total}] ❌ {ad_id}: Error")
            
            time.sleep(0.15)  # OPTIMIZED: Reduced from 0.3s to 0.15s
            
    except Exception as e:
        print(f"❌ Scraper error: {str(e)}")
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
    
    print(f"✅ Scraping complete: {processed}/{total} ads")
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
    Run parallel AI processing with multiple workers - EXACTLY LIKE main.py
    """
    load_config()
    
    total = len(df)
    print(f"\n🤖 Starting PARALLEL AI classification")
    print(f"   Total Ads: {total}")
    print(f"   Workers: {num_workers}")
    print(f"   Model: {config.gemini_model}")
    print(f"   API Keys: {len(config.gemini_api_keys)}")
    print(f"   📋 [Rules.json] Will be loaded by each worker from: {config.rules_json}")
    print(f"\n   🔧 DUALLY DETECTION SETTINGS:")
    print(f"      🌑 Darth CV2 (OpenCV) Detection: {'✅ ENABLED' if config.enable_darth_cv2_dually else '❌ DISABLED'}")
    if config.enable_darth_cv2_dually:
        print(f"         └─ Threshold: {config.darth_cv2_dually_threshold} (tire-like contours required)")
    print(f"      🔍 Post-Processing LLM Verification: {'✅ ENABLED' if config.enable_dually_llm_verification else '❌ DISABLED'}")
    
    # Initialize progress tracking
    job_progress[job_id] = {
        "total": total,
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
        time.sleep(0.5)  # Stagger to avoid race conditions
    
    print(f"\n   All {num_workers} workers started. Processing...")
    print(f"   (Worker output appears in worker log files in /logs folder)")
    
    results = []
    last_print = 0
    check_count = 0
    
    # Result Collector
    while any(p.is_alive() for p in procs) or not res_q.empty():
        try:
            r = res_q.get(timeout=2)
            if r and r.get("Ad ID"):
                results.append(r)
                ad_id = r.get("Ad ID", "?")
                status = r.get("Status", "?")
                top1 = r.get("Annotated_Top1", "?")
                print(f"   ✅ [{len(results)}/{total}] {ad_id}: {top1} ({status})")
                last_print = len(results)
                
                # Update progress tracking
                if job_id in job_progress:
                    job_progress[job_id]["completed"] = len(results)
        except queue.Empty:
            check_count += 1
            # Every 10 checks (~20 seconds), show status
            if check_count % 10 == 0:
                alive = sum(1 for p in procs if p.is_alive())
                elapsed = int(time.time() - start_time)
                print(f"   ⏳ Waiting... {len(results)}/{total} done | {alive} workers alive | {elapsed}s elapsed")
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
    
    elapsed = int(time.time() - start_time)
    print(f"\n✅ AI classification complete: {len(results)}/{total} ads in {elapsed//60}m {elapsed%60}s")
    
    # Update final progress
    if job_id in job_progress:
        job_progress[job_id]["completed"] = len(results)
        job_progress[job_id]["elapsed"] = elapsed
    
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
    
    # Preserve original input order by merging from input DataFrame
    clean_input_df = df[["Ad ID"]].copy()
    clean_input_df["Ad ID"] = clean_input_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    result_df = pd.merge(clean_input_df, result_df, on="Ad ID", how="inner") 
    
    return result_df[final_columns]


def verify_dually_listings(result_df: pd.DataFrame, job_id: str, yoda_instance):
    """
    OPTIMIZED Post-processing step: LLM verification for Dually false positives.
    
    This function:
    1. Identifies all listings marked with "Dually" in any annotation column
    2. Pre-fetches all images to avoid delays during LLM calls
    3. Makes LLM calls to verify if each one really has dual rear wheels
    4. Removes "Dually" from annotations if the LLM says it's a false positive
    
    Returns: (Updated DataFrame, verification_cost_cents)
    """
    from ai_tool.utils import calculate_cost_cents
    
    print("\n" + "="*60)
    print("🔍 DUALLY VERIFICATION PHASE - Checking for False Positives")
    print("="*60)
    
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
    print(f"   Found {total_dually} listings marked as Dually")
    
    # Update job status for frontend
    if job_id in jobs:
        jobs[job_id]['status'] = JobStatus.VERIFYING_DUALLY
        jobs[job_id]['dually_total'] = total_dually
        jobs[job_id]['dually_verified'] = 0
    
    # STEP 1: Pre-fetch all images first (fast, uses cache)
    print("   📥 Pre-fetching images from cache...")
    prefetched_images = {}
    for idx, row in dually_listings.iterrows():
        ad_id = str(row.get("Ad ID", "")).strip()
        image_urls_str = str(row.get("Image_URLs", "")).strip()
        if image_urls_str:
            image_urls = [url.strip() for url in image_urls_str.split(",") if url.strip()]
            if image_urls:
                img_bytes_list = web_utils.get_images_with_caching(image_urls[:1])
                if img_bytes_list and img_bytes_list[0]:
                    prefetched_images[idx] = (ad_id, img_bytes_list[0])
    
    print(f"   ✅ Pre-fetched {len(prefetched_images)} images")
    
    # Initialize for LLM calls - reuse existing resources efficiently
    m = Manager()
    key_queue = m.Queue()
    for k in config.gemini_api_keys_info:
        key_queue.put(k)
    status_queue = m.Queue()
    
    # Initialize classification trackers once
    classification.initialize_all_trackers()
    
    verified_count = 0
    removed_count = 0
    error_count = 0
    total_cost = 0
    
    # STEP 2: Fast LLM verification loop (no unnecessary delays)
    print("   🔍 Starting fast LLM verification...")
    start_time = time.time()
    
    for idx, row in dually_listings.iterrows():
        ad_id = str(row.get("Ad ID", "")).strip()
        current_num = verified_count + removed_count + error_count + 1
        
        # Update job progress for frontend
        if job_id in jobs:
            jobs[job_id]['dually_verified'] = current_num
        
        # Check if we have pre-fetched image
        if idx not in prefetched_images:
            print(f"   [{current_num}/{total_dually}] ⚠️ {ad_id}: No image available, keeping Dually")
            error_count += 1
            continue
        
        ad_id, img_bytes = prefetched_images[idx]
        
        try:
            # Call LLM verification (Yoda handles rate limiting, no sleep needed)
            is_dually, confidence, in_tok, out_tok = classification.verify_dually_with_llm(
                img_bytes, 
                yoda_instance, 
                key_queue, 
                worker_id=0, 
                ad_id=ad_id, 
                status_queue=status_queue
            )
            
            # Calculate cost for this verification
            cost = calculate_cost_cents(in_tok, out_tok, config.gemini_model)
            total_cost += cost
            
            # ADD verification cost to this listing's Cost_Cents in the dataframe
            current_cost = result_df.at[idx, 'Cost_Cents'] if 'Cost_Cents' in result_df.columns else 0
            try:
                current_cost = float(current_cost) if pd.notna(current_cost) else 0
            except:
                current_cost = 0
            result_df.at[idx, 'Cost_Cents'] = current_cost + cost
            
            if is_dually:
                # LLM confirmed Dually - keep it
                print(f"   [{current_num}/{total_dually}] ✅ {ad_id}: CONFIRMED (+{cost}¢)")
                verified_count += 1
                
                # RECALCULATE STATUS for confirmed dually too (in case it was wrong before)
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
                
                # Normalize and compare
                bc_set = {b.lower() for b in breadcrumbs if b}
                ann_set = {a.lower() for a in annotations if a}
                
                result_df.at[idx, "Status"] = "No change" if bc_set == ann_set else "Require Update"
                
            else:
                # LLM says NOT Dually - remove it from annotations
                print(f"   [{current_num}/{total_dually}] ❌ {ad_id}: FALSE POSITIVE - Removing (+{cost}¢)")
                removed_count += 1
                
                # Remove "Dually" from each annotation column
                for col in annotation_cols:
                    val = str(result_df.at[idx, col]).strip()
                    if "dually" in val.lower():
                        # If it's "Something Dually", remove the Dually part
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
            # Get breadcrumbs for comparison
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
            
            # Normalize and compare (case-insensitive, remove empty)
            bc_set = {b.lower() for b in breadcrumbs if b}
            ann_set = {a.lower() for a in annotations if a}
            
            # Update status based on match
            if bc_set == ann_set:
                result_df.at[idx, "Status"] = "No change"
            else:
                result_df.at[idx, "Status"] = "Require Update"
                    
        except Exception as e:
            print(f"   [{current_num}/{total_dually}] ⚠️ {ad_id}: Error - {str(e)[:30]}")
            error_count += 1
            continue
        
        # NO SLEEP HERE - Yoda handles rate limiting!
    
    elapsed = int(time.time() - start_time)
    
    print("\n" + "-"*60)
    print("🔍 DUALLY VERIFICATION SUMMARY:")
    print(f"   Total checked: {total_dually} | Time: {elapsed}s")
    print(f"   ✅ Confirmed: {verified_count} | ❌ Removed: {removed_count} | ⚠️ Errors: {error_count}")
    print(f"   💰 Cost: {total_cost}¢")
    print("-"*60 + "\n")
    
    # Update job status back to processing for final save
    if job_id in jobs:
        jobs[job_id]['status'] = JobStatus.PROCESSING
        jobs[job_id]['dually_verified'] = total_dually
        jobs[job_id]['dually_removed'] = removed_count
        jobs[job_id]['dually_verification_cost'] = total_cost
    
    # FINAL STEP: Recalculate ALL statuses after verification with proper normalization
    # This ensures status is correct even for listings not verified
    print("\n   🔄 Recalculating all statuses after verification...")
    
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
    
    print(f"   ✅ Status recalculation complete: {status_updated_count} status(es) corrected")
    
    return result_df, total_cost  # Return both dataframe and verification cost


def run_job_pipeline_sync(job_id: str, file_path: str):
    """Main pipeline: Scraping -> Parallel AI Processing (runs synchronously)"""
    job = jobs[job_id]
    run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    try:
        # Load input file
        df = pd.read_excel(file_path, dtype={"Ad ID": str})
        df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # Add required columns
        for col in ["Breadcrumb_Top1", "Breadcrumb_Top2", "Breadcrumb_Top3", "Image_URLs"]:
            if col not in df.columns:
                df[col] = ""
        
        job['total_ads'] = len(df)
        job['status'] = JobStatus.SCRAPING
        
        print(f"\n{'='*60}")
        print(f"JOB {job_id} STARTED")
        print(f"File: {job.get('filename')}")
        print(f"Total Ads: {len(df)}")
        print(f"{'='*60}\n")
        
        # Phase 1: Scraping
        df = scrape_ads_sync(df, job_id)
        
        # Save scraped data
        scraper_output_path = os.path.join(config.scrapper_output_dir, f"Scrapper_{run_ts}.xlsx")
        os.makedirs(config.scrapper_output_dir, exist_ok=True)
        df.to_excel(scraper_output_path, index=False)
        
        job['status'] = JobStatus.PROCESSING
        
        # Phase 2: Parallel AI Processing with 10 workers
        # num_workers = min(10, max(1, len(config.gemini_api_keys) // 2))
        num_workers = 5
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
        
        # Save final output
        output_filename = f"output_annotated_{run_ts}.xlsx"
        output_path = os.path.join(config.output_dir, output_filename)
        os.makedirs(config.output_dir, exist_ok=True)
        result_df.to_excel(output_path, index=False)
        
        job['status'] = JobStatus.COMPLETED
        job['output_file'] = output_path
        job['output_filename'] = output_filename
        
        # Calculate summary - Include BOTH annotation cost AND dually verification cost
        annotation_cost = result_df['Cost_Cents'].sum() if 'Cost_Cents' in result_df.columns else 0
        total_cost = annotation_cost + dually_verification_cost
        job['total_cost'] = total_cost
        job['annotation_cost'] = annotation_cost
        job['dually_verification_cost'] = dually_verification_cost
        
        # Merge session reports
        merge_all_session_reports(run_ts)
        
        print(f"\n{'='*60}")
        print(f"🎉 JOB {job_id} COMPLETE!")
        print(f"Output: {output_filename}")
        print(f"💰 Annotation Cost: {annotation_cost}¢")
        print(f"💰 Dually Verification Cost: {dually_verification_cost}¢")
        print(f"💰 TOTAL COST: {total_cost}¢")
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
        
        job['total_ads'] = len(df)
        job['status'] = JobStatus.PROCESSING
        
        print(f"\n{'='*60}")
        print(f"🔄 RE-ANNOTATION JOB {job_id} STARTED")
        print(f"File: {job.get('filename')}")
        print(f"Total Ads: {len(df)}")
        print(f"Skipping scraping - using existing data")
        print(f"{'='*60}\n")
        
        # Phase 1: Parallel AI Processing (no scraping)
        num_workers = 5
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
        
        # Save final output
        output_filename = f"output_reannotated_{run_ts}.xlsx"
        output_path = os.path.join(config.output_dir, output_filename)
        os.makedirs(config.output_dir, exist_ok=True)
        result_df.to_excel(output_path, index=False)
        
        job['status'] = JobStatus.COMPLETED
        job['output_file'] = output_path
        job['output_filename'] = output_filename
        
        # Calculate summary
        annotation_cost = result_df['Cost_Cents'].sum() if 'Cost_Cents' in result_df.columns else 0
        total_cost = annotation_cost + dually_verification_cost
        job['total_cost'] = total_cost
        job['annotation_cost'] = annotation_cost
        job['dually_verification_cost'] = dually_verification_cost
        
        # Merge session reports
        merge_all_session_reports(run_ts)
        
        print(f"\n{'='*60}")
        print(f"🎉 RE-ANNOTATION JOB {job_id} COMPLETE!")
        print(f"Output: {output_filename}")
        print(f"💰 Annotation Cost: {annotation_cost}¢")
        print(f"💰 Dually Verification Cost: {dually_verification_cost}¢")
        print(f"💰 TOTAL COST: {total_cost}¢")
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
    upload_dir = os.path.join(config.project_root, "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, f"{job_id}_{file.filename}")
    
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
    upload_dir = os.path.join(config.project_root, "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, f"{job_id}_reannotate_{file.filename}")
    
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
        "elapsed": progress.get('elapsed', 0)
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


@app.get("/api/config")
async def get_config():
    """Get configuration info"""
    return {
        "api_keys_count": len(config.gemini_api_keys),
        "model": config.gemini_model,
        "max_images_per_ad": config.max_images,
        "rate_limit_rpm": config.rate_limit_rpm
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
    
    audit_df = pd.DataFrame(audit_results)
    final_output = pd.merge(merged, audit_df[["Ad ID", "Feedback Status"]], on="Ad ID", how="left")
    
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
    else:
        hall_of_shame = pd.DataFrame([{"Message": "No Rejections! Perfect accuracy!"}])
    
    # Save Audit Report
    audit_dir = os.path.join(config.project_root, "Audit Reports")
    os.makedirs(audit_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_filename = f"Audit_Report_{timestamp}.xlsx"
    report_path = os.path.join(audit_dir, report_filename)
    
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
async def download_audit_report(audit_id: str):
    """Download the audit report Excel file"""
    if audit_id not in audit_jobs:
        raise HTTPException(status_code=404, detail="Audit report not found")
    
    audit = audit_jobs[audit_id]
    report_path = audit.get('report_path')
    
    if not report_path or not os.path.exists(report_path):
        raise HTTPException(status_code=404, detail="Audit report file not found")
    
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


if __name__ == "__main__":
    freeze_support()  # Required for Windows multiprocessing
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
