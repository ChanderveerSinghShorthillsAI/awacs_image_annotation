# Parallel Scraper Worker Module
# This file contains the multiprocessing worker function for parallel scraping.
# Required to be in a separate module for Windows multiprocessing compatibility.
# 
# FIXES IMPLEMENTED (from Senior's analysis):
# 1. Per-worker Chrome profiles (via setup_driver worker_id)
# 2. Increased page load timeout (25s)
# 3. data: URL detection (prevents false inactive detection)
# 4. Retry logic (2 attempts max)
# 5. Better error handling

import os
import sys
import time
import random
from pathlib import Path


def scrape_process_worker(worker_id: int, job_queue, result_queue, max_images: int):
    """
    MULTIPROCESSING worker for parallel scraping.
    Implements all fixes from Senior's analysis.
    """
    # Setup path for imports (required for spawned processes on Windows)
    PROJECT_ROOT = Path(__file__).parent.parent
    MODULES_PATH = str(PROJECT_ROOT / "modules")
    if MODULES_PATH not in sys.path:
        sys.path.insert(0, MODULES_PATH)
    
    from ai_tool.web_utils import setup_driver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException
    from selenium.webdriver.common.action_chains import ActionChains
    
    driver = None
    processed = 0
    worker_start = time.time()
    
    print(f"   🚀 [Worker-{worker_id}] Process started (PID: {os.getpid()})", flush=True)
    
    try:
        # FIX 1: Per-worker Chrome profile (prevents profile collision)
        driver = setup_driver(headless=True, worker_id=worker_id)
        print(f"   ✅ [Worker-{worker_id}] Browser launched with isolated profile", flush=True)
        
        while True:
            try:
                # Get job from queue (non-blocking with timeout)
                job = job_queue.get(timeout=3)
            except:
                # No more jobs
                break
            
            if job is None:  # Poison pill
                break
                
            idx, ad_id = job
            listing_start = time.time()
            
            result = {
                "idx": idx,
                "ad_id": ad_id,
                "Breadcrumb_Top1": "",
                "Breadcrumb_Top2": "",
                "Breadcrumb_Top3": "",
                "Image_URLs": ""
            }
            
            url = f"https://www.commercialtrucktrader.com/listing/{ad_id}"
            
            try:
                # FIX 2 & 4: Increased timeout (25s) + Retry logic (2 attempts)
                driver.set_page_load_timeout(25)
                
                page_loaded = False
                for attempt in range(2):
                    try:
                        driver.get(url)
                        
                        # Wait for page to actually load (not data: URL)
                        WebDriverWait(driver, 10).until(
                            lambda d: d.current_url.startswith("https://")
                        )
                        page_loaded = True
                        break
                    except TimeoutException:
                        driver.execute_script("window.stop();")
                        if attempt == 0:
                            print(f"   [W{worker_id}] ⚠️ {ad_id}: Attempt 1 timeout, retrying...", flush=True)
                            time.sleep(2)
                        else:
                            print(f"   [W{worker_id}] ❌ {ad_id}: Attempt 2 timeout", flush=True)
                    except WebDriverException as we:
                        if attempt == 0:
                            print(f"   [W{worker_id}] ⚠️ {ad_id}: Network error, retrying...", flush=True)
                            time.sleep(2)
                        else:
                            raise
                
                current_url = driver.current_url
                page_title = driver.title.strip()
                
                # FIX 3: Detect data: URL (page never loaded)
                if current_url.startswith("data:") or current_url == "" or not page_loaded:
                    print(f"   [W{worker_id}] ❌ {ad_id}: Page never loaded (data: URL)", flush=True)
                    result["Breadcrumb_Top1"] = "Inactive ad"
                    result_queue.put(result)
                    processed += 1
                    continue
                
                # Check if inactive (redirected away)
                if f"/listing/{ad_id}" not in current_url:
                    result["Breadcrumb_Top1"] = "Inactive ad"
                    listing_time = time.time() - listing_start
                    print(f"   [W{worker_id}] ⚠️ {ad_id}: Redirected (inactive) | {listing_time:.1f}s", flush=True)
                    result_queue.put(result)
                    processed += 1
                    continue
                
                lower_title = page_title.lower()
                if "no longer available" in lower_title or "listing not found" in lower_title:
                    result["Breadcrumb_Top1"] = "Inactive ad"
                    listing_time = time.time() - listing_start
                    print(f"   [W{worker_id}] ⚠️ {ad_id}: Not found in title | {listing_time:.1f}s", flush=True)
                    result_queue.put(result)
                    processed += 1
                    continue
                
                # Extract breadcrumbs (Original logic)
                try:
                    nav = WebDriverWait(driver, 5).until(
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
                        result["Breadcrumb_Top1"] = breadcrumbs[0] if len(breadcrumbs) > 0 else ""
                        result["Breadcrumb_Top2"] = breadcrumbs[1] if len(breadcrumbs) > 1 else ""
                        result["Breadcrumb_Top3"] = breadcrumbs[2] if len(breadcrumbs) > 2 else ""
                    else:
                        result["Breadcrumb_Top1"] = "Inactive ad"
                        
                except Exception as e:
                    result["Breadcrumb_Top1"] = "Inactive ad"
                    print(f"   [W{worker_id}] ⚠️ {ad_id}: Breadcrumb error: {str(e)[:30]}", flush=True)
                
                # Extract images (MAXIMUM timing for 100% 3-image reliability)
                try:
                    # Increased wait from 6s to 8s for heavy load (5 workers)
                    WebDriverWait(driver, 8).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "img.rsImg"))
                    )
                    # Increased from 0.4s to 0.6s for lazy-loaded images
                    time.sleep(0.6)
                    
                    # Try to interact with gallery to load more images (increased clicks)
                    try:
                        arrow = driver.find_element(By.CSS_SELECTOR, ".rsArrowRight .rsArrowIcn")
                        action = ActionChains(driver)
                        # Increased from 3 to 5 clicks to ensure all images appear
                        for click_count in range(5):
                            try:
                                action.click(arrow).perform()
                                # Increased from 0.2s to 0.3s to ensure images load after each click
                                time.sleep(0.3)
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
                        pass
                    
                    # Increased from 0.3s to 0.5s to ensure final images are fully loaded
                    time.sleep(0.5)
                    
                    imgs = driver.find_elements(By.CSS_SELECTOR, "img.rsImg")
                    image_urls = []
                    
                    for im in imgs:
                        src = im.get_attribute("src") or im.get_attribute("data-src") or im.get_attribute("data-lazy-src")
                        if not src:
                            continue
                        
                        elem_adid = im.get_attribute("data-adid")
                        if elem_adid and str(elem_adid).strip() != ad_id:
                            continue
                        
                        if "placeholder" not in src.lower() and src not in image_urls:
                            if src.startswith("http") or src.startswith("//"):
                                image_urls.append(src)
                    
                    result["Image_URLs"] = ",".join(image_urls[:max_images])
                    
                except Exception:
                    # Fallback
                    try:
                        imgs = driver.find_elements(By.CSS_SELECTOR, "img.rsImg")
                        image_urls = []
                        for im in imgs[:10]:
                            src = im.get_attribute("src") or im.get_attribute("data-src") or im.get_attribute("data-lazy-src")
                            if src and "placeholder" not in src.lower() and src not in image_urls:
                                if src.startswith("http") or src.startswith("//"):
                                    image_urls.append(src)
                                    if len(image_urls) >= max_images:
                                        break
                        result["Image_URLs"] = ",".join(image_urls[:max_images])
                    except:
                        result["Image_URLs"] = ""
                
                # ==================== IMAGE RETRY MECHANISM ====================
                # If active ad has 0 images, retry with longer wait times
                if result["Breadcrumb_Top1"] and result["Breadcrumb_Top1"] != "Inactive ad":
                    if not result["Image_URLs"]:  # 0 images for active ad
                        print(f"   [W{worker_id}] 🔄 {ad_id}: 0 images - retrying with longer wait (2.5s)...", flush=True)
                        time.sleep(2.5)  # Wait 2.5 seconds for images to fully load
                        
                        # Re-extract images with longer timeouts
                        try:
                            # Longer wait for image elements (12s instead of 8s)
                            WebDriverWait(driver, 12).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "img.rsImg"))
                            )
                            time.sleep(0.8)  # Longer wait after detection
                            
                            # Try more gallery clicks with longer delays
                            try:
                                arrow = driver.find_element(By.CSS_SELECTOR, ".rsArrowRight .rsArrowIcn")
                                action = ActionChains(driver)
                                for click_count in range(7):  # More clicks on retry
                                    try:
                                        action.click(arrow).perform()
                                        time.sleep(0.4)  # Longer delay between clicks
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
                                pass
                            
                            time.sleep(0.6)  # Final wait before extraction
                            
                            imgs = driver.find_elements(By.CSS_SELECTOR, "img.rsImg")
                            retry_image_urls = []
                            
                            for im in imgs:
                                src = im.get_attribute("src") or im.get_attribute("data-src") or im.get_attribute("data-lazy-src")
                                if not src:
                                    continue
                                elem_adid = im.get_attribute("data-adid")
                                if elem_adid and str(elem_adid).strip() != ad_id:
                                    continue
                                if "placeholder" not in src.lower() and src not in retry_image_urls:
                                    if src.startswith("http") or src.startswith("//"):
                                        retry_image_urls.append(src)
                            
                            if retry_image_urls:
                                result["Image_URLs"] = ",".join(retry_image_urls[:max_images])
                                print(f"   [W{worker_id}] ✅ {ad_id}: Retry successful! Got {len(retry_image_urls)} images", flush=True)
                            else:
                                print(f"   [W{worker_id}] ⚠️ {ad_id}: Retry failed - still 0 images", flush=True)
                        except Exception as retry_err:
                            print(f"   [W{worker_id}] ⚠️ {ad_id}: Retry error: {str(retry_err)[:30]}", flush=True)
                # ===============================================================
                
                listing_time = time.time() - listing_start
                img_count = len(result["Image_URLs"].split(",")) if result["Image_URLs"] else 0
                bc1 = result['Breadcrumb_Top1']
                print(f"   [W{worker_id}] ✅ {ad_id}: {bc1} | {img_count} imgs | {listing_time:.1f}s", flush=True)
                
            except Exception as e:
                result["Breadcrumb_Top1"] = "Inactive ad"
                listing_time = time.time() - listing_start
                err_short = str(e)[:40]
                print(f"   [W{worker_id}] ❌ {ad_id}: {err_short} | {listing_time:.1f}s", flush=True)
            
            # Staggered random delay for IP ban prevention (0.2-0.5s)
            # Creates natural, human-like scraping pattern
            delay = random.uniform(0.2, 0.5)
            time.sleep(delay)
            
            # Send result
            result_queue.put(result)
            processed += 1
            
    except Exception as e:
        print(f"   [W{worker_id}] 💥 FATAL: {str(e)}", flush=True)
    finally:
        # Always cleanup driver to free RAM
        if driver:
            try:
                driver.quit()
            except:
                pass
    
    worker_elapsed = time.time() - worker_start
    avg_time = worker_elapsed / max(processed, 1)
    print(f"   🏁 [W{worker_id}] Done: {processed} ads | {worker_elapsed:.1f}s | avg {avg_time:.1f}s/ad", flush=True)
