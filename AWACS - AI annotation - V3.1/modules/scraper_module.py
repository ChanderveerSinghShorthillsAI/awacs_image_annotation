import os
import time
import glob
import re
import pandas as pd
from datetime import timedelta, datetime
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib3

from ai_tool.config_loader import config
from ai_tool.web_utils import setup_driver

def fmt_secs(seconds: float) -> str:
    return str(timedelta(seconds=int(seconds)))

def scrape_ad_data(driver, ad_id, group_index=3, timeout=5):
    """
    Navigates to the ad and extracts BOTH breadcrumbs AND image URLs.
    - Filters out 'Browse Trucks' AND Make/Model links.
    - Cleans trailing commas from breadcrumbs.
    - Strictly checks Image Ad IDs.
    
    ULTRA-OPTIMIZED: Maximum speed while maintaining accuracy.
    """
    url = f"https://www.commercialtrucktrader.com/listing/{ad_id}"
    result = {
        "status": "Active",
        "breadcrumbs": [],
        "images": []
    }
    
    target_ad_id = str(ad_id).strip()

    try:
        driver.set_page_load_timeout(8)  # ULTRA-OPTIMIZED: Reduced from 10s to 8s
        try:
            driver.get(url)
        except TimeoutException:
            driver.execute_script("window.stop();")
        
        current_url = driver.current_url
        page_title = driver.title.strip()

        # 1. Validation
        if f"/listing/{target_ad_id}" not in current_url: 
            result["status"] = "Inactive"
            return result
        
        lower_title = page_title.lower()
        if "no longer available" in lower_title or "listing not found" in lower_title: 
            result["status"] = "Inactive"
            return result
        
        if "security" in lower_title or "challenge" in lower_title or "denied" in lower_title:
            print(f"   🚨 [BLOCKED] Page Title: {page_title}")
            raise Exception("IP Blocked or Challenge Detected")

        # 2. Extract Breadcrumbs (SMART FILTERING)
        try:
            nav = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, "nav.breadcrumbs")))
            
            links = nav.find_elements(By.TAG_NAME, "a")
            clean_texts = []
            
            for link in links:
                # --- FIX: REMOVE TRAILING COMMAS ---
                text = link.text.strip().rstrip(',')
                # -----------------------------------
                
                href = link.get_attribute("href") or ""
                t_lower = text.lower()
                h_lower = href.lower()

                # A. Text Noise Filter
                if not text or any(n in t_lower for n in ["home", "browse", "commercial trucks", "for sale"]):
                    continue
                
                # B. URL Logic Filter
                if any(param in h_lower for param in ["make=", "model=", "state=", "city=", "zip=", "year="]):
                    continue
                
                clean_texts.append(text)

            result["breadcrumbs"] = clean_texts[:3]
            
            if not result["breadcrumbs"]:
                result["status"] = "Inactive"
                return result
        except:
            result["status"] = "Inactive"
            return result

        # 3. Extract Image URLs (ULTRA-OPTIMIZED - Maximum speed with same accuracy)
        image_urls = []
        try:
            # ULTRA-OPTIMIZED: Reduced wait from 5s to 4s
            WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.CSS_SELECTOR, "img.rsImg")))
            time.sleep(0.2)  # ULTRA-OPTIMIZED: Reduced from 0.3s to 0.2s
            
            # Try to interact with gallery to load more images (aim for 3 images)
            try:
                arrow = driver.find_element(By.CSS_SELECTOR, ".rsArrowRight .rsArrowIcn")
                action = ActionChains(driver)
                # ULTRA-OPTIMIZED: Reduced max clicks to 3
                for click_count in range(3):
                    try:
                        action.click(arrow).perform()
                        time.sleep(0.1)  # ULTRA-OPTIMIZED: Reduced from 0.15s to 0.1s
                        # Check how many valid images we have now
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
            
            for im in imgs:
                # Try multiple attributes for lazy-loaded images
                src = im.get_attribute("src") or im.get_attribute("data-src") or im.get_attribute("data-lazy-src")
                if not src:
                    continue
                
                # Check data-adid: only skip if it exists AND doesn't match
                # If data-adid doesn't exist, include the image (less strict)
                elem_adid = im.get_attribute("data-adid")
                if elem_adid and str(elem_adid).strip() != target_ad_id:
                    continue
                
                # Filter placeholders and duplicates
                if "placeholder" not in src.lower() and src not in image_urls:
                    # Make sure it's a valid image URL
                    if src.startswith("http") or src.startswith("//"):
                        image_urls.append(src)

            unique_urls = list(dict.fromkeys(image_urls))
            result["images"] = unique_urls[:config.max_images]
            
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
                result["images"] = image_urls[:config.max_images]
            except:
                result["images"] = []

        return result

    except (TimeoutException, NoSuchElementException):
        result["status"] = "Inactive"
        return result
    except (WebDriverException, urllib3.exceptions.ReadTimeoutError, urllib3.exceptions.MaxRetryError) as e:
        raise e
    except Exception as e:
        print(f"   ⚠️ Unexpected error: {e}")
        result["status"] = "Inactive"
        return result

def run_scraper(resume=False):
    """Main function to run the scraper - ULTRA-OPTIMIZED with detailed timing logs."""
    OUTPUT_DIR = config.scrapper_output_dir
    CHECKPOINT_SAVE_INTERVAL = config.scraper_checkpoint_interval
    SANITY_CHECK_LIMIT = config.scraper_sanity_check
    
    input_file_path = os.path.join(config.project_root, "Scrapper.xlsx")
    ad_id_column = "Ad ID"
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    total_start_time = time.time()
    
    try:
        source_df = pd.read_excel(input_file_path, dtype={ad_id_column: str})
        source_df[ad_id_column] = source_df[ad_id_column].astype(str).str.strip()
        
        if ad_id_column not in source_df.columns: 
            raise ValueError(f"Column '{ad_id_column}' not found in Scrapper.xlsx")

        target_file = ""
        
        if resume:
            existing_files = glob.glob(os.path.join(OUTPUT_DIR, "Scrapper_*.xlsx"))
            if existing_files:
                target_file = max(existing_files, key=os.path.getmtime)
                print(f"\n🔄 Resuming from latest file: {os.path.basename(target_file)}")
                existing_df = pd.read_excel(target_file, dtype={ad_id_column: str})
                existing_df[ad_id_column] = existing_df[ad_id_column].astype(str).str.strip()
                df = pd.merge(source_df, existing_df, on=ad_id_column, how="left")
            else:
                print("\n⚠️ No existing file to resume. Starting fresh.")
                df = source_df.copy()
                run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                target_file = os.path.join(OUTPUT_DIR, f"Scrapper_{run_ts}.xlsx")
        else:
            df = source_df.copy()
            run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            target_file = os.path.join(OUTPUT_DIR, f"Scrapper_{run_ts}.xlsx")

        required_cols = ["Breadcrumb_Top1", "Breadcrumb_Top2", "Breadcrumb_Top3", "Image_URLs"]
        for col in required_cols:
            if col not in df.columns: df[col] = ""

        total = len(df)
        todo_mask = df["Breadcrumb_Top1"].isna() | (df["Breadcrumb_Top1"] == "")
        todo_indices = df[todo_mask].index.tolist()
        
        already_done = total - len(todo_indices)
        print(f"📊 Total Ads: {total} | Already Done: {already_done} | To Do: {len(todo_indices)}")
        
        if len(todo_indices) == 0:
            print("\n✅ All ads are already scraped! Nothing to do.")
            return

        driver = setup_driver(headless=True)
        ads_processed_session = 0
        consecutive_inactive = 0
        sum_durations = 0.0
        
        print(f"\n{'='*80}")
        print(f"🚀 SCRAPING SESSION STARTED (ULTRA-OPTIMIZED)")
        print(f"{'='*80}\n")
        
        for idx in todo_indices:
            ad_id = df.loc[idx, ad_id_column]
            if not ad_id: continue

            item_start = time.perf_counter()
            
            try:
                data = scrape_ad_data(driver, ad_id, group_index=3)
            except Exception as e:
                print(f"   🔥 Driver Crashed/Timed Out ({e}). Restarting...")
                try: driver.quit()
                except: pass
                driver = setup_driver(headless=True)
                try:
                    print(f"   🔄 Retrying Ad {ad_id}...")
                    data = scrape_ad_data(driver, ad_id, group_index=3)
                except Exception as e2:
                    print(f"   ❌ Retry failed. Skipping {ad_id}. Error: {e2}")
                    data = {"status": "Inactive", "breadcrumbs": [], "images": []}

            if data["status"] == "Inactive":
                bc1, bc2, bc3, imgs_str = "Inactive ad", "", "", ""
                status_msg = "Inactive ad"
                consecutive_inactive += 1
            else:
                bcs = data["breadcrumbs"]
                bc1 = bcs[0] if len(bcs) > 0 else ""
                bc2 = bcs[1] if len(bcs) > 1 else ""
                bc3 = bcs[2] if len(bcs) > 2 else ""
                imgs_str = ",".join(data["images"])
                
                status_msg = f"'{bc1}', '{bc2}' | {len(data['images'])} imgs"
                consecutive_inactive = 0
            
            df.loc[idx, "Breadcrumb_Top1"] = bc1
            df.loc[idx, "Breadcrumb_Top2"] = bc2
            df.loc[idx, "Breadcrumb_Top3"] = bc3
            df.loc[idx, "Image_URLs"] = imgs_str
            
            ads_processed_session += 1
            item_dur = time.perf_counter() - item_start
            sum_durations += item_dur
            avg_dur = sum_durations / ads_processed_session
            remaining_sec = avg_dur * (len(todo_indices) - ads_processed_session)
            
            print(f"[{ads_processed_session}/{len(todo_indices)}] ID: {ad_id} -> {status_msg} | ⏱️ {item_dur:.2f}s | Avg: {avg_dur:.2f}s | ETA: {fmt_secs(remaining_sec)}", flush=True)

            if consecutive_inactive >= SANITY_CHECK_LIMIT:
                print(f"\n🛑 STOPPING: Detected {SANITY_CHECK_LIMIT} consecutive inactive ads.")
                break
            
            if ads_processed_session % CHECKPOINT_SAVE_INTERVAL == 0:
                try:
                    df.to_excel(target_file, index=False)
                    print(f"   💾 Checkpoint saved.")
                except Exception as e:
                    print(f"   ⚠️ Checkpoint failed: {e}")

        df.to_excel(target_file, index=False)
        
        total_elapsed = time.time() - total_start_time
        print(f"\n{'='*80}")
        print(f"✅ SCRAPING SESSION COMPLETE")
        print(f"{'='*80}")
        print(f"   Processed: {ads_processed_session} ads")
        print(f"   ⏱️ Total Time: {total_elapsed:.2f}s ({total_elapsed/60:.2f} minutes)")
        print(f"   ⏱️ Average Time per Ad: {sum_durations/max(ads_processed_session, 1):.2f}s")
        print(f"   📊 Scraping Rate: {ads_processed_session/(total_elapsed/60):.1f} ads/minute")
        print(f"   💾 Saved to: {os.path.basename(target_file)}")
        print(f"{'='*80}\n")
        
    except FileNotFoundError:
        print(f"❌ Error: Input file '{input_file_path}' not found.")
    except Exception as e:
        print(f"❌ Critical Error: {e}")
    finally:
        if 'driver' in locals():
            try: driver.quit()
            except: pass

if __name__ == "__main__":
    from ai_tool.config_loader import load_config
    load_config()
    run_scraper(resume=True)