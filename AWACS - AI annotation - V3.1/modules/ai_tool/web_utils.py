import os
import hashlib
import requests
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager

# Import the centrally managed config object
from .config_loader import config
from .utils import log_msg

def setup_driver(headless=False, worker_id=None):
    """
    Initializes and returns a Selenium WebDriver instance using automatically managed ChromeDriver.
    
    Args:
        headless: Whether to run browser in headless mode
        worker_id: Optional worker ID for multiprocessing (creates isolated Chrome profile)
    """
    chrome_options = Options()
    
    # --- CRITICAL SPEED FIX ---
    # 'eager' = DOM access is ready, but images/scripts might still be loading.
    # This makes driver.get() return 3x-5x faster for inactive/redirected pages.
    chrome_options.page_load_strategy = 'eager' 
    # --------------------------

    if headless:
        chrome_options.add_argument("--headless=new")
    
    # MULTIPROCESSING FIX: Isolated Chrome profile per worker
    # Prevents profile collision when multiple browsers run simultaneously
    if worker_id is not None:
        import tempfile
        import time
        # Add timestamp to ensure truly unique profiles even on restarts
        profile_dir = os.path.join(tempfile.gettempdir(), f"chrome-worker-{worker_id}-{int(time.time())}")
        chrome_options.add_argument(f"--user-data-dir={profile_dir}")
        # CRITICAL: Let Chrome choose its own debugging port to avoid conflicts
        chrome_options.add_argument("--remote-debugging-port=0")
    
    # Standard options for a cleaner browsing experience and stability
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1280,900")
    chrome_options.add_argument("--disable-webgl")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--silent")
    
    # RAM Optimization: Disable unnecessary features
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-background-networking")
    
    # Multiprocessing stability flags
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_experimental_option("prefs", {"profile.default_content_setting_values.notifications": 2})
    
    # Use webdriver-manager to automatically download and manage ChromeDriver
    # This eliminates the need for chromedriver.exe in the repository
    service = Service(ChromeDriverManager().install())
    # Suppress driver logs
    try:
        service.log_output = open(os.devnull, "w")
    except Exception:
        pass
    
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def get_all_image_urls(driver, ad_id, timeout=10):
    """Fetches all high-quality image URLs for a given ad ID. Tries to get 3 images if available."""
    url = f"https://www.commercialtrucktrader.com/listing/{ad_id}"
    try:
        driver.get(url)
        # Wait for images to load
        WebDriverWait(driver, timeout).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "img.rsImg")))
        time.sleep(0.3)  # OPTIMIZED: 1s -> 0.3s
        
        # Try to interact with gallery to load more images (aim for 3 images)
        try:
            arrow = driver.find_element(By.CSS_SELECTOR, ".rsArrowRight .rsArrowIcn")
            action = ActionChains(driver)
            # OPTIMIZED: Reduced max clicks from 10 to 4
            for click_count in range(4):
                try:
                    action.click(arrow).perform()
                    time.sleep(0.15)  # OPTIMIZED: 0.4s -> 0.15s
                    # Check how many images we have now
                    current_imgs = driver.find_elements(By.CSS_SELECTOR, "img.rsImg")
                    current_urls = []  
                    for img in current_imgs:
                        src = img.get_attribute("src") or img.get_attribute("data-src") or img.get_attribute("data-lazy-src")
                        if src and "placeholder" not in src.lower() and src not in current_urls:
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
        urls = []
        for im in imgs:
            # Try multiple attributes for lazy-loaded images
            src = im.get_attribute("src") or im.get_attribute("data-src") or im.get_attribute("data-lazy-src")
            # Filter out placeholders and duplicates
            if src and "placeholder" not in src.lower() and src not in urls:
                if src.startswith("http") or src.startswith("//"):
                    urls.append(src)
                
        return urls[:config.max_images]
    except Exception:
        try:
            # Fallback: Try to get just the main image if gallery fails
            main_img = driver.find_element(By.CSS_SELECTOR, "img.rsImg.rsMainSlideImage")
            src = main_img.get_attribute("src")
            return [src] if src and "placeholder" not in src.lower() else []
        except Exception:
            return []

def get_images_with_caching(image_urls, retry_count=0, timeout=5):
    """
    Fetches images directly from URLs as bytes without caching to disk.
    This avoids storage issues and follows Gemini's recommended pattern.
    Images are fetched directly into memory and passed to the AI model.
    
    OPTIMIZED: Uses ThreadPoolExecutor for parallel downloads (I/O-bound).
    
    Args:
        image_urls: List of image URLs to fetch
        retry_count: Number of retry attempts (0 = no retry, 1 = one retry, etc.)
        timeout: Request timeout in seconds (increased on retry)
    """
    from concurrent.futures import ThreadPoolExecutor
    import time
    
    def fetch_single_image(url, attempt_timeout):
        """Fetch a single image with retry logic"""
        max_attempts = retry_count + 1
        url_short = url[:60] + "..." if len(url) > 60 else url  # Truncate long URLs for display
        
        for attempt in range(max_attempts):
            try:
                # Create a session for this request (thread-safe)
                with requests.Session() as session:
                    # Increase timeout on retry attempts
                    current_timeout = attempt_timeout + (attempt * 2)  # Add 2s per retry
                    if attempt > 0:
                        print(f"   🔄 Retry attempt {attempt + 1}/{max_attempts} for image: {url_short} (timeout: {current_timeout}s)")
                    r = session.get(url, timeout=current_timeout, stream=True)
                    if r.status_code == 200:
                        content = r.content
                        # Validate that we got actual image data (not empty or too small)
                        if content and len(content) > 100:  # At least 100 bytes
                            if attempt > 0:
                                print(f"   ✅ Image retry successful: {url_short} ({len(content)} bytes)")
                            return content
                        else:
                            if attempt < max_attempts - 1:
                                print(f"   ⚠️ Image too small ({len(content)} bytes), retrying: {url_short}")
            except requests.exceptions.Timeout:
                if attempt < max_attempts - 1:
                    print(f"   ⏱️ Timeout on attempt {attempt + 1}/{max_attempts}, retrying: {url_short}")
                    time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    print(f"   ❌ Timeout after {max_attempts} attempts: {url_short}")
            except Exception as e:
                # Only log on final attempt to avoid spam
                if attempt == max_attempts - 1:
                    print(f"   ❌ Failed after {max_attempts} attempts: {url_short} - {str(e)[:50]}")
                    log_msg(f"Error downloading {url} after {max_attempts} attempts: {e}", -1)
                elif attempt < max_attempts - 1:
                    print(f"   ⚠️ Error on attempt {attempt + 1}/{max_attempts}, retrying: {url_short} - {str(e)[:50]}")
                    time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    continue
        return None
    
    if not image_urls:
        return []
    
    # Fetch all images in parallel (max 3-5 images per ad typically)
    img_bytes_list = []
    with ThreadPoolExecutor(max_workers=min(len(image_urls), 5)) as executor:
        results = executor.map(lambda url: fetch_single_image(url, timeout), image_urls)
        img_bytes_list = [img for img in results if img is not None]
    
    return img_bytes_list