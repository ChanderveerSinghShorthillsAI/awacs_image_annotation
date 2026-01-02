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

def setup_driver(headless=False):
    """Initializes and returns a Selenium WebDriver instance using automatically managed ChromeDriver."""
    chrome_options = Options()
    
    # --- CRITICAL SPEED FIX ---
    # 'eager' = DOM access is ready, but images/scripts might still be loading.
    # This makes driver.get() return 3x-5x faster for inactive/redirected pages.
    chrome_options.page_load_strategy = 'eager' 
    # --------------------------

    if headless:
        chrome_options.add_argument("--headless=new")
    
    # Standard options for a cleaner browsing experience and stability
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1280,900")
    chrome_options.add_argument("--disable-webgl")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--silent")
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

def get_images_with_caching(image_urls):
    """
    Downloads images from a list of URLs, utilizing a local cache.
    """
    img_bytes_list = []
    
    # Ensure cache directory exists
    os.makedirs(config.image_cache_dir, exist_ok=True)
    
    # Use a session for faster connection reuse (Keep-Alive)
    with requests.Session() as session:
        for url in image_urls:
            try:
                # Create a safe filename hash
                file_hash = hashlib.md5(url.encode()).hexdigest()
                cache_path = os.path.join(config.image_cache_dir, f"{file_hash}.jpg")
                
                if os.path.exists(cache_path):
                    with open(cache_path, 'rb') as f:
                        img_bytes_list.append(f.read())
                else:
                    # Download with a short timeout using the session
                    r = session.get(url, timeout=5)  # OPTIMIZED: 8s -> 5s
                    if r.status_code == 200:
                        content = r.content
                        img_bytes_list.append(content)
                        with open(cache_path, 'wb') as f:
                            f.write(content)
                            
            except Exception as e:
                # Log silently to file
                log_msg(f"Error downloading {url}: {e}", -1)
            
    return img_bytes_list