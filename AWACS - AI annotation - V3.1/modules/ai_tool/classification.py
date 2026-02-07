import os
import base64
import re
import json
import time
import random
import contextlib
from multiprocessing import Queue
import queue

# Try importing OpenCV for Mosaic
try:
    import cv2
    import numpy as np
    OPENCV_AVAILABLE = True
except ImportError:
    OPENCV_AVAILABLE = False

# This silences the initial import.
with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
    import google.generativeai as genai

from .config_loader import config
from .utils import log_msg

_current_key_info = None
_key_usage_stats = {}
_token_usage_stats = {'total_tokens': 0, 'api_calls': 0}
_current_key_info = None

# Tracks how many times we have resurrected the key pool
_phoenix_cycle_count = 0

class AllKeysExhaustedError(Exception):
    pass

class NoKeysAvailableError(Exception):
    pass

def initialize_all_trackers(worker_key_pool: list = None):
    global _key_usage_stats, _token_usage_stats, _current_key_info, _phoenix_cycle_count
    if worker_key_pool is None:
        worker_key_pool = config.gemini_api_keys_info
    _key_usage_stats = {}
    _token_usage_stats = {'total_tokens': 0, 'api_calls': 0}
    _current_key_info = None
    _phoenix_cycle_count = 0

def get_new_key(key_queue: Queue):
    """
    Tries to get a key. Implements 3-Stage Phoenix Protocol if empty.
    """
    global _current_key_info, _phoenix_cycle_count
    
    try:
        # Try to get a fresh key from the main pile
        _current_key_info = key_queue.get_nowait()
        log_msg(f"🔑 Switched to Key #{_current_key_info['original_index']}", -1)
        return True
    except (queue.Empty, EOFError):
        
        # --- PHOENIX PROTOCOL ---
        if not config.gemini_api_keys_info: 
            return False

        # Stage 1: First Collapse -> Wait 2 mins, Restart
        if _phoenix_cycle_count == 0:
            log_msg("🔥 [Phoenix Stage 1] All keys exhausted. Waiting 2 MIN before resurrection...", -1)
            time.sleep(120) 
            _phoenix_cycle_count += 1
            _current_key_info = random.choice(config.gemini_api_keys_info)
            log_msg(f"🦅 [Phoenix] Resurrected Key #{_current_key_info['original_index']} (Cycle 1)", -1)
            return True
            
        # Stage 2: Second Collapse -> Wait 5 mins, Restart
        elif _phoenix_cycle_count == 1:
            log_msg("🔥🔥 [Phoenix Stage 2] All keys died AGAIN. Waiting 5 MIN before final attempt...", -1)
            time.sleep(300) 
            _phoenix_cycle_count += 1
            _current_key_info = random.choice(config.gemini_api_keys_info)
            log_msg(f"🦅 [Phoenix] Resurrected Key #{_current_key_info['original_index']} (Cycle 2)", -1)
            return True
            
        # Stage 3: Total Collapse -> Graceful Exit
        else:
            log_msg("☠️ [Phoenix Failed] All keys died 3 times. Giving up.", -1)
            _current_key_info = None
            return False

def setup_genai_client():
    if not _current_key_info:
        raise NoKeysAvailableError("Worker has no API key to use.")
    genai.configure(api_key=_current_key_info['key'])
    return genai.GenerativeModel(config.gemini_model)

# --- MOSAIC HELPER FUNCTIONS ---
def create_image_mosaic(img_bytes1: bytes, img_bytes2: bytes) -> bytes:
    """
    Stitches two images side-by-side using OpenCV.
    Returns: Bytes of the single combined image.
    """
    if not OPENCV_AVAILABLE:
        return img_bytes1 # Fallback if no CV2

    try:
        # Decode
        nparr1 = np.frombuffer(img_bytes1, np.uint8)
        img1 = cv2.imdecode(nparr1, cv2.IMREAD_COLOR)
        nparr2 = np.frombuffer(img_bytes2, np.uint8)
        img2 = cv2.imdecode(nparr2, cv2.IMREAD_COLOR)

        if img1 is None or img2 is None: return img_bytes1

        # Resize to same height (e.g. 600px) to keep tokens manageable
        target_h = 600
        
        # Calculate new widths maintaining aspect ratio
        h1, w1 = img1.shape[:2]
        h2, w2 = img2.shape[:2]
        
        new_w1 = int(w1 * (target_h / h1))
        new_w2 = int(w2 * (target_h / h2))
        
        img1_resized = cv2.resize(img1, (new_w1, target_h))
        img2_resized = cv2.resize(img2, (new_w2, target_h))

        # Add a small black border between them
        separator = np.zeros((target_h, 10, 3), dtype=np.uint8) # 10px black line
        
        # Combine
        combined = np.hstack((img1_resized, separator, img2_resized))

        # Encode back to bytes (JPEG, Quality 80)
        _, buf = cv2.imencode('.jpg', combined, [cv2.IMWRITE_JPEG_QUALITY, 80])
        return buf.tobytes()

    except Exception:
        return img_bytes1 # Safe fallback

def create_image_mosaic_multi(img_bytes_list: list) -> bytes:
    """
    Stitches multiple images (2 or 3) side-by-side using OpenCV.
    For 3 images: Creates horizontal strip with all 3 images.
    Returns: Bytes of the single combined image.
    """
    if not OPENCV_AVAILABLE or not img_bytes_list:
        return img_bytes_list[0] if img_bytes_list else b''

    try:
        # Filter out None/empty images
        valid_images = [img for img in img_bytes_list if img]
        if not valid_images:
            return b''
        
        if len(valid_images) == 1:
            return valid_images[0]
        
        # Decode all images
        decoded_images = []
        for img_bytes in valid_images:
            nparr = np.frombuffer(img_bytes, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if img is not None:
                decoded_images.append(img)
        
        if not decoded_images:
            return valid_images[0]
        
        # Resize all to same height (600px for good quality/token balance)
        target_h = 600
        resized_images = []
        
        for img in decoded_images:
            h, w = img.shape[:2]
            new_w = int(w * (target_h / h))
            img_resized = cv2.resize(img, (new_w, target_h))
            resized_images.append(img_resized)
        
        # Create separator (10px black line)
        separator = np.zeros((target_h, 10, 3), dtype=np.uint8)
        
        # Combine all images horizontally with separators
        combined = resized_images[0]
        for img in resized_images[1:]:
            combined = np.hstack((combined, separator, img))
        
        # Encode back to bytes (JPEG, Quality 80 for good compression)
        _, buf = cv2.imencode('.jpg', combined, [cv2.IMWRITE_JPEG_QUALITY, 80])
        return buf.tobytes()
    
    except Exception as e:
        # Safe fallback - return first image
        log_msg(f"⚠️ Mosaic creation failed: {e}", -1)
        return valid_images[0] if valid_images else b''

def save_mosaic_image(mosaic_bytes: bytes, ad_id: str, image_type: str):
    """
    Saves a mosaic image to the configured directory if SaveMosaicImages is enabled.
    
    Args:
        mosaic_bytes: The mosaic image bytes
        ad_id: The Ad ID for filename
        image_type: Type of mosaic ("classification" or "dually_verify")
    """
    if not config.save_mosaic_images:
        return
    
    try:
        # Create directory if it doesn't exist
        os.makedirs(config.mosaic_images_dir, exist_ok=True)
        
        # Create filename with timestamp for uniqueness
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"{ad_id}_{image_type}_{timestamp}.jpg"
        filepath = os.path.join(config.mosaic_images_dir, filename)
        
        # Save the image
        with open(filepath, 'wb') as f:
            f.write(mosaic_bytes)
        
        # Optional: Log the save action (comment out if too verbose)
        # print(f"   💾 Saved mosaic: {filename}")
        
    except Exception as e:
        # Silently fail - we don't want to break the main flow if saving fails
        pass
# ------------------------------

def parse_gemini_response(raw_text: str) -> list:
    results = []
    cleaned_text = raw_text.replace("**", "").replace("Category:", "").replace("Classification:", "")
    entries = [e.strip() for e in re.split(r"[\n;]", cleaned_text) if e.strip()]
    for e in entries:
        match = re.search(r"^\d*\.?\s*([A-Za-z0-9 \-/\.]+?)\s*\((\d{1,3})%\)", e)
        if match:
            cat_name = match.group(1).strip()
            score = float(match.group(2))
            results.append((cat_name, score))
        else:
            if len(e) < 50 and not e.startswith("Analyze") and not e.startswith("Step"):
                clean_e = re.sub(r"^\d+\.\s*", "", e)
                results.append((clean_e, 0.0))
        if len(results) >= 3:
            break
    return results

def check_promotional_image(ad_img_bytes: bytes, yoda_instance=None, key_queue: Queue = None, 
                           worker_id: int = 0, status_queue: Queue = None, ad_id: str = "") -> tuple:
    """
    Pre-check function to detect promotional/coming soon images BEFORE classification.
    Returns: (is_promotional: bool, input_tokens: int, output_tokens: int)
    """
    global _key_usage_stats, _token_usage_stats, _current_key_info
    
    if not ad_img_bytes:
        return False, 0, 0
    
    # Ensure we have at least one key to start
    if not _current_key_info:
        if not get_new_key(key_queue):
            raise AllKeysExhaustedError("No more API keys available.")
    
    prompt_text = """Determine if this image is a placeholder/promotional (no vehicle) or a real truck listing.

DEFAULT: "NO" (real listing). Answer "YES" ONLY if BOTH are true:
1. ZERO vehicle visible (no truck, wheels, cab, bed, body parts)
2. Obvious placeholder: "COMING SOON" text, camera icon, black screen, dealership-only, or "No Image" graphic

Answer "NO" if ANY vehicle is visible, even if blurry, dark, obscured, or poor quality. When in doubt, answer "NO".

Format: "YES" or "NO"
"""
    
    parts = [prompt_text]
    parts.append({
        "inline_data": {
            "mime_type": "image/jpeg", 
            "data": base64.b64encode(ad_img_bytes).decode("utf-8")
        }
    })
    
    max_retries = 3
    attempt = 0
    
    while attempt < max_retries:
        try:
            # --- YODA INTERVENTION START ---
            if _current_key_info is None:
                if not get_new_key(key_queue): 
                    raise AllKeysExhaustedError("No keys")

            current_idx = _current_key_info['original_index']
            valid_key_idx, wait_time = yoda_instance.get_usable_key(current_idx)
            
            if valid_key_idx is None:
                log_msg(f"🧘 Yoda says: All keys busy. Meditating for {wait_time:.1f}s...", worker_id)
                time.sleep(wait_time)
                continue 
            
            if valid_key_idx != current_idx:
                new_key_info = next((k for k in config.gemini_api_keys_info if k['original_index'] == valid_key_idx), None)
                if new_key_info:
                    _current_key_info = new_key_info
                    log_msg(f"🔄 Yoda Swapped: Key #{current_idx} -> Key #{valid_key_idx}", worker_id)
            # --- YODA INTERVENTION END ---

            if status_queue:
                status_queue.put({
                    "worker_id": worker_id, "state": "CHECKING_PROMO", "ad_id": ad_id,
                    "key_idx": _current_key_info['original_index'], "key_total": len(config.gemini_api_keys)
                })

            log_msg(f"🔍 Pre-checking for promotional/coming soon image (Ad {ad_id})...", worker_id)
            model = setup_genai_client()
            
            t_start = time.time()
            with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
                response = model.generate_content(parts, request_options={'timeout': 30})  # OPTIMIZED: 60s -> 30s
            duration = time.time() - t_start

            key_idx = _current_key_info['original_index']
            _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['success'] += 1
            
            in_tok = getattr(response.usage_metadata, 'prompt_token_count', 0)
            out_tok = getattr(response.usage_metadata, 'candidates_token_count', 0)
            _token_usage_stats['total_tokens'] += (in_tok + out_tok)
            _token_usage_stats['api_calls'] += 1
            
            # Print LLM response to terminal
            print(f"\n{'='*80}")
            print(f"[PROMOTIONAL CHECK - Ad {ad_id}] LLM Response:")
            print(f"{'='*80}")
            print(response.text)
            print(f"{'='*80}\n")
            
            # Parse response - handle both "YES" and "YES - reason" formats
            response_text = response.text.strip().upper()
            is_promotional = response_text.startswith("YES")
            
            log_msg(f"📥 Promotional check: {'🚫 PROMOTIONAL/PLACEHOLDER' if is_promotional else '✅ REAL LISTING'} ({duration:.1f}s)", worker_id)
            
            return is_promotional, in_tok, out_tok

        except Exception as e:
            error_msg = str(e).lower()
            
            if "api_key_invalid" in error_msg:
                log_msg(f"❌ Key #{_current_key_info['original_index']} INVALID. Switching key...", worker_id)
                if status_queue: status_queue.put({"type": "key_exhausted"})
                if not get_new_key(key_queue): 
                    raise AllKeysExhaustedError("No more API keys available.")
                attempt = 0
                continue

            elif any(x in error_msg for x in ["quota", "resource", "429", "500", "502", "503", "504", "deadline", "timeout"]):
                attempt += 1
                
                if attempt == 1:
                    if status_queue and "429" in error_msg: status_queue.put({"type": "rate_limit"})
                    log_msg(f"⚠️ Promotional check hiccup (Strike 1). Waiting 3s. Error: {e}", worker_id)
                    time.sleep(3)  # OPTIMIZED: 5s -> 3s
                
                elif attempt == 2:
                    if status_queue and "429" in error_msg: status_queue.put({"type": "rate_limit"})
                    log_msg(f"🧊 Promotional check (Strike 2). Switching key...", worker_id)
                    key_idx = _current_key_info['original_index']
                    _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['quota_failure'] += 1
                    if not get_new_key(key_queue):
                        raise AllKeysExhaustedError("No more API keys available.")
                    attempt = 0
                    continue
                
                elif attempt >= max_retries:
                    log_msg(f"💀 Key exhausted during promotional check. Switching...", worker_id)
                    key_idx = _current_key_info['original_index']
                    _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['quota_failure'] += 1
                    if status_queue: status_queue.put({"type": "key_exhausted"})
                    if not get_new_key(key_queue):
                        raise AllKeysExhaustedError("No more API keys available.")
                    attempt = 0
            else:
                log_msg(f"❌ Unexpected promotional check error: {e}", worker_id)
                raise e
    
    # If all retries failed, assume it's not promotional to avoid false positives
    return False, 0, 0


def classify_with_gemini(breadcrumb: str, category_data: dict, ad_img_bytes: bytes | None = None, 
                         yoda_instance=None, key_queue: Queue = None, worker_id: int = 0, status_queue: Queue = None, ad_id: str = "", skip_promo_check: bool = False) -> tuple:
    """
    Returns: (list_of_results, input_tokens, output_tokens)
    Uses YODA for Rate Limiting.
    """
    global _key_usage_stats, _token_usage_stats, _current_key_info
    
    # Ensure we have at least one key to start
    if not _current_key_info:
        if not get_new_key(key_queue):
            raise AllKeysExhaustedError("No more API keys available.")
    
    # Initialize promotional check token counters (always track, even if check is skipped)
    promo_check_tokens_in = 0
    promo_check_tokens_out = 0
    
    # 🛡️ PRE-CHECK: Detect promotional/coming soon images BEFORE classification 🛡️
    # Skip if already checked by caller (e.g., classify_with_gemini_multi)
    if ad_img_bytes and not skip_promo_check:
        try:
            is_promotional, promo_in, promo_out = check_promotional_image(
                ad_img_bytes, yoda_instance, key_queue, worker_id, status_queue, ad_id
            )
            promo_check_tokens_in += promo_in
            promo_check_tokens_out += promo_out
            
            if is_promotional:
                log_msg(f"🚫 Promotional/Coming Soon image detected - returning 'Image Not Clear'", worker_id)
                return ([("Image Not Clear", 100.0)], promo_check_tokens_in, promo_check_tokens_out)
        except Exception as e:
            log_msg(f"⚠️ Promotional check failed, proceeding with classification: {e}", worker_id)
            # Continue with classification if check fails

    prompt_text = f"""You are an expert vehicle classifier.
Identify the vehicle in the provided 'Ad Image'.
Note: The image may be a 'Mosaic' containing two different angles.

Context Breadcrumb: "{breadcrumb}"

CRITICAL RULES:
1. **The Ladder Rack Trap:** Do NOT classify as 'Contractor Truck' just because you see a ladder rack. Utility Trucks also have ladder racks. 
   - Look for **Cabinets/Compartments** -> Utility Truck.
   - Look for **Removable Stakes/Slats** -> Contractor Truck.

2. **FLATBED PRIORITY CHECK (HIGH PRIORITY):**
   - If the cargo area is a FLAT, OPEN PLATFORM with NO side walls (you can see the bed surface from the side), the primary category MUST be **Flatbed Truck** unless a LARGE curved bulkhead is present (then **Flatbed Dump**).
   - Do NOTlabel it as **Utility Truck**, **Contractor Truck**, **Cab-Chassis**, or **Pickup Truck** if a flat open bed is clearly present.
   - **Utility Truck** requires side tool cabinets/compartments.
   - **Contractor Truck** requires a service body with toolboxes PLUS a rear stakebed/dropside section.
   - **Cab-Chassis** has NO bed/body at all (exposed frame rails).
   - **Pickup Truck** has a factory box with closed sides and a tailgate.

3. **BUCKET TRUCK - BOOM TRUCK DETECTION (HIGHEST PRIORITY - CHECK FIRST):**
   - **What is a Bucket Truck?** A truck with an aerial boom/lift that has a BUCKET/BASKET at the end where a person can stand.
   - **KEY IDENTIFIER: Look at the END of the boom - is there a BUCKET/BASKET?**
     * If YES (bucket/basket present) → 'Bucket Truck - Boom Truck'
     * If NO (hook present instead) → Crane Truck or Mechanics Truck
   - **IMPORTANT:** The presence of a bucket OVERRIDES the base truck type. Classify as 'Bucket Truck - Boom Truck' regardless of whether the base is:
     * A pickup truck with a bucket boom
     * A utility truck with service body + bucket boom
     * Any other truck type with a bucket boom
   - **Visual Cues for Bucket:**
     * A platform/basket at the end of the boom (usually rectangular or rounded)
     * Designed for a person to stand in safely
     * May have safety rails around it
     * Common on utility/electric company trucks
   - **DO NOT confuse with Crane Truck or Mechanics Truck** - those have a HOOK at the end, not a bucket.

4. **MECHANICS TRUCK DETECTION (CHECK FOR CRANE WITH HOOK + UTILITY BODY):**
   - **What is a Mechanics Truck?** A Mechanics Truck is a COMBINATION of a Utility/Service Truck body WITH a mounted crane that has a HOOK.
   - **Formula: Utility Truck + Crane WITH HOOK = Mechanics Truck**
   - **Key Visual Cues:**
     * A service body with tool compartments/cabinets on the sides (like a Utility Truck)
     * PLUS a crane/boom mounted on the body (typically at the rear or behind the cab)
     * The boom ends with a HOOK (for lifting), NOT a bucket/basket
   - **IMPORTANT:** If you see BOTH a utility/service body with compartments AND a crane with HOOK, classify as 'Mechanics Truck' - NOT as 'Utility Truck - Service Truck' or 'Crane Truck' separately.
   - **CRITICAL:** If the boom has a BUCKET/BASKET at the end (even with a utility body), classify as 'Bucket Truck - Boom Truck' instead.
   - **DO NOT confuse with:**
     * Bucket Truck (has a bucket/basket at the end, NOT a hook)
     * Pure Crane Truck (has crane but NO service body compartments)
     * Pure Utility Truck (has service body compartments but NO crane)
5. **BOX TRUCK vs DRY VAN DETECTION (SIZE-BASED DISTINCTION):**
   - **The ONLY difference between Box Truck and Dry Van is the SIZE of the cargo box:**
     * **Box Truck - Straight Truck**: Cargo box is SMALLER than 20 feet (under 6 meters)
     * **Dry Van**: Cargo box is LARGER than 20 feet (over 6 meters)
   - **Visual Size Estimation Guide (compare box length to cab):**
     * **Box Truck (under 20ft):** Box length is typically 1.5 to 2.5 times the cab length. Appears compact and proportional.
     * **Dry Van (over 20ft):** Box length is typically 3 to 4+ times the cab length. Appears much longer and dominates the vehicle profile.
   - **Additional Visual Cues:**
     * **Box Truck:** Often built on medium-duty chassis (like Ford F-650, Chevrolet 4500/5500). Box appears "integrated" with the truck.
     * **Dry Van:** Longer wheelbase, may have tandem rear axles, box extends significantly beyond the cab. Often seen on larger commercial chassis.
   - **Quick Visual Test:** If the box looks like it could fit in a residential driveway or be used for local deliveries → likely Box Truck. If it looks like a long-haul commercial vehicle → likely Dry Van.
   - **WHEN IN DOUBT:** Use the cab-to-box ratio. If box is noticeably more than 2.5x cab length, classify as Dry Van.

5a. **🚨 BOX TRUCK vs CUTAWAY CUBE VAN (CRITICAL - HIERARCHY RULE):**
   - **⚠️ THIS IS A CRITICAL CLASSIFICATION PRIORITY RULE!**
   - **"Box Truck - Straight Truck" is the PRIMARY body type category**
   - **"Cutaway Cube Van" is a SECONDARY/MODIFIER category that describes the CHASSIS TYPE only**
   
   **🚨 HIERARCHY RULE (MUST FOLLOW):**
   - If you see a truck with a rectangular cargo box attached to a cab → The PRIMARY category is **ALWAYS "Box Truck - Straight Truck"**
   - "Cutaway Cube Van" should ONLY be added as a SECONDARY category IF you can clearly see a pass-through door/opening from the cab to the cargo box
   
   **What is a Cutaway Cube Van?**
   - It is a Box Truck built on a VAN CHASSIS (like Ford E-Series, Chevrolet Express, etc.)
   - The KEY FEATURE is a visible PASS-THROUGH DOOR/OPENING from the cab to the cargo area
   - The driver can walk from the cab into the cargo box without exiting the vehicle
   
   **🚨 CRITICAL CLASSIFICATION RULES:**
   1. **NEVER output "Cutaway Cube Van" as the ONLY category** - it is NOT a standalone primary category
   2. **ALWAYS output "Box Truck - Straight Truck" FIRST as the primary category**
   3. **ONLY add "Cutaway Cube Van" as a SECOND category IF you can see the pass-through feature**
   4. **If you cannot see the pass-through door clearly → Default to "Box Truck - Straight Truck" ONLY**
   
   **CORRECT OUTPUT FORMAT:**
   - If pass-through IS visible: 
     1. Box Truck - Straight Truck (95%)
     2. Cutaway Cube Van (90%)
   - If pass-through is NOT visible or unclear:
     1. Box Truck - Straight Truck (95%)
   
   **INCORRECT OUTPUT (NEVER DO THIS):**
   ❌ 1. Cutaway Cube Van (95%)  ← WRONG! Must always have Box Truck first!
   ❌ 1. Cutaway Cube Van (95%)
      2. Box Truck - Straight Truck (85%)  ← WRONG ORDER! Box Truck must be PRIMARY!
   
   **Visual Cues for Pass-Through (Cutaway feature):**
   - A door or opening visible between the cab and cargo box
   - The cab and box appear more "integrated" rather than separate
   - The driver can access the cargo area from inside the cab
   - Common on delivery trucks where drivers need quick access to packages
   
   **When in Doubt:**
   - If you're unsure whether it's a Cutaway → Output ONLY "Box Truck - Straight Truck"
   - The pass-through feature must be CLEARLY VISIBLE to add "Cutaway Cube Van"

6. **DUALLY DETECTION (CRITICAL - ALWAYS CHECK):**
   - **CRITICAL: ALWAYS check for Dually indicators - false negatives are a major issue!**
   - **What is a Dually?** A vehicle with DUAL REAR WHEELS - TWO separate wheels/tires mounted on EACH SIDE of the rear axle (4 rear tires total instead of 2)
   - **Dually is an ATTRIBUTE, not a body type. If you detect Dually, include it as a SECONDARY category alongside the primary body type.**
   
   **==== PRIMARY VISUAL CUES (Check ALL of these carefully) ====**
   - **1. Dual Wheel Pattern:** Can you see TWO distinct wheels, rims, or tires on each rear side?
     * Look for two separate circular shapes (wheels/rims) on the same axle
     * May see a gap or shadow between the two wheels
     * Wheels appear "sandwiched" together
     * NOT just one wide tire - must be TWO separate wheels
   
   - **2. Rear Fender Width/Flare:** Is the rear section noticeably WIDER than the front?
     * Look for distinctive "hip" bulge where rear fenders flare outward
     * Rear fender should extend beyond cab width
     * Creates a noticeable "wide-hip" profile when viewed from side
     * The rear appears wider/taller than front due to fender flares
   
   - **3. Dual Rim Profile:** Look for the deep "dish" (concave) or "sandwich" appearance
     * Outer rim may appear deeply recessed or concave
     * May see two distinct rim reflections or patterns per side
     * Deep dish shape indicates dual wheel assembly
   
   - **4. Wheel Well Width:** Wider rear wheel wells to accommodate dual wheels
     * Rear wheel opening appears taller/wider than front
     * More space between body and wheels
     * Wheel wells are noticeably larger on duallys
   
   **==== SECONDARY VISUAL CUES (Additional Evidence) ====**
   - **5. Front Hub Extensions:** Dually trucks often have protruding metal hub caps on FRONT wheels
     * Large circular extensions sticking out from front wheels
     * This balances the wider rear stance
     * Look for metal hub extensions on front wheels
   
   - **6. Shadows and Gaps:** Look for shadows or gaps between dual rear wheels
     * There should be visible space or shadow between the two wheels
     * This is NOT present on single-wheel configurations
   
   - **7. Rim Pattern:** Two distinct rim patterns/reflections visible on each rear side
     * Each wheel has its own rim pattern
     * May see two separate rim reflections
   
   **==== VEHICLE TYPE CONTEXT (Statistical Likelihood) ====**
   These vehicle types are commonly Duallys - check EXTRA CAREFULLY:
   - **Box Truck - Straight Truck** (90% are Duallys) - Assume Dually unless you clearly see single thin tire
   - **Cutaway-Cube Van** (90% are Duallys) - Assume Dually unless you clearly see single thin tire
   - **Stepvan** (95% are Duallys) - Assume Dually unless you clearly see single thin tire
   - **Cabover Truck - COE** (80% are Duallys) - Check carefully for dual wheels
   - **Cab-Chassis** with utility/service body (70% are Duallys) - Look for dual wheels under body
   - **Utility Truck - Service Truck** - 🚨 EXTREME CAUTION REQUIRED: This category has the HIGHEST false positive rate. The wide service body with compartments is DESIGNED to be wide for storage - this is NORMAL and does NOT indicate Dually. You must have IRONCLAD visual proof. ONLY mark as Dually if you can see ACTUAL dual wheels (two separate wheels on each rear side). Fender flare alone is NOT sufficient. Body width is NOT sufficient. If you cannot clearly see the rear wheels themselves showing dual configuration, the answer is NOT Dually.
   - **Pickup Truck** (30% are Duallys) - Especially heavy-duty models: Ford F-350/F-450, RAM 3500, Chevy 3500, GMC 3500. Look for wide rear fenders extending beyond cab, double rear wheels visible from rear/side/3-quarter view, front wheel hub extensions
   - **Flatbed Truck** - 🚨 CAUTION REQUIRED: Flatbed trucks have a FLAT platform/deck. Many flatbeds have SINGLE rear wheels. The flat platform is NOT evidence of Dually. You MUST see ACTUAL dual wheels (two separate wheels on each rear side) to mark as Dually. Platform width, stake pockets, rails, headache racks are NOT evidence of Dually. If you cannot clearly see two wheels per rear side, the answer is NOT Dually.
   - **Contractor Truck** (40% are Duallys) - Check for dual rear wheels
   
   **==== HOW TO HANDLE UNCERTAINTY ====**
   - **🚨 FOR UTILITY-SERVICE TRUCKS - READ THIS FIRST:**
     * If you cannot SEE the actual rear wheels clearly → Answer is NOT Dually (no exceptions)
     * If rear wheels are hidden by the service body → Answer is NOT Dually
     * If you're using body width, fender appearance, or "it looks wider" as reasoning → STOP. Answer is NOT Dually
     * The ONLY acceptable reason to mark utility truck as Dually: "I can clearly see TWO separate wheel rims on each rear side"
   - **🚨 FOR FLATBED TRUCKS - READ THIS FIRST:**
     * Flatbeds have a FLAT platform - the platform width is NOT evidence of Dually
     * **⚠️ TANDEM AXLE CHECK:** Many flatbeds have MULTIPLE rear axles (tandem/tri-axle) with tires arranged LENGTHWISE (one behind another) - this is NOT Dually!
     * Dually = 2 wheels SIDE-BY-SIDE on SINGLE axle. Tandem = multiple axles arranged front-to-back.
     * If you see tires arranged ONE BEHIND ANOTHER (lengthwise) → This is tandem axle, NOT Dually. Answer is NOT Dually.
     * If you cannot SEE the actual rear wheels clearly showing dual configuration on a SINGLE axle → Answer is NOT Dually (no exceptions)
     * If you're using platform width, stake pockets, rails, headache rack, or multiple tires arranged lengthwise as reasoning → STOP. Answer is NOT Dually
     * The ONLY acceptable reason to mark flatbed truck as Dually: "I can clearly see TWO separate wheel rims SIDE-BY-SIDE on each rear side on a SINGLE axle"
   - **For other vehicle types - If rear wheels are NOT clearly visible:** Look at front wheels for hub extensions, check fender width, consider vehicle type
   - **For other vehicle types - If you see wide body but unclear wheels:** Check for fender flare, wheel well width, and vehicle type context
   - **If it's a Box Truck/Cutaway/Stepvan:** Assume Dually UNLESS you clearly see a single thin rear tire
   - **When in doubt (non-utility, non-flatbed vehicles):** If multiple secondary indicators are present (fender flare + vehicle type + front hub extensions), lean towards Dually
   - **When in doubt (UTILITY-SERVICE TRUCKS):** Answer is NOT Dually
   - **When in doubt (FLATBED TRUCKS):** Answer is NOT Dually

   
   **==== COMMON FALSE POSITIVES TO AVOID ====**
   - **⚠️ UTILITY-SERVICE TRUCK FALSE POSITIVE (VERY COMMON):** The wide service body with compartments/cabinets is NOT evidence of Dually. Many single-wheel utility trucks have wide service bodies. Do NOT mark as Dually unless you see ACTUAL dual wheels or rear fender flare extending beyond the cab.
   - **⚠️ FLATBED TRUCK FALSE POSITIVE:** The flat platform/deck is NOT evidence of Dually. Many flatbed trucks have SINGLE rear wheels. **TANDEM AXLE TRAP: Many flatbeds have MULTIPLE rear axles (tandem/tri-axle) with tires arranged LENGTHWISE (one behind another) - this is NOT Dually!** Dually = 2 wheels SIDE-BY-SIDE on SINGLE axle. If tires go front-to-back → tandem axle, NOT dually. Do NOT mark as Dually unless you can see ACTUAL dual wheels (two separate wheels SIDE-BY-SIDE on SINGLE axle). Platform width, stake pockets, rails, tires arranged lengthwise are NOT evidence of Dually.
   - **Wide service body does NOT automatically mean Dually** - check for actual dual wheels or fender flare at rear axle
   - **Single wheel with decorative hub cap** - look for two separate wheels, not one wide wheel
   - **Dirt/shadows that look like extra tires** - verify actual wheel shapes, not just shadows
   - **Wide Body != Dually:** Service/utility bodies can be wider than cab even with single rear wheels
   - **Conservative approach for Utility Trucks:** When in doubt on a Utility-Service Truck, default to NOT Dually unless you have strong visual evidence
   - **Conservative approach for Flatbed Trucks:** When in doubt on a Flatbed Truck, default to NOT Dually unless you can clearly see dual rear wheels

   
   **==== DECISION LOGIC ====**
   **Include "Dually" as a category if ANY of these are true:**
   1. You can clearly see TWO separate wheels/rims on the rear (per side)
   2. You see distinctive rear fender flare/bulge + vehicle type is typically Dually (BUT NOT for Utility-Service Trucks - see special rule below)
   3. You see dual rim "dish" pattern + wider rear profile
   4. It's a Box Truck/Cutaway/Stepvan AND you don't see a single thin tire
   5. Multiple secondary indicators are present (fender flare + vehicle type + front hub extensions)
   
   **🚨 SPECIAL RULE FOR UTILITY-SERVICE TRUCKS (MOST IMPORTANT RULE - READ CAREFULLY):**
   
   This category has 80%+ false positive rate if you're not careful. Follow this rule EXACTLY:
   
   **ONLY mark Utility-Service Truck as Dually if you meet THIS requirement:**
   ✅ You can CLEARLY SEE two separate, distinct wheel rims/tires on EACH side of the rear axle
      - Look for two circular wheel shapes side-by-side on each rear corner
      - You should see a visible gap or separation between the two wheels
      - This must be UNAMBIGUOUS - not "maybe" or "it looks like"
   
   **NEVER mark Utility-Service Truck as Dually based on:**
   ❌ Wide service body (service bodies are DESIGNED to be wide - this is normal)
   ❌ Rear fender appears wider than front (fenders can be wider without dual wheels)
   ❌ Body compartments/cabinets make it look bulky (this is the truck's purpose)
   ❌ "It looks like it should be a dually based on size" (not valid reasoning)
   ❌ Front wheel hub extensions (not sufficient alone)
   ❌ Wheel well width differences (not sufficient alone)
   ❌ Any inference or assumption (must SEE the wheels)
   
   **🚨 SPECIAL RULE FOR FLATBED TRUCKS (READ CAREFULLY):**
   
   Flatbed trucks have HIGH false positive rate. Follow this rule EXACTLY:
   
   **CRITICAL TANDEM AXLE CHECK FOR FLATBEDS:**
   ⚠️ Many flatbed trucks have MULTIPLE REAR AXLES (tandem or tri-axle) - these are NOT Dually!
   - Look at the rear wheels: are tires arranged ONE BEHIND ANOTHER (lengthwise)?
   - If you see 2+ rows of tires going front-to-back on the rear section → This is TANDEM AXLE, NOT Dually
   - Dually = 2 wheels SIDE-BY-SIDE on SINGLE axle. Tandem = multiple axles arranged lengthwise.
   - If it has multiple rear axles → Answer is NOT Dually (even if each axle has dual wheels)
   
   **ONLY mark Flatbed Truck as Dually if ALL of these are true:**
   ✅ You can CLEARLY SEE two separate, distinct wheel rims/tires on EACH side of the rear axle
      - Look for two circular wheel shapes SIDE-BY-SIDE (not front-to-back) on each rear corner
      - You should see a visible gap or separation between the two wheels
      - This must be UNAMBIGUOUS - not "maybe" or "it looks like"
   ✅ The vehicle has ONLY ONE rear axle (NOT tandem or tri-axle)
      - If you see tires arranged lengthwise (one behind another) → NOT Dually
   
   **NEVER mark Flatbed Truck as Dually based on:**
   ❌ Wide or long flatbed platform/deck (this is just the body style)
   ❌ Stake pockets, rails, or headache rack (these are accessories, not wheel indicators)
   ❌ "It looks like a heavy-duty truck" (not valid reasoning)
   ❌ Platform width or length (not evidence of dual wheels)
   ❌ Multiple tires visible (could be tandem axle, not dually)
   ❌ Tires arranged LENGTHWISE/FRONT-TO-BACK (this is tandem axle, NOT dually)
   ❌ Any inference or assumption (must SEE the wheels arranged side-by-side on single axle)

   
   **If rear wheels are NOT clearly visible in the image:**
   → Answer is NOT Dually (no exceptions, no guessing)
   
   **Do NOT include "Dually" if:**
   1. You clearly see a SINGLE thin rear tire with no dual pattern
   2. Rear width is same as front with no fender flare
   3. You're certain it's a single rear wheel configuration
   4. **🚨 It's a Utility-Service Truck and you CANNOT see the actual rear wheels clearly**
   5. **🚨 It's a Utility-Service Truck and your only evidence is "wide body" or "looks wider"**
   6. **🚨 It's a Utility-Service Truck and rear wheels are hidden/obscured by service body compartments**
   7. **🚨 It's a Flatbed Truck and you CANNOT see the actual rear wheels clearly showing dual configuration**
   8. **🚨 It's a Flatbed Truck and your only evidence is platform width, stakes, rails, or accessories**

   
   **==== OUTPUT FORMAT FOR DUALLY ====**
   - If you detect Dually, include it as a SECONDARY category (not primary)
   - Example: "1. Box Truck - Straight Truck (95%)" followed by "2. Dually (90%)"
   - The primary body type should ALWAYS be listed first, Dually should be second
   - Dually is an attribute that modifies the vehicle, not a standalone category

7. **BOX TRUCK vs REEFER/REFRIGERATED TRUCK DETECTION (CRITICAL - CHECK FIRST):**
   - **⚠️ CRITICAL: This is a HIGH-PRIORITY check. ALWAYS look for the refrigeration unit BEFORE classifying as Box Truck.**
   - **The KEY visual difference between these two is the REFRIGERATION UNIT (AC unit) on the front of the cargo box:**
     * **Reefer/Refrigerated Truck**: Has a visible refrigeration/cooling unit mounted on the FRONT (upper portion) of the cargo box, typically above/behind the cab
     * **Box Truck - Straight Truck**: The front of the cargo box is COMPLETELY FLAT and SMOOTH with NO refrigeration unit attached
   
   - **What does the refrigeration unit look like?**
     * A large boxy unit (typically white/grey) mounted on top-front of the cargo box
     * Has visible vents, fans, grilles, or louvers (air intake/exhaust)
     * Often has a brand name visible (Carrier, Thermo King, etc.)
     * May have a rounded or rectangular housing protruding from the front of the box
     * Located at the front wall of the cargo box, positioned above or just behind the truck cab
   
   - **Visual Check Steps:**
     1. Look at the FRONT of the cargo box (the wall facing the cab)
     2. Check the TOP portion of this front wall
     3. Is there a machine/unit with vents attached there?
        - YES → **Reefer/Refrigerated Truck**
        - NO (flat/smooth front) → **Box Truck - Straight Truck**
   
   - **COMMON MISTAKE TO AVOID:**
     * Do NOT confuse the cab roof (driver compartment) with the refrigeration unit
     * The refrigeration unit is ON THE CARGO BOX, not on the cab
     * If the front wall of the cargo box is plain/flat (like a smooth metal wall), it is a Box Truck



8. **\"Image Not Clear\" Rule (EXTREMELY STRICT - Use Only When Truly Impossible to Classify):**
   
   ⚠️ **CRITICAL: This image has already passed a pre-check filter. Do NOT return "Image Not Clear" unless ABSOLUTELY NECESSARY!**
   
   **ONLY use "Image Not Clear" if the image is COMPLETELY IMPOSSIBLE to classify:**
   - The image is completely black, white, or corrupted with NO vehicle visible
   - The image failed to load (shows error or blank screen)
   - You see ZERO vehicle features - no wheels, no cab, no body, no truck parts whatsoever
   - The image is 100% a placeholder graphic (camera icon with "no image" text) and NO vehicle is present
   
   **You MUST classify the vehicle normally (DO NOT use "Image Not Clear") if:**
   - You can see ANY truck or vehicle in the image, even if:
     * The image is blurry, dark, grainy, or low quality
     * The vehicle is far away or small in the frame
     * There are shadows, reflections, or poor lighting
     * The image has text, watermarks, or dealership backgrounds
     * The vehicle is partially obscured by objects, people, or other vehicles
     * Only part of the vehicle is visible (e.g., just the cab or just the bed)
     * The image angle is awkward or unusual
     * Multiple vehicles are in the frame
   - You can identify ANY of these vehicle features:
     * Wheels/tires (front or rear)
     * Cab/driver compartment
     * Bed/cargo area
     * Body panels
     * Bumpers or grille
     * Vehicle outline or silhouette
   
   **STRICT RULE: If you can see a vehicle and identify what type it is (even with low confidence), you MUST classify it. DO NOT use "Image Not Clear" just because the image quality is poor.**
   
   **Your job is to classify vehicles, not judge image quality. Focus on identifying the truck type, not the image clarity.**

9. **FLATBED / DUMP TRUCK CLASSIFICATION (CRITICAL - FOLLOW THESE STEPS IN ORDER):**
   
   ⚠️ **These four categories are frequently confused. Follow this decision tree EXACTLY:**
   
   🛑🛑🛑 **DUALLY + FLATBED DUMP ALERT (HIGH PRIORITY FIX):** 🛑🛑🛑
   **If the truck has DUALLY wheels (dual rear wheels) AND a FLAT OPEN bed with curved bulkhead:**
   **→ Output: 1. Flatbed Dump (95%), 2. Dually (90%)**
   **→ Do NOT output "Dump Truck"! Dump Truck requires SIDE WALLS!**
   **A truck can be BOTH Dually AND Flatbed Dump - these are NOT mutually exclusive!**
   **COMMON ERROR: Seeing Dually and thinking "heavy truck" = "Dump Truck" - WRONG! Check for side walls!**
   
   **🚨 QA MISMATCH GUARDRAILS (READ BEFORE CLASSIFYING):**
   - **Do NOT output "Landscape Truck" unless side walls are present AND clearly SHORT.**
   - **Never output "Landscape Truck" by itself. If landscape applies, also output "Dump Truck" (primary).**
   - **If NO side walls are visible → "Dump Truck" and "Landscape Truck" are IMPOSSIBLE.**
   - **If there IS a curved bulkhead and the bed is OPEN (no side walls) → "Flatbed Dump" only (not Dump, not Landscape).**
   - **Do not "hedge" by adding Dump/Landscape when unsure. Follow the side-wall test.**
   - **🛑 DUALLY DOES NOT CHANGE THE BED TYPE! A Flatbed Dump with Dually wheels is still Flatbed Dump, NOT Dump Truck!**
   
   🚨🚨🚨 **MOST COMMON ERROR ALERT - READ THIS FIRST:** 🚨🚨🚨
   **If you see a truck with a FLAT, OPEN bed (NO side walls) and a LARGE CURVED/ARCHED bulkhead behind the cab:**
   **→ This is FLATBED DUMP! NOT "Dump Truck"! NOT "Flatbed Truck"!**
   **The curved bulkhead is the KEY visual feature that makes it a FLATBED DUMP.**
   **Error #1: Calling it "Dump Truck" - WRONG! Dump Trucks have SIDE WALLS forming a box.**
   **Error #2: Calling it "Flatbed Truck" - WRONG! Flatbed Trucks do NOT have the large curved bulkhead.**
   
   **📋 EXAMPLE OUTPUT FOR DUALLY + FLATBED DUMP (THIS IS THE CORRECT FORMAT):**
   If you see a Flatbed Dump truck with dual rear wheels, your output should be:
   ```
   1. Flatbed Dump (95%)
   2. Dually (90%)
   ```
   **NOT: "1. Dump Truck (95%), 2. Dually (90%)" - THIS IS WRONG!**
   
   🚨🚨🚨 **EQUALLY IMPORTANT - THE OPPOSITE ERROR:** 🚨🚨🚨
   **If you see a truck with SIDE WALLS forming an ENCLOSED BOX (you CANNOT see the bed surface from the side):**
   **→ This is DUMP TRUCK! NOT "Flatbed Dump"!**
   **The SIDE WALLS are the KEY visual feature that makes it a DUMP TRUCK.**
   **Flatbed Dump has NO side walls - you can see the flat bed surface from the side!**
   
   **🔑 SIDE WALLS ARE THE FIRST AND MOST IMPORTANT CHECK!**
   
   **🚨 FUNDAMENTAL RULE - UNDERSTAND THIS FIRST (READ CAREFULLY BEFORE CLASSIFYING):**
   
   **CATEGORY A - NO SIDE WALLS (Flat/Open Bed):**
   - **Flatbed Truck**: Flat open bed, NO side walls, simple headache rack or nothing behind cab
   - **Flatbed Dump**: Flat open bed, NO side walls, LARGE CURVED bulkhead behind cab ← LOOK FOR THIS!
   
   **CATEGORY B - HAS SIDE WALLS (Enclosed Box) → NEVER classify as Flatbed Dump!**
   - **Dump Truck**: Enclosed cargo box WITH side walls, walls are TALL (≥ cab height)
   - **Landscape Truck**: Enclosed cargo box WITH side walls, walls are SHORT (< cab height)
   
   **🚨 VISUAL TEST - HOW TO IDENTIFY SIDE WALLS:**
   - Look at the SIDES of the cargo area (not the front bulkhead!)
   - **NO side walls**: You can see the FLAT BED SURFACE from the side view. The bed is OPEN and EXPOSED. You could place items on the bed and see them from the side.
   - **YES side walls**: You see SOLID METAL PANELS running along the left and right sides of the bed, creating an ENCLOSED BOX. The bed surface is HIDDEN behind the walls.
   
   **🚨 CRITICAL: A curved bulkhead behind the cab is NOT a side wall! Side walls run along the LENGTH of the bed on the LEFT and RIGHT sides!**
   
   **KEY DISTINCTION: Check for side walls FIRST! If the bed is flat and open (no side walls) → ONLY Flatbed Truck or Flatbed Dump are possible!**
   
   **==== STEP 1: CHECK FOR SIDE WALLS (MOST CRITICAL STEP - DO THIS FIRST!) ====**
   
   **🔍 WHERE TO LOOK:** Look at the LEFT and RIGHT sides of the cargo bed area (NOT the front bulkhead behind the cab!).
   
   **🚨 VISUAL TEST FOR SIDE WALLS:**
   
   **NO SIDE WALLS looks like this:**
   - You see a FLAT, HORIZONTAL bed surface (often aluminum or steel platform)
   - The bed is OPEN and EXPOSED on the sides
   - You can see UNDER and ACROSS the bed from the side view
   - Items placed on the bed would be VISIBLE from the side
   - The cargo area looks like a PLATFORM, not a BOX
   - Example: A flat aluminum bed with no walls blocking the view of the bed surface
   
   **YES SIDE WALLS looks like this:**
   - You see SOLID VERTICAL METAL PANELS on the left and right sides
   - These panels CREATE AN ENCLOSED BOX/CONTAINER
   - You CANNOT see the bed surface from the side - it's hidden behind the walls
   - The walls run the FULL LENGTH of the cargo area
   - The cargo area looks like a BOX or CONTAINER, not a platform
   
   **🚨 IMPORTANT: A curved front bulkhead (behind the cab) is NOT a side wall!**
   - The front bulkhead is ONLY at the FRONT, behind the driver's cab
   - Side walls run along the LEFT and RIGHT sides, running the LENGTH of the bed
   - A truck can have a curved front bulkhead but NO side walls = Flatbed Dump
   - A truck can have a curved front bulkhead AND side walls = Dump Truck or Landscape Truck
   
   **🚨 CRITICAL DECISION POINT:**
   - **NO side walls** (bed is FLAT, OPEN, EXPOSED - you can see the bed surface from the side) → **STOP! Dump Truck and Landscape Truck are IMPOSSIBLE. Go to STEP 2 - choose between Flatbed Truck or Flatbed Dump ONLY.**
   - **YES side walls** (solid vertical panels on left/right sides forming an enclosed box) → Go to STEP 3
   
   **==== STEP 2: NO SIDE WALLS DETECTED - CHOOSE BETWEEN FLATBED TRUCK OR FLATBED DUMP ONLY ====**
   
   ⚠️ **YOU ARE IN THIS STEP BECAUSE YOU SAW NO SIDE WALLS (FLAT, OPEN BED). THIS MEANS:**
   - ❌ **Dump Truck is IMPOSSIBLE** - Dump Trucks MUST have side walls forming an enclosed box
   - ❌ **Landscape Truck is IMPOSSIBLE** - Landscape Trucks MUST have side walls forming an enclosed box
   - ✅ **You can ONLY classify as Flatbed Truck OR Flatbed Dump**
   - ✅ **Do NOT even consider Dump Truck or Landscape Truck as options - they are eliminated!**
   
   **NOW: Check for CURVED FRONT BULKHEAD behind the cab (this determines Flatbed Dump vs Flatbed Truck):**
   
   ⚠️ **CRITICAL: Look at the area DIRECTLY BEHIND the driver's cab (NOT the sides of the bed).**
   
   **🔍 What is a curved front bulkhead?** A distinctive large steel structure that:
   - **CURVES or ARCHES forward over the cab** (like a shed roof or arc)
   - **Is SIGNIFICANTLY TALLER and MORE PROMINENT than a simple flat headache rack**
   - **Has a distinctive CURVED/BENT/ARCHED shape** (not just a flat vertical panel)
   - Often painted black or dark color
   - Often looks like a "hump" or "dome" or "arc" behind the cab
   - Creates a protective arc over the cab area
   - May have hydraulic lines or equipment visible behind the cab
   - Often seen on mason dump trucks, gooseneck flatbed dumps
   - **This curved bulkhead + flat bed (no side walls) = FLATBED DUMP!**
   
   **🎯 QUICK VISUAL TEST FOR FLATBED DUMP:**
   Look at the profile/side view: If the structure behind the cab CURVES UPWARD and FORWARD creating an ARC shape, and the bed is FLAT/OPEN with NO side walls → It's FLATBED DUMP!
   
   
   **VISUAL COMPARISON:**
   
   **Flatbed Truck (NO large curved bulkhead):**
   - Flat, open bed (no side walls) ✅
   - Either has a flat/straight headache rack (simple vertical panel), OR
   - Has nothing behind the cab (completely open), OR
   - Has only a low rack with horizontal bars
   - NO prominent curved structure behind cab
   
   **Flatbed Dump (YES large curved bulkhead):**
   - Flat, open bed (no side walls) ✅
   - Has a LARGE, PROMINENT curved/arched structure behind the cab ✅
   - The curve extends UP and forward over the cab
   - Often painted black or dark color
   - Much more substantial than a simple flat rack
   - Designed to protect the cab when dumping material
   - **THIS IS FLATBED DUMP - NOT DUMP TRUCK! (No side walls = NOT Dump Truck)**
   
   **🚨 FINAL DECISION (REMEMBER: NO SIDE WALLS = ONLY THESE TWO OPTIONS):**
   - **NO large curved bulkhead** → **"Flatbed Truck"**
   - **YES large curved/arched bulkhead present** → **"Flatbed Dump"**
   
   ⚠️ **CRITICAL REMINDER:** 
   - A flat bed with a curved bulkhead = **FLATBED DUMP** (NOT Dump Truck!)
   - Dump Truck requires an ENCLOSED BOX with SIDE WALLS
   - If you see a FLAT, OPEN bed surface, it is NOT a Dump Truck!
   
   ⚠️ **DO NOT CLASSIFY AS DUMP TRUCK OR LANDSCAPE TRUCK - THOSE REQUIRE SIDE WALLS WHICH THIS TRUCK DOES NOT HAVE!**
   
   **==== STEP 3: HAS SIDE WALLS - COMPARE HEIGHT TO CAB ====**
   Both Dump Truck and Landscape Truck have side walls AND a curved front bulkhead.
   The ONLY difference is the HEIGHT of the side walls compared to the cab.
   
   **Visual Height Test:**
   - Look at the side of the truck
   - Compare the height of the cargo box/carrier walls to the driver's cab height
   
   - **Side walls are TALL** (equal to or taller than cab height):
     * The cargo box looks deep and fully enclosed
     * Walls reach up to or above the cab roof line
     * → **"Dump Truck"**
   
   - **Side walls are SHORT** (clearly lower than cab height):
     * The cargo box walls are noticeably shorter than the cab
     * You can see over the walls when viewing from the side
     * Carrier height is visibly less than where the driver sits
     * → Output as TWO SEPARATE categories:
       - "Dump Truck" (primary)
       - "Landscape Truck" (secondary)
   
   **==== QUICK REFERENCE TABLE ====**
   | Side Walls? | Front Bulkhead? | Wall Height vs Cab | Classification | Notes |
   |------------|-----------------|-------------------|----------------|-------|
   | **NO** | NO (flat/rack)  | N/A               | **Flatbed Truck** | Flat bed, open sides, simple rack or nothing |
   | **NO** | YES (LARGE CURVED) | N/A            | **Flatbed Dump** | Flat bed, open sides, LARGE curved bulkhead |
   | **YES** | YES (curved)    | SHORT (< cab)     | 1. Dump Truck, 2. Landscape Truck | Enclosed box with short walls |
   | **YES** | YES (curved)    | TALL (≥ cab)      | Dump Truck | Enclosed box with tall walls |
   
   **🚨 CRITICAL RULE: NO SIDE WALLS = IMPOSSIBLE to be Dump Truck or Landscape Truck!**
   
   **==== COMMON MISTAKES TO AVOID ====**
   ❌ **MOST CRITICAL ERROR:** Classifying a truck with NO side walls as "Dump Truck" or "Landscape Truck" - THIS IS IMPOSSIBLE! If there are NO side walls, you can ONLY choose Flatbed Truck or Flatbed Dump!
   ❌ **CRITICAL:** Do NOT confuse Flatbed Truck with Flatbed Dump - check for the LARGE CURVED/ARCHED bulkhead behind the cab!
   ❌ **CRITICAL:** A flat headache rack is NOT the same as a curved bulkhead - Flatbed Dump has a PROMINENT CURVED structure!
   ❌ **CRITICAL:** If the bed is FLAT and OPEN (no side walls), Dump Truck and Landscape Truck are NOT OPTIONS - eliminate them immediately!
   ❌ Do NOT classify as Flatbed Dump if there are ANY solid side walls running along the bed - side walls = Dump Truck category
   ❌ Do NOT confuse a curved front bulkhead (behind cab only) with side walls (running along the sides of the bed)
   ❌ Do NOT classify short-walled trucks as just "Dump Truck" - include "Landscape Truck" too
   ❌ Do NOT use wall material (metal vs stakes) as the differentiator - use HEIGHT comparison
   ❌ Do NOT combine "Dump Truck + Landscape Truck" as one category - they must be SEPARATE entries
   ❌ **FLATBED DUMP FALSE NEGATIVE:** If you see a flatbed with a LARGE CURVED structure behind the cab but NO side walls → This IS Flatbed Dump!
   ❌ **DUMP TRUCK FALSE POSITIVE:** If you see NO side walls on a truck → This is NOT a Dump Truck - it's either Flatbed Truck or Flatbed Dump!
   
   **🎯 REMEMBER THE KEY FORMULA (CHECK SIDE WALLS FIRST!):**
   - **STEP 1: CHECK FOR SIDE WALLS** - This is the dividing line between categories!
   - **NO side walls + NO curved bulkhead = FLATBED TRUCK**
   - **NO side walls + YES curved bulkhead = FLATBED DUMP**
   - **YES side walls (enclosed box) = DUMP TRUCK** (or add Landscape if walls are short)
   
   **⚠️ KEY INSIGHT:** The curved bulkhead ONLY matters when there are NO side walls!
   - If there ARE side walls → It's Dump Truck (the bulkhead doesn't change this)
   - If there are NO side walls → Check for curved bulkhead to distinguish Flatbed Truck vs Flatbed Dump

10. **CABOVER TRUCK - COE DETECTION (CAB OVER ENGINE):**
   - **What is a Cabover Truck (COE)?** A truck where the driver's cab is positioned directly ABOVE the engine compartment, creating a distinctive FLAT-FRONT profile.
   - **Key Visual Cues for Cabover Truck - COE:**
     * **FLAT FRONT**: The cab has a flat, vertical front face with NO hood extending forward
     * **Driver sits ABOVE engine**: The cab is positioned over the engine, not behind it
     * **NO sleeping compartment**: Unlike sleeper cabs, COE trucks have a compact cab with no sleeping area behind the seats
     * **Short cab-to-axle distance**: The front axle is typically directly under or very close to the driver's position
     * **Compact overall length**: The truck appears shorter front-to-back compared to conventional trucks
   - **Common Examples:**
     * Isuzu NPR/NQR/NRR series
     * Mitsubishi Fuso Canter
     * Hino 155/195/238 series
     * GMC W-Series (Isuzu rebadge)
     * Chevrolet Tiltmaster
     * Classic Kenworth K100, Peterbilt 352, Freightliner COE
   - **Important Distinction:**
     * COE is a CAB STYLE, not a body type. A Cabover truck can have various body types (cab-chassis, box body, flatbed, etc.)
     * Include "Cabover Truck - COE" as a category when you identify this cab configuration
     * The COE category can appear alongside body type categories (e.g., "Cab-Chassis" + "Cabover Truck - COE")
   - **DO NOT confuse with:**
     * Conventional trucks with hoods (engine is in front of the cab)
     * Sleeper cab trucks (have a sleeping compartment behind the cab)


OUTPUT FORMAT INSTRUCTIONS:
- **ONLY** return the numbered list of categories with confidence scores.
- Each category must be on its OWN LINE with its OWN NUMBER.
- For short-walled dump trucks, output BOTH categories separately:
  Example: 
  1. Dump Truck (95%)
  2. Landscape Truck (90%)
- Example Output:
  1. Pickup Truck (98%)
  2. Flatbed Truck (15%)
"""
    parts = [prompt_text]
    if ad_img_bytes:
        parts.append({"inline_data": {"mime_type": "image/jpeg", "data": base64.b64encode(ad_img_bytes).decode("utf-8")}})
    else:
        return ([("Image Not Clear", 100.0)], 0, 0)
    
    parts.append("\n---\n**Category Reference:**\n")
    for name, data in category_data.items():
        parts.append(f"\n**Category: {name}**\nDefinition: {data.get('definition', 'No definition.')}")
        if config.include_example_images:
            parts.append("Example Image:")
            if data.get("image_bytes"):
                parts.append({"inline_data": {"mime_type": "image/jpeg", "data": base64.b64encode(data['image_bytes']).decode("utf-8")}})
            else:
                parts.append("(No example image)")
    
    max_retries = 3
    attempt = 0
    
    while attempt < max_retries:
        try:
            # --- YODA INTERVENTION START ---
            if _current_key_info is None:
                if not get_new_key(key_queue): raise AllKeysExhaustedError("No keys")

            current_idx = _current_key_info['original_index']
            valid_key_idx, wait_time = yoda_instance.get_usable_key(current_idx)
            
            if valid_key_idx is None:
                log_msg(f"🧘 Yoda says: All keys busy. Meditating for {wait_time:.1f}s...", worker_id)
                time.sleep(wait_time)
                continue 
            
            if valid_key_idx != current_idx:
                new_key_info = next((k for k in config.gemini_api_keys_info if k['original_index'] == valid_key_idx), None)
                if new_key_info:
                    _current_key_info = new_key_info
                    log_msg(f"🔄 Yoda Swapped: Key #{current_idx} -> Key #{valid_key_idx}", worker_id)
            # --- YODA INTERVENTION END ---

            if status_queue:
                status_queue.put({
                    "worker_id": worker_id, "state": "PROCESSING", "ad_id": ad_id,
                    "key_idx": _current_key_info['original_index'], "key_total": len(config.gemini_api_keys)
                })

            log_msg(f"📤 Sending Request (Key #{_current_key_info['original_index']})...", worker_id)
            model = setup_genai_client()
            
            t_start = time.time()
            with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
                response = model.generate_content(parts, request_options={'timeout': 45})  # OPTIMIZED: 90s -> 45s
            duration = time.time() - t_start

            key_idx = _current_key_info['original_index']
            _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['success'] += 1
            
            in_tok = getattr(response.usage_metadata, 'prompt_token_count', 0)
            out_tok = getattr(response.usage_metadata, 'candidates_token_count', 0)
            _token_usage_stats['total_tokens'] += (in_tok + out_tok)
            _token_usage_stats['api_calls'] += 1
            
            # Print LLM response to terminal
            print(f"\n{'='*80}")
            print(f"[CLASSIFICATION - Ad {ad_id}] LLM Response:")
            print(f"{'='*80}")
            print(response.text)
            print(f"{'='*80}\n")
            
            log_msg(f"📥 Response ({duration:.1f}s): Tokens In:{in_tok}/Out:{out_tok}", worker_id)
            
            # Note: No time.sleep() needed here because Yoda handles the pacing!
            # Add promotional check tokens to total
            total_in_tok = in_tok + promo_check_tokens_in
            total_out_tok = out_tok + promo_check_tokens_out
            return parse_gemini_response(response.text), total_in_tok, total_out_tok

        except Exception as e:
            error_msg = str(e).lower()
            
            if "api_key_invalid" in error_msg:
                log_msg(f"❌ Key #{_current_key_info['original_index']} INVALID: {e}", worker_id)
                if status_queue: status_queue.put({"type": "key_exhausted"})
                if not get_new_key(key_queue): raise AllKeysExhaustedError("No keys.")
                attempt = 0
                continue

            elif any(x in error_msg for x in ["quota", "resource", "429", "500", "502", "503", "504", "deadline", "timeout"]):
                attempt += 1
                
                if attempt == 1:
                    if status_queue and "429" in error_msg: status_queue.put({"type": "rate_limit"})
                    log_msg(f"⚠️ API Hiccup (Strike 1). Waiting 5s. Error: {e}", worker_id)
                    time.sleep(5)  # OPTIMIZED: 30s -> 5s
                
                elif attempt == 2:
                    if status_queue and "429" in error_msg: status_queue.put({"type": "rate_limit"})
                    if status_queue: status_queue.put({"worker_id": worker_id, "state": "🥶 Cooling", "ad_id": ad_id})
                    log_msg(f"🧊 API Freeze (Strike 2). Cooling 15s. Error: {e}", worker_id)
                    time.sleep(15)  # OPTIMIZED: 180s -> 15s (Yoda handles rate limiting) 
                
                elif attempt >= max_retries:
                    log_msg(f"💀 Key #{_current_key_info['original_index']} DEAD (Strike 3). Switching.", worker_id)
                    
                    key_idx = _current_key_info['original_index']
                    _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['quota_failure'] += 1
                    
                    if status_queue and "50" not in error_msg: status_queue.put({"type": "key_exhausted"})
                    
                    if not get_new_key(key_queue):
                        raise AllKeysExhaustedError("No more API keys available.")
                    
                    attempt = 0 
            else:
                log_msg(f"❌ Unexpected API Error: {e}", worker_id)
                raise e

def classify_with_refinement(categories: list, rule: dict, ad_img_bytes: bytes, 
                             yoda_instance, key_queue: Queue, worker_id: int, ad_id: str = "", status_queue: Queue = None) -> tuple:
    """
    Returns: (refined_category_string, input_tokens, output_tokens)
    """
    global _key_usage_stats, _token_usage_stats, _current_key_info

    if not _current_key_info:
        if not get_new_key(key_queue):
            raise AllKeysExhaustedError("No more API keys available.")

    is_feature_checklist = "feature_checklist" in rule
    prompt = ""
    if is_feature_checklist:
        checklist = rule["feature_checklist"]
        prompt_lines = [checklist.get("prompt", "Analyze the image and answer with 'Yes' or 'No'.")]
        for feature in checklist.get("features", []):
            prompt_lines.append(f"- {feature.get('question')}")
        prompt_lines.append("\nFormat your answer ONLY as a JSON object.")
        prompt = "\n".join(prompt_lines)
    elif "decision_rule" in rule:
        pair = categories[:2]
        prompt = f"An AI identified a vehicle as possibly a '{pair[0]}' or a '{pair[1]}'.\nYour task is to use this visual test: \"{rule['decision_rule']}\"\nLook for visual cues like Rim Depth (Deep Dish = Dually) and Hub Shape. Analyze the image strictly and output ONLY the final category name."
    else:
        return None, 0, 0

    max_retries = 3
    attempt = 0
    
    while attempt < max_retries:
        try:
            # --- YODA INTERVENTION START ---
            if _current_key_info is None:
                if not get_new_key(key_queue): raise AllKeysExhaustedError("No keys")

            current_idx = _current_key_info['original_index']
            valid_key_idx, wait_time = yoda_instance.get_usable_key(current_idx)
            
            if valid_key_idx is None:
                log_msg(f"🧘 Yoda says: All keys busy. Meditating {wait_time:.1f}s...", worker_id)
                time.sleep(wait_time)
                continue 
            
            if valid_key_idx != current_idx:
                new_key_info = next((k for k in config.gemini_api_keys_info if k['original_index'] == valid_key_idx), None)
                if new_key_info:
                    _current_key_info = new_key_info
                    log_msg(f"🔄 Yoda Swapped: Key #{current_idx} -> Key #{valid_key_idx}", worker_id)
            # --- YODA INTERVENTION END ---

            if status_queue:
                status_queue.put({
                    "worker_id": worker_id, "state": "PROCESSING", "ad_id": ad_id,
                    "key_idx": _current_key_info['original_index'], "key_total": len(config.gemini_api_keys)
                })

            log_msg(f"📤 Sending Refinement Request...", worker_id)
            model = setup_genai_client()
            with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
                 response = model.generate_content([prompt, {"inline_data": {"mime_type": "image/jpeg", "data": base64.b64encode(ad_img_bytes).decode("utf-8")}}], request_options={'timeout': 45})  # OPTIMIZED: 90s -> 45s
            
            key_idx = _current_key_info['original_index']
            _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['success'] += 1
            
            in_tok = getattr(response.usage_metadata, 'prompt_token_count', 0)
            out_tok = getattr(response.usage_metadata, 'candidates_token_count', 0)
            _token_usage_stats['total_tokens'] += (in_tok + out_tok)
            _token_usage_stats['api_calls'] += 1
            
            # Print LLM response to terminal
            print(f"\n{'='*80}")
            print(f"[REFINEMENT - Ad {ad_id}] LLM Response:")
            print(f"{'='*80}")
            print(response.text)
            print(f"{'='*80}\n")
            
            log_msg(f"📥 Refinement Response: {repr(response.text)} (In:{in_tok}/Out:{out_tok})", worker_id)
            
            result_str = None
            if is_feature_checklist:
                try:
                    json_str = response.text.strip().replace("```json", "").replace("```", "")
                    ai_features = json.loads(json_str)
                    features = {f['name']: ai_features.get(f['name'], "No").lower() == "yes" for f in rule["feature_checklist"].get("features", [])}
                    logic_str = rule["feature_checklist"].get("logic", "")
                    if " if " in logic_str and " else " in logic_str:
                        true_val_str, condition_str = logic_str.split(" if ")
                        condition_str, false_val_str = condition_str.split(" else ")
                        if eval(condition_str, {"__builtins__": {}}, {"features": features}):
                            result_str = true_val_str.strip().strip("'\"")
                        else:
                            result_str = false_val_str.strip().strip("'\"")
                except Exception as e:
                    log_msg(f"   [W-{worker_id}] ⚠️ Could not parse feature logic: {e}", worker_id)
            else:
                refined_category = response.text.strip().replace("'", "").replace('"', "")
                for c in categories:
                    if c.lower() in refined_category.lower():
                        result_str = c
                        break
            
            return result_str, in_tok, out_tok
                
        except Exception as e:
            error_msg = str(e).lower()
            if any(x in error_msg for x in ["quota", "resource", "429", "500", "502", "503", "504", "deadline", "timeout"]):
                attempt += 1
                if attempt == 1:
                    if status_queue and "429" in error_msg: status_queue.put({"type": "rate_limit"})
                    log_msg(f"⚠️ Refinement Hiccup (Strike 1). Waiting 5s. Error: {e}", worker_id)
                    time.sleep(5)  # OPTIMIZED: 30s -> 5s
                elif attempt == 2:
                    if status_queue and "429" in error_msg: status_queue.put({"type": "rate_limit"})
                    if status_queue: status_queue.put({"worker_id": worker_id, "state": "🥶 Cooling", "ad_id": ad_id})
                    log_msg(f"🧊 Refinement Freeze (Strike 2). Cooling 15s...", worker_id)
                    time.sleep(15)  # OPTIMIZED: 180s -> 15s
                elif attempt >= max_retries:
                    log_msg(f"💀 Key DEAD during refinement. Switching.", worker_id)
                    key_idx = _current_key_info['original_index']
                    _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['quota_failure'] += 1
                    if status_queue: status_queue.put({"type": "key_exhausted"})
                    if not get_new_key(key_queue):
                        raise AllKeysExhaustedError("No more API keys available.")
                    attempt = 0
            else:
                raise e

def classify_with_gemini_multi(breadcrumb: str, category_data: dict, img_bytes_list: list | None = None, 
                               fast_mode: bool = False, yoda_instance=None, key_queue: Queue = None, worker_id: int = 0, status_queue: Queue = None, ad_id: str = "") -> tuple:
    """
    Returns: (list_of_results, total_input_tokens, total_output_tokens)
    Uses MOSAIC Strategy + YODA.
    """
    if not img_bytes_list:
        res, t_in, t_out = classify_with_gemini(breadcrumb, category_data, None, yoda_instance, key_queue, worker_id, status_queue, ad_id, skip_promo_check=True)
        return res, t_in, t_out
    
    total_in = 0
    total_out = 0
    all_results = []
    
    # 🛡️ PRE-CHECK: Check first image for promotional/coming soon BEFORE processing
    # This saves API costs by catching promotional images early
    if img_bytes_list and len(img_bytes_list) > 0:
        try:
            is_promotional, promo_in, promo_out = check_promotional_image(
                img_bytes_list[0], yoda_instance, key_queue, worker_id, status_queue, ad_id
            )
            total_in += promo_in
            total_out += promo_out
            
            if is_promotional:
                log_msg(f"🚫 Promotional/Coming Soon image detected on first image - returning 'Image Not Clear'", worker_id)
                return [("Image Not Clear", 100.0)], total_in, total_out
        except Exception as e:
            log_msg(f"⚠️ Promotional check failed, proceeding with classification: {e}", worker_id)
            # Continue with classification if check fails
    
    # --- MOSAIC STRATEGY START ---
    # If we have 2+ images, combine them and send 1 Request.
    if len(img_bytes_list) >= 2 and OPENCV_AVAILABLE:
        try:
            log_msg(f"🧩 Stitching 2 Images into Mosaic (Cost Saving)...", worker_id)
            # Use Index 0 and 1 (Usually sorted by Vision V2 as best)
            mosaic_bytes = create_image_mosaic(img_bytes_list[0], img_bytes_list[1])
            
            # Save mosaic if enabled in config
            save_mosaic_image(mosaic_bytes, ad_id, "classification")
            
            res, t_in, t_out = classify_with_gemini(breadcrumb, category_data, mosaic_bytes, yoda_instance, key_queue, worker_id, status_queue, ad_id, skip_promo_check=True)
            total_in += t_in
            total_out += t_out
            all_results.extend(res)
            
        except Exception as e:
            log_msg(f"⚠️ Mosaic failed ({e}). Falling back to single image.", worker_id)
            # Fallback to single image
            res, t_in, t_out = classify_with_gemini(breadcrumb, category_data, img_bytes_list[0], yoda_instance, key_queue, worker_id, status_queue, ad_id, skip_promo_check=True)
            total_in += t_in
            total_out += t_out
            all_results.extend(res)
    else:
        # Single Image Case
        log_msg(f"📸 Single Image Classification...", worker_id)
        res, t_in, t_out = classify_with_gemini(breadcrumb, category_data, img_bytes_list[0], yoda_instance, key_queue, worker_id, status_queue, ad_id, skip_promo_check=True)
        total_in += t_in
        total_out += t_out
        all_results.extend(res)
    # -----------------------------

    if not all_results:
        return [], total_in, total_out

    combined = {cat: score for cat, score in all_results if cat and (cat not in (c:={}) or score > c[cat])}
    normalized = {cat: round(score - (score - 90) * 0.8, 1) if score > 95 else round(score, 1) for cat, score in combined.items()}
    final_res = sorted(normalized.items(), key=lambda x: x[1], reverse=True)[:3]
    
    return final_res, total_in, total_out

def get_key_usage_stats(): return {"stats": _key_usage_stats}
def get_token_usage_stats(): return _token_usage_stats


# ==================== DUALLY LLM VERIFICATION ====================

# def verify_dually_with_llm(ad_img_bytes_list: list[bytes], yoda_instance, key_queue: Queue, 
#                            worker_id: int = 0, ad_id: str = "", status_queue: Queue = None) -> tuple:
#     """
#     LLM-based verification for Dually detection to reduce false positives.
    
#     This function serves as a double-check for listings that have been marked as Dually.
#     It asks the LLM to specifically verify if the vehicle has dual rear wheels.
    
#     Args:
#         ad_img_bytes_list: List of image bytes (all available images for better accuracy)
    
#     Returns: (is_dually: bool, confidence: float, input_tokens: int, output_tokens: int)
#     """
#     global _key_usage_stats, _token_usage_stats, _current_key_info
    
#     # Ensure we have at least one key to start
#     if not _current_key_info:
#         if not get_new_key(key_queue):
#             raise AllKeysExhaustedError("No more API keys available.")
    
#     # Filter out None/empty images
#     valid_images = [img for img in ad_img_bytes_list if img]
#     if not valid_images:
#         return False, 0.0, 0, 0
    
#     # Detailed prompt for accurate Dually verification (ENHANCED - More Aggressive)
#     image_count_text = f"{len(valid_images)} image{'s' if len(valid_images) > 1 else ''}"
#     prompt_text = f"""You are an expert vehicle analyst specializing in wheel configuration detection.

# Your CRITICAL task is to determine if this vehicle has DUAL REAR WHEELS (Dually).

# You have been provided with {image_count_text} of this vehicle from different angles. Use ALL images to get the best view of the rear wheels and fenders.

# ⚠️ IMPORTANT: False NEGATIVES are a major problem - we are MISSING many Duallys. Be thorough and look for ALL indicators across ALL images.

# ==== WHAT IS A DUALLY? ====
# A "Dually" truck has TWO separate wheels/tires mounted on EACH SIDE of the rear axle:
# - Total of 4 rear tires (2 per side) instead of 2 rear tires (1 per side)
# - Creates a wider rear stance with distinctive "hip" bulge
# - Often has flared rear fenders that protrude beyond the cab width

# ==== PRIMARY VISUAL CUES (Check ALL of these across ALL images) ====
# **IMPORTANT: Examine ALL provided images carefully. Different angles may show dually indicators more clearly.**
# - Look at side views for fender flare and wheel well width
# - Look at rear views for dual wheel pattern
# - Look at 3/4 views for overall width comparison

# 1. **Dual Wheel Pattern**: Can you see TWO distinct wheels, rims, or tires on each rear side?
#    - Look for two separate circular shapes (wheels/rims) on the same axle
#    - May see a gap or shadow between the two wheels
#    - Wheels appear "sandwiched" together
#    - Check rear-view and side-view images for best visibility

# 2. **Rear Fender Width/Flare**: Is the rear section noticeably WIDER than the front?
#    - Look for distinctive "hip" bulge where rear fenders flare outward
#    - Rear fender should extend beyond cab width
#    - Creates a noticeable "wide-hip" profile
#    - Side-view images are best for seeing this

# 3. **Dual Rim Profile**: Look for the deep "dish" (concave) or "sandwich" appearance
#    - Outer rim may appear deeply recessed or concave
#    - May see two distinct rim reflections or patterns per side
#    - Check images from different angles to see rim depth

# 4. **Wheel Well Width**: Wider rear wheel wells to accommodate dual wheels
#    - Rear wheel opening appears taller/wider than front
#    - More space between body and wheels
#    - Compare front and rear wheel well sizes across images

# ==== SECONDARY CUES (Additional Evidence) ====
# 5. **Front Hub Extensions**: Dually trucks often have protruding metal hub caps on FRONT wheels
#    - Large circular extensions sticking out from front wheels
#    - This balances the wider rear stance

# 6. **Vehicle Type Context**: These vehicle types are commonly Duallys:
#    - **Box Truck / Straight Truck** (90% are Duallys)
#    - **Cutaway-Cube Van** (90% are Duallys)
#    - **Stepvan** (95% are Duallys)
#    - **Cabover / COE commercial trucks** (80% are Duallys)
#    - **Heavy-Duty Pickup Trucks** (30% are Duallys) - Ford F-350/F-450, RAM 3500, Chevy 3500, GMC 3500
#    - If you see these types, look EXTRA CAREFULLY for dually indicators
#    - **UTILITY/SERVICE TRUCK DUALLY TIPS**: Don't let the wide service body confuse you - look at the REAR WHEELS specifically (below/behind the compartments). Check for fender width at rear axle vs cab width.
#    - **PICKUP TRUCK DUALLY TIPS**: Look for wide rear fenders that extend beyond the cab, double rear wheels visible from rear/side/3-quarter view, and front wheel hub extensions

# 7. **Side Profile**: Rear appears noticeably wider/taller than front when viewed from side

# 8. **Shadows and Gaps**: Look for shadows or gaps between dual rear wheels

# ==== HOW TO HANDLE UNCERTAINTY ====
# - **Use ALL images**: If one image doesn't show rear wheels clearly, check other images from different angles
# - **If rear wheels are NOT clearly visible in one image**: Check other images - side views, rear views, or 3/4 views may show the wheels better
# - **If you see wide body but unclear wheels**: Check multiple images for fender flare, wheel well width, and vehicle type
# - **If it's a Box Truck/Cutaway/Stepvan**: Assume Dually UNLESS you clearly see a single thin rear tire in ANY of the images
# - **Cross-reference between images**: If one image suggests dually but another doesn't, look for consistent indicators across multiple images

# ==== COMMON FALSE POSITIVES TO AVOID ====
# - Wide service body does NOT automatically mean Dually (but check other cues!)
# - Single wheel with decorative hub cap (look for two separate wheels, not one wide wheel)
# - Dirt/shadows that look like extra tires (verify actual wheel shapes)

# ==== DECISION LOGIC ====
# Answer "YES" if ANY of these are true:
# 1. You can clearly see TWO separate wheels/rims on the rear (per side)
# 2. You see distinctive rear fender flare/bulge + vehicle type is typically Dually
# 3. You see dual rim "dish" pattern + wider rear profile
# 4. It's a Box Truck/Cutaway/Stepvan AND you don't see a single thin tire

# Answer "NO" only if:
# 1. You clearly see a SINGLE thin rear tire with no dual pattern
# 2. Rear width is same as front with no fender flare
# 3. You're certain it's a single rear wheel configuration

# When in doubt, lean towards "YES" if multiple secondary indicators are present.

# ==== RESPONSE FORMAT ====
# Respond with ONLY one of these formats:
# - "YES - [specific reason: what visual cues you saw]"
# - "NO - [specific reason: why you're certain it's single wheel]"

# Examples:
# - "YES - I can see two distinct wheel rims on each rear side with a gap between them"
# - "YES - Box truck with distinctive rear fender flare extending beyond cab width"
# - "YES - Rear fenders are noticeably wider than front, creating hip bulge typical of dually"
# - "NO - I can clearly see a single thin rear tire on each side with no dual pattern"
# """

#     parts = [prompt_text]
#     # Add all images to the request for better accuracy
#     for img_bytes in valid_images:
#         parts.append({
#             "inline_data": {
#                 "mime_type": "image/jpeg", 
#                 "data": base64.b64encode(img_bytes).decode("utf-8")
#             }
#         })
    
#     print(f"[DUALLY VERIFY] Ad {ad_id}: Sending {len(valid_images)} image(s) for verification")
    
#     max_retries = 3
#     attempt = 0
    
#     while attempt < max_retries:
#         try:
#             # --- YODA INTERVENTION START ---
#             if _current_key_info is None:
#                 if not get_new_key(key_queue): 
#                     raise AllKeysExhaustedError("No keys")

#             current_idx = _current_key_info['original_index']
#             valid_key_idx, wait_time = yoda_instance.get_usable_key(current_idx)
            
#             if valid_key_idx is None:
#                 log_msg(f"🧘 Yoda says: All keys busy. Meditating for {wait_time:.1f}s...", worker_id)
#                 time.sleep(wait_time)
#                 continue 
            
#             if valid_key_idx != current_idx:
#                 new_key_info = next((k for k in config.gemini_api_keys_info if k['original_index'] == valid_key_idx), None)
#                 if new_key_info:
#                     _current_key_info = new_key_info
#                     log_msg(f"🔄 Yoda Swapped: Key #{current_idx} -> Key #{valid_key_idx}", worker_id)
#             # --- YODA INTERVENTION END ---

#             if status_queue:
#                 status_queue.put({
#                     "worker_id": worker_id, "state": "VERIFYING_DUALLY", "ad_id": ad_id,
#                     "key_idx": _current_key_info['original_index'], "key_total": len(config.gemini_api_keys)
#                 })

#             log_msg(f"🔍 Verifying Dually for Ad {ad_id} (Key #{_current_key_info['original_index']})...", worker_id)
#             model = setup_genai_client()
            
#             t_start = time.time()
#             with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
#                 response = model.generate_content(parts, request_options={'timeout': 45})  # OPTIMIZED: 90s -> 45s
#             duration = time.time() - t_start

#             key_idx = _current_key_info['original_index']
#             _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['success'] += 1
            
#             in_tok = getattr(response.usage_metadata, 'prompt_token_count', 0)
#             out_tok = getattr(response.usage_metadata, 'candidates_token_count', 0)
#             _token_usage_stats['total_tokens'] += (in_tok + out_tok)
#             _token_usage_stats['api_calls'] += 1
            
#             # Print LLM response to terminal
#             print(f"\n{'='*80}")
#             print(f"[DUALLY VERIFICATION - Ad {ad_id}] LLM Response:")
#             print(f"{'='*80}")
#             print(response.text)
#             print(f"{'='*80}\n")
            
#             # Parse response
#             response_text = response.text.strip().upper()
#             is_dually = response_text.startswith("YES")
            
#             # Confidence based on response clarity
#             confidence = 95.0 if is_dually else 5.0
            
#             log_msg(f"📥 Dually Verification Result: {'✅ CONFIRMED' if is_dually else '❌ NOT DUALLY'} ({duration:.1f}s)", worker_id)
            
#             return is_dually, confidence, in_tok, out_tok

#         except Exception as e:
#             error_msg = str(e).lower()
            
#             if "api_key_invalid" in error_msg:
#                 log_msg(f"❌ Key #{_current_key_info['original_index']} INVALID. Switching key...", worker_id)
#                 if status_queue: status_queue.put({"type": "key_exhausted"})
#                 if not get_new_key(key_queue): 
#                     raise AllKeysExhaustedError("No more API keys available.")
#                 attempt = 0  # Reset attempts with new key
#                 continue

#             elif any(x in error_msg for x in ["quota", "resource", "429", "500", "502", "503", "504", "deadline", "timeout"]):
#                 attempt += 1
                
#                 if attempt == 1:
#                     if status_queue and "429" in error_msg: status_queue.put({"type": "rate_limit"})
#                     log_msg(f"⚠️ Dually Verification Hiccup (Strike 1). Waiting 3s. Error: {e}", worker_id)
#                     time.sleep(3)  # OPTIMIZED: 5s -> 3s
                
#                 elif attempt == 2:
#                     if status_queue and "429" in error_msg: status_queue.put({"type": "rate_limit"})
#                     log_msg(f"🧊 Dually Verification (Strike 2). Switching key...", worker_id)
#                     # Switch to next key instead of waiting
#                     key_idx = _current_key_info['original_index']
#                     _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['quota_failure'] += 1
#                     if not get_new_key(key_queue):
#                         raise AllKeysExhaustedError("No more API keys available.")
#                     attempt = 0  # Reset with new key
#                     continue
                
#                 elif attempt >= max_retries:
#                     log_msg(f"💀 Key #{_current_key_info['original_index']} exhausted. Switching...", worker_id)
#                     key_idx = _current_key_info['original_index']
#                     _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['quota_failure'] += 1
#                     if status_queue: status_queue.put({"type": "key_exhausted"})
#                     if not get_new_key(key_queue):
#                         raise AllKeysExhaustedError("No more API keys available.")
#                     attempt = 0  # Reset with new key
#             else:
#                 log_msg(f"❌ Unexpected Dually Verification Error: {e}", worker_id)
#                 raise e
    
#     return False, 0.0, 0, 0

# ==================== DUALLY LLM VERIFICATION ====================

def verify_dually_with_llm(ad_img_bytes_list: list[bytes], yoda_instance, key_queue: Queue, 
                           worker_id: int = 0, ad_id: str = "", status_queue: Queue = None) -> tuple:
    """
    LLM-based verification for Dually detection to reduce false positives.
    
    This function serves as a double-check for listings that have been marked as Dually.
    It asks the LLM to specifically verify if the vehicle has dual rear wheels.
    
    Args:
        ad_img_bytes_list: List of image bytes (all available images for better accuracy)
    
    Returns: (is_dually: bool, confidence: float, input_tokens: int, output_tokens: int)
    """
    global _key_usage_stats, _token_usage_stats, _current_key_info
    
    # Ensure we have at least one key to start
    if not _current_key_info:
        if not get_new_key(key_queue):
            raise AllKeysExhaustedError("No more API keys available.")
    
    # Filter out None/empty images
    valid_images = [img for img in ad_img_bytes_list if img]
    if not valid_images:
        return False, 0.0, 0, 0
    
    # Create mosaic from all available images for efficiency (reduces tokens significantly)
    mosaic_image = create_image_mosaic_multi(valid_images)
    image_count_text = f"{len(valid_images)} image{'s' if len(valid_images) > 1 else ''}"
    
    # Save mosaic if enabled in config
    save_mosaic_image(mosaic_image, ad_id, "dually_verify")
    
    # Detailed prompt for accurate Dually verification (ENHANCED - More Aggressive)
    if len(valid_images) > 1:
        prompt_text = f"""You are an expert vehicle analyst specializing in wheel configuration detection.

Your CRITICAL task is to determine if this vehicle has DUAL REAR WHEELS (Dually) on a SINGLE REAR AXLE.

You have been provided with a MOSAIC image showing {image_count_text} of this vehicle from different angles side-by-side. Examine ALL views in the mosaic to get the best view of the rear wheels and fenders.

⚠️ IMPORTANT: False NEGATIVES are a major problem - we are MISSING many Duallys. Be thorough and look for ALL indicators across ALL views in the mosaic."""
    else:
        prompt_text = f"""You are an expert vehicle analyst specializing in wheel configuration detection.

Your CRITICAL task is to determine if this vehicle has DUAL REAR WHEELS (Dually) on a SINGLE REAR AXLE.

You have been provided with 1 image of this vehicle. Examine it carefully to determine if it has dual rear wheels.

⚠️ IMPORTANT: False NEGATIVES are a major problem - we are MISSING many Duallys. Be thorough and look for ALL indicators."""
    
    prompt_text += """

==== WHAT IS A DUALLY (FOR THIS TASK)? ====
A "Dually" truck has TWO separate wheels/tires mounted on EACH SIDE of the rear axle:
- Total of 4 rear tires (2 per side).
- **CRITICAL CONSTRAINT: It must have ONLY ONE rear axle.**
- Creates a wider rear stance with distinctive "hip" bulge.
- Often has flared rear fenders that protrude beyond the cab width.

==== EXCLUSION RULE: MULTI-AXLE VEHICLES ====
**🚫 DO NOT CLASSIFY AS DUALLY IF:**
- The vehicle has **MULTIPLE REAR AXLES** (Tandem axle, Tri-axle, etc.).
- If you see tires arranged **lengthwise** (one tire in front of another tire) on the rear side.
- Even if those axles have dual tires, if there is more than one axle row, the answer must be **NO**.
- We only want standard Dually trucks (4 rear tires total), NOT heavy commercial multi-axle trucks (8+ rear tires).

==== PRIMARY VISUAL CUES (Check ALL of these) ====
**IMPORTANT: If viewing a mosaic, examine each view carefully. Different angles may show dually indicators more clearly.**
- Look at side views for fender flare and wheel well width
- Look at rear views for dual wheel pattern
- Look at 3/4 views for overall width comparison

1. **Dual Wheel Pattern**: Can you see TWO distinct wheels, rims, or tires on each rear side?
   - Look for two separate circular shapes (wheels/rims) on the same axle
   - May see a gap or shadow between the two wheels
   - Wheels appear "sandwiched" together
   - Check rear-view and side-view images for best visibility

2. **Rear Fender Width/Flare**: Is the rear section noticeably WIDER than the front?
   - Look for distinctive "hip" bulge where rear fenders flare outward
   - Rear fender should extend beyond cab width
   - Creates a noticeable "wide-hip" profile
   - Side-view images are best for seeing this

3. **Dual Rim Profile**: Look for the deep "dish" (concave) or "sandwich" appearance
   - Outer rim may appear deeply recessed or concave
   - May see two distinct rim reflections or patterns per side
   - Check images from different angles to see rim depth

4. **Wheel Well Width**: Wider rear wheel wells to accommodate dual wheels
   - Rear wheel opening appears taller/wider than front
   - More space between body and wheels
   - Compare front and rear wheel well sizes across images

==== SECONDARY CUES (Additional Evidence) ====
5. **Front Hub Extensions**: Dually trucks often have protruding metal hub caps on FRONT wheels
   - Large circular extensions sticking out from front wheels
   - This balances the wider rear stance

6. **Vehicle Type Context**: These vehicle types are commonly Duallys:
   - **Box Truck / Straight Truck** (90% are Duallys)
   - **Cutaway-Cube Van** (90% are Duallys)
   - **Stepvan** (95% are Duallys)
   - **Cabover / COE commercial trucks** (80% are Duallys)
   - **Heavy-Duty Pickup Trucks** (30% are Duallys) - Ford F-350/F-450, RAM 3500, Chevy 3500, GMC 3500
   - If you see these types, look EXTRA CAREFULLY for dually indicators
   - **🚨 UTILITY/SERVICE TRUCK DUALLY TIPS (CRITICAL - 80% FALSE POSITIVE RATE)**: 
     * Wide service body with compartments is DESIGNED to be wide - this is NORMAL, NOT evidence of Dually
     * Service bodies often extend beyond cab width even with single rear wheels
     * You MUST SEE the actual rear wheels themselves showing dual configuration
     * Fender flare alone is NOT sufficient for utility trucks
     * Body width is NOT sufficient for utility trucks
     * If you cannot clearly see TWO separate wheels on each rear side → Answer is NO
     * When in doubt for utility trucks → Answer is NO (no exceptions)
   - **🚨 FLATBED TRUCK DUALLY TIPS (CRITICAL - HIGH FALSE POSITIVE RATE)**: 
      * Flatbed trucks have a FLAT platform/deck - they do NOT have wide body or fender flares like box trucks
      * The flat platform does NOT indicate Dually - many flatbeds have SINGLE rear wheels
      * **⚠️ TANDEM AXLE CHECK (VERY IMPORTANT FOR FLATBEDS):**
        - Many flatbeds have MULTIPLE REAR AXLES (tandem or tri-axle) - these are NOT Dually!
        - Look at rear wheels: if tires are arranged ONE BEHIND ANOTHER (lengthwise/front-to-back) → This is TANDEM AXLE, NOT Dually
        - Dually = 2 wheels SIDE-BY-SIDE on SINGLE axle. Tandem = multiple axles arranged lengthwise
        - If you see 2+ rows of tires going front-to-back → Answer is NO (tandem axle, not dually)
      * For flatbeds, you MUST clearly see the REAR WHEELS on a SINGLE AXLE showing dual configuration (side-by-side)
      * Look directly at the rear axle area: can you see TWO separate wheels/tires SIDE-BY-SIDE (not front-to-back) on EACH side?
      * A wide flatbed platform/deck is NOT evidence of Dually
      * Stake pockets, rails, or headache racks are NOT evidence of Dually
      * Multiple tires visible could be tandem axle - verify they are SIDE-BY-SIDE on single axle
      * If you cannot clearly see TWO wheels SIDE-BY-SIDE per rear side on a SINGLE axle → Answer is NO
      * When in doubt for flatbed trucks → Answer is NO (no exceptions)
   - **PICKUP TRUCK DUALLY TIPS**: Look for wide rear fenders that extend beyond the cab, double rear wheels visible from rear/side/3-quarter view, and front wheel hub extensions

7. **Side Profile**: Rear appears noticeably wider/taller than front when viewed from side

8. **Shadows and Gaps**: Look for shadows or gaps between dual rear wheels

==== HOW TO HANDLE UNCERTAINTY ====
- **Use ALL views in the mosaic**: If one view doesn't show rear wheels clearly, check other views from different angles
- **If rear wheels are NOT clearly visible in one view**: Check other views - side views, rear views, or 3/4 views may show the wheels better
- **🚨 CRITICAL - If this is a UTILITY/SERVICE TRUCK:**
  * If rear wheels are NOT clearly visible in ANY of the views → Answer is NO
  * If you see wide body but cannot see actual wheels → Answer is NO
  * If you're basing decision on body width, fender appearance, or inferences → Answer is NO
  * The ONLY valid reason to answer YES: "I can clearly see TWO separate wheel rims on each rear side in at least one of the views"
- **🚨 CRITICAL - If this is a FLATBED TRUCK:**
  * Flatbeds do NOT have fender flares or wide bodies like box trucks - the platform is just flat
  * **TANDEM AXLE CHECK:** If you see tires arranged ONE BEHIND ANOTHER (lengthwise) → This is tandem axle, NOT Dually. Answer is NO.
  * Dually = 2 wheels SIDE-BY-SIDE on SINGLE axle. Tandem = multiple axles arranged front-to-back.
  * If rear wheels are NOT clearly visible in ANY of the views → Answer is NO
  * If you see a flat platform/deck but cannot see actual wheels showing dual configuration on a SINGLE axle → Answer is NO
  * The ONLY valid reason to answer YES: "I can clearly see TWO separate wheel rims SIDE-BY-SIDE on each rear side on a SINGLE axle"
  * Platform width, stake pockets, rails, multiple tires arranged lengthwise are NOT evidence of Dually
- **If you see wide body but unclear wheels (non-utility vehicles)**: Check multiple views for fender flare, wheel well width, and vehicle type
- **If it's a Box Truck/Cutaway/Stepvan**: Assume Dually UNLESS you clearly see a single thin rear tire OR you see multiple rear axles.
- **Cross-reference between views**: If one view suggests dually but another doesn't, look for consistent indicators across multiple views

==== COMMON FALSE POSITIVES TO AVOID ====
- **MULTIPLE AXLES**: If you see tires behind other tires (lengthwise) -> **ANSWER NO**.
- **🚨 UTILITY/SERVICE TRUCK WIDE BODY (MOST COMMON FALSE POSITIVE - 80% of errors)**: 
  * Service bodies with compartments/cabinets are DESIGNED to be wide
  * This wide body is for storage and does NOT mean dual wheels
  * You MUST see the actual rear wheels to confirm
  * If rear wheels are hidden by the service body → Answer is NO
  * Body width alone is NEVER sufficient evidence for utility trucks
- **🚨 FLATBED TRUCK PLATFORM (COMMON FALSE POSITIVE)**:
  * Flatbed trucks have a FLAT platform/deck - this is NOT evidence of Dually
  * Many flatbeds have SINGLE rear wheels - the flat platform is just the body style
  * **⚠️ TANDEM AXLE TRAP:** Many flatbeds have MULTIPLE rear axles (tandem/tri-axle) with tires arranged LENGTHWISE (one behind another) - this is NOT Dually!
  * Dually = 2 wheels SIDE-BY-SIDE on SINGLE axle. If tires go front-to-back → tandem axle, NOT dually
  * You MUST see actual dual rear wheels SIDE-BY-SIDE on a SINGLE axle to confirm Dually on a flatbed
  * Stake pockets, rails, headache racks are NOT evidence of Dually
  * Multiple tires arranged lengthwise (front-to-back) = tandem axle = NOT Dually
  * If rear wheels are not clearly visible showing dual configuration on SINGLE axle → Answer is NO
- Wide service body does NOT automatically mean Dually (but check other cues!)
- Single wheel with decorative hub cap (look for two separate wheels, not one wide wheel)
- Dirt/shadows that look like extra tires (verify actual wheel shapes)


==== DECISION LOGIC ====
Answer "YES" if ALL of these are true:
1. You can clearly see TWO separate wheels/rims on the rear (per side) OR distinctive dually fenders/width.
2. The vehicle has only ONE rear axle (not a tandem/tri-axle setup).
3. **🚨 ADDITIONAL CHECK FOR UTILITY/SERVICE TRUCKS:** If this is a utility/service truck, you MUST be able to see the actual rear wheels showing dual configuration. Body width and fender appearance alone are NOT sufficient.
4. **🚨 ADDITIONAL CHECK FOR FLATBED TRUCKS:** If this is a flatbed truck, you MUST be able to see the actual rear wheels showing dual configuration. Platform width and accessories are NOT sufficient.

Answer "NO" if ANY of these are true:
1. You clearly see a SINGLE thin rear tire with no dual pattern.
2. Rear width is same as front with no fender flare.
3. **The vehicle has MULTIPLE REAR AXLES (tires arranged lengthwise/tandem).**
4. **🚨 It's a UTILITY/SERVICE TRUCK and you CANNOT clearly see the rear wheels in ANY view.**
5. **🚨 It's a UTILITY/SERVICE TRUCK and your only evidence is wide body or fender appearance.**
6. **🚨 It's a UTILITY/SERVICE TRUCK and rear wheels are hidden/obscured by service body compartments.**
7. **🚨 It's a FLATBED TRUCK and you CANNOT clearly see TWO separate rear wheels SIDE-BY-SIDE on a SINGLE axle.**
8. **🚨 It's a FLATBED TRUCK and your only evidence is platform width, stake pockets, or rails.**
9. **🚨 It's a FLATBED TRUCK and you see tires arranged LENGTHWISE/FRONT-TO-BACK (tandem axle = NOT Dually).**


**SPECIAL INSTRUCTION FOR UNCERTAINTY:**
- For Box Truck/Cutaway/Stepvan: When in doubt, lean towards "YES" if multiple secondary indicators are present.
- **For Utility/Service Truck: CRITICAL - Answer is "NO" unless you can SEE actual dual rear wheels. Fender flare, body width, and all other indicators are NOT sufficient. If rear wheels not visible → Answer must be "NO".**
- **For Flatbed Truck: CRITICAL - Answer is "NO" unless you can SEE actual dual rear wheels. Platform width, stakes, rails are NOT evidence of Dually. If rear wheels not visible → Answer must be "NO".**
- For other vehicle types: Require clear visual evidence (dual wheels or fender flare) to answer "YES".


==== RESPONSE FORMAT ====
Respond with ONLY one of these formats:
- "YES - [specific reason: what visual cues you saw]"
- "NO - [specific reason: why you're certain it's single wheel OR multi-axle]"

Examples:
- "YES - I can see two distinct wheel rims on each rear side with a gap between them"
- "YES - Box truck with distinctive rear fender flare extending beyond cab width"
- "NO - I can clearly see a single thin rear tire on each side with no dual pattern"
- "NO - Vehicle has tandem rear axles (multiple tires in length), which is excluded"
"""

    parts = [prompt_text]
    # Add mosaic image (combines all images into one for token efficiency)
    parts.append({
        "inline_data": {
            "mime_type": "image/jpeg", 
            "data": base64.b64encode(mosaic_image).decode("utf-8")
        }
    })
    
    print(f"[DUALLY VERIFY] Ad {ad_id}: Sending mosaic of {len(valid_images)} image(s) for verification (COST OPTIMIZED)")
    
    max_retries = 3
    attempt = 0
    
    while attempt < max_retries:
        try:
            # --- YODA INTERVENTION START ---
            if _current_key_info is None:
                if not get_new_key(key_queue): 
                    raise AllKeysExhaustedError("No keys")

            current_idx = _current_key_info['original_index']
            valid_key_idx, wait_time = yoda_instance.get_usable_key(current_idx)
            
            if valid_key_idx is None:
                log_msg(f"🧘 Yoda says: All keys busy. Meditating for {wait_time:.1f}s...", worker_id)
                time.sleep(wait_time)
                continue 
            
            if valid_key_idx != current_idx:
                new_key_info = next((k for k in config.gemini_api_keys_info if k['original_index'] == valid_key_idx), None)
                if new_key_info:
                    _current_key_info = new_key_info
                    log_msg(f"🔄 Yoda Swapped: Key #{current_idx} -> Key #{valid_key_idx}", worker_id)
            # --- YODA INTERVENTION END ---

            if status_queue:
                status_queue.put({
                    "worker_id": worker_id, "state": "VERIFYING_DUALLY", "ad_id": ad_id,
                    "key_idx": _current_key_info['original_index'], "key_total": len(config.gemini_api_keys)
                })

            log_msg(f"🔍 Verifying Dually for Ad {ad_id} (Key #{_current_key_info['original_index']})...", worker_id)
            model = setup_genai_client()
            
            t_start = time.time()
            with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
                response = model.generate_content(parts, request_options={'timeout': 45})  # OPTIMIZED: 90s -> 45s
            duration = time.time() - t_start

            key_idx = _current_key_info['original_index']
            _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['success'] += 1
            
            in_tok = getattr(response.usage_metadata, 'prompt_token_count', 0)
            out_tok = getattr(response.usage_metadata, 'candidates_token_count', 0)
            _token_usage_stats['total_tokens'] += (in_tok + out_tok)
            _token_usage_stats['api_calls'] += 1
            
            # Print LLM response to terminal
            print(f"\n{'='*80}")
            print(f"[DUALLY VERIFICATION - Ad {ad_id}] LLM Response:")
            print(f"{'='*80}")
            print(response.text)
            print(f"{'='*80}\n")
            
            # Parse response
            response_text = response.text.strip().upper()
            is_dually = response_text.startswith("YES")
            
            # Confidence based on response clarity
            confidence = 95.0 if is_dually else 5.0
            
            log_msg(f"📥 Dually Verification Result: {'✅ CONFIRMED' if is_dually else '❌ NOT DUALLY'} ({duration:.1f}s)", worker_id)
            
            return is_dually, confidence, in_tok, out_tok

        except Exception as e:
            error_msg = str(e).lower()
            
            if "api_key_invalid" in error_msg:
                log_msg(f"❌ Key #{_current_key_info['original_index']} INVALID. Switching key...", worker_id)
                if status_queue: status_queue.put({"type": "key_exhausted"})
                if not get_new_key(key_queue): 
                    raise AllKeysExhaustedError("No more API keys available.")
                attempt = 0  # Reset attempts with new key
                continue

            elif any(x in error_msg for x in ["quota", "resource", "429", "500", "502", "503", "504", "deadline", "timeout"]):
                attempt += 1
                
                if attempt == 1:
                    if status_queue and "429" in error_msg: status_queue.put({"type": "rate_limit"})
                    log_msg(f"⚠️ Dually Verification Hiccup (Strike 1). Waiting 3s. Error: {e}", worker_id)
                    time.sleep(3)  # OPTIMIZED: 5s -> 3s
                
                elif attempt == 2:
                    if status_queue and "429" in error_msg: status_queue.put({"type": "rate_limit"})
                    log_msg(f"🧊 Dually Verification (Strike 2). Switching key...", worker_id)
                    # Switch to next key instead of waiting
                    key_idx = _current_key_info['original_index']
                    _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['quota_failure'] += 1
                    if not get_new_key(key_queue):
                        raise AllKeysExhaustedError("No more API keys available.")
                    attempt = 0  # Reset with new key
                    continue
                
                elif attempt >= max_retries:
                    log_msg(f"💀 Key #{_current_key_info['original_index']} exhausted. Switching...", worker_id)
                    key_idx = _current_key_info['original_index']
                    _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['quota_failure'] += 1
                    if status_queue: status_queue.put({"type": "key_exhausted"})
                    if not get_new_key(key_queue):
                        raise AllKeysExhaustedError("No more API keys available.")
                    attempt = 0  # Reset with new key
            else:
                log_msg(f"❌ Unexpected Dually Verification Error: {e}", worker_id)
                raise e
    
    return False, 0.0, 0, 0

# new code for classification

