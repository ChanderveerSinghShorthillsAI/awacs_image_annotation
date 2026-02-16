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

def setup_genai_client(model_name: str = None):
    if not _current_key_info:
        raise NoKeysAvailableError("Worker has no API key to use.")
    genai.configure(api_key=_current_key_info['key'])
    # Use provided model_name, or fall back to default config.gemini_model
    effective_model = model_name or config.gemini_model
    return genai.GenerativeModel(effective_model)

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
        return False, 0, 0, 0
    
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

            log_msg(f"🔍 Pre-checking for promotional/coming soon image (Ad {ad_id}) [Model: {config.gemini_model_promo_check}]...", worker_id)
            model = setup_genai_client(config.gemini_model_promo_check)
            
            t_start = time.time()
            with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
                response = model.generate_content(parts, request_options={'timeout': 30})  # OPTIMIZED: 60s -> 30s
            duration = time.time() - t_start

            key_idx = _current_key_info['original_index']
            _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['success'] += 1
            
            in_tok = getattr(response.usage_metadata, 'prompt_token_count', 0)
            out_tok = getattr(response.usage_metadata, 'candidates_token_count', 0)
            cached_tok = getattr(response.usage_metadata, 'cached_content_token_count', 0) or 0
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
            
            log_msg(f"📥 Promotional check: {'🚫 PROMOTIONAL/PLACEHOLDER' if is_promotional else '✅ REAL LISTING'} ({duration:.1f}s, Cached:{cached_tok})", worker_id)
            
            return is_promotional, in_tok, out_tok, cached_tok

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
    return False, 0, 0, 0


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
    promo_check_tokens_cached = 0
    
    # 🛡️ PRE-CHECK: Detect promotional/coming soon images BEFORE classification 🛡️
    # Skip if already checked by caller (e.g., classify_with_gemini_multi)
    if ad_img_bytes and not skip_promo_check:
        try:
            is_promotional, promo_in, promo_out, promo_cached = check_promotional_image(
                ad_img_bytes, yoda_instance, key_queue, worker_id, status_queue, ad_id
            )
            promo_check_tokens_in += promo_in
            promo_check_tokens_out += promo_out
            promo_check_tokens_cached += promo_cached
            
            if is_promotional:
                log_msg(f"🚫 Promotional/Coming Soon image detected - returning 'Image Not Clear'", worker_id)
                # Return: (results, classify_in, classify_out, classify_cached, promo_in, promo_out, promo_cached)
                return ([("Image Not Clear", 100.0)], 0, 0, 0, promo_check_tokens_in, promo_check_tokens_out, promo_check_tokens_cached)
        except Exception as e:
            log_msg(f"⚠️ Promotional check failed, proceeding with classification: {e}", worker_id)
            # Continue with classification if check fails

    prompt_text = f"""Expert vehicle classifier. Image may be mosaic (2 angles). Context: "{breadcrumb}"

RULES:
1. Ladder Rack: Cabinets/Compartments=Utility Truck | Stakes/Slats=Contractor Truck

2. DUALLY (CRITICAL): 4 rear tires (2/side). Attribute, not body type. Include as SECONDARY if detected.
   PRIMARY CUES: 1)Dual wheel pattern-two circles/rims per side with gap 2)Rear fender flare-hip bulge wider than front 3)Dual rim dish/concave 4)Wider rear wheel wells
   SECONDARY: Front hub extensions, shadows/gaps between wheels, dual rim patterns
   VEHICLE TYPES (commonly dually): Box Truck/Straight(90%), Cutaway-Cube(90%), Stepvan(95%), Cabover/COE(80%), Cab-Chassis(70%), Utility/Service(60%), Pickup F-350/RAM3500(30%), Flatbed(50%), Contractor(40%)
   UNCERTAINTY: If rear unclear→check front hubs+fender+type. Box/Cutaway/Stepvan→assume Dually unless single tire visible
   FALSE POSITIVES AVOID: Wide body alone≠Dually. Single wheel+hub cap≠dual. Dirt/shadows≠tires
   INCLUDE DUALLY IF: See 2 wheels/rims rear OR fender flare+typical type OR dual dish+wide rear OR Box/Cutaway/Stepvan without single tire OR multiple secondary indicators
   EXCLUDE IF: Single tire visible OR same front/rear width OR certain single-wheel
   OUTPUT: Primary body first, then Dually as 2nd. Ex: 1.Box Truck(95%) 2.Dually(90%)

3. "Image Not Clear" STRICT: Image passed pre-filter. Use ONLY if: completely black/white/corrupt/failed load/zero features/100% placeholder. MUST classify if ANY vehicle visible (blurry/dark/partial/obscured/distant/poor quality OK). If see wheels/cab/bed/body/bumper/outline→CLASSIFY. Job=classify vehicles not judge quality.

OUTPUT: Numbered list with scores only. Ex: 1.Pickup Truck(98%) 2.Flatbed Truck(15%)
"""
    parts = [prompt_text]
    if ad_img_bytes:
        parts.append({"inline_data": {"mime_type": "image/jpeg", "data": base64.b64encode(ad_img_bytes).decode("utf-8")}})
    else:
        # Return: (results, classify_in, classify_out, classify_cached, promo_in, promo_out, promo_cached)
        return ([("Image Not Clear", 100.0)], 0, 0, 0, promo_check_tokens_in, promo_check_tokens_out, promo_check_tokens_cached)
    
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

            log_msg(f"📤 Sending Request (Key #{_current_key_info['original_index']}) [Model: {config.gemini_model_classification}]...", worker_id)
            model = setup_genai_client(config.gemini_model_classification)
            
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
            
            cached_tok = getattr(response.usage_metadata, 'cached_content_token_count', 0) or 0
            log_msg(f"📥 Response ({duration:.1f}s): Tokens In:{in_tok}/Out:{out_tok} (Cached:{cached_tok})", worker_id)
            
            # Note: No time.sleep() needed here because Yoda handles the pacing!
            # Return: (results, classify_in, classify_out, classify_cached, promo_in, promo_out, promo_cached)
            return parse_gemini_response(response.text), in_tok, out_tok, cached_tok, promo_check_tokens_in, promo_check_tokens_out, promo_check_tokens_cached

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
        return None, 0, 0, 0

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

            log_msg(f"📤 Sending Refinement Request [Model: {config.gemini_model_classification}]...", worker_id)
            model = setup_genai_client(config.gemini_model_classification)
            with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
                 response = model.generate_content([prompt, {"inline_data": {"mime_type": "image/jpeg", "data": base64.b64encode(ad_img_bytes).decode("utf-8")}}], request_options={'timeout': 45})  # OPTIMIZED: 90s -> 45s
            
            key_idx = _current_key_info['original_index']
            _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['success'] += 1
            
            in_tok = getattr(response.usage_metadata, 'prompt_token_count', 0)
            out_tok = getattr(response.usage_metadata, 'candidates_token_count', 0)
            cached_tok = getattr(response.usage_metadata, 'cached_content_token_count', 0) or 0
            _token_usage_stats['total_tokens'] += (in_tok + out_tok)
            _token_usage_stats['api_calls'] += 1
            
            # Print LLM response to terminal
            print(f"\n{'='*80}")
            print(f"[REFINEMENT - Ad {ad_id}] LLM Response:")
            print(f"{'='*80}")
            print(response.text)
            print(f"{'='*80}\n")
            
            log_msg(f"📥 Refinement Response: {repr(response.text)} (In:{in_tok}/Out:{out_tok}, Cached:{cached_tok})", worker_id)
            
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
            
            return result_str, in_tok, out_tok, cached_tok
                
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
        res, cls_in, cls_out, cls_cached, p_in, p_out, p_cached = classify_with_gemini(breadcrumb, category_data, None, yoda_instance, key_queue, worker_id, status_queue, ad_id, skip_promo_check=True)
        return res, cls_in, cls_out, cls_cached, p_in, p_out, p_cached
    
    # Separate token tracking for accurate per-model costing
    total_classify_in = 0
    total_classify_out = 0
    total_classify_cached = 0
    total_promo_in = 0
    total_promo_out = 0
    total_promo_cached = 0
    all_results = []
    
    # 🛡️ PRE-CHECK: Check first image for promotional/coming soon BEFORE processing
    # This saves API costs by catching promotional images early
    if img_bytes_list and len(img_bytes_list) > 0:
        try:
            is_promotional, promo_in, promo_out, promo_cached = check_promotional_image(
                img_bytes_list[0], yoda_instance, key_queue, worker_id, status_queue, ad_id
            )
            total_promo_in += promo_in
            total_promo_out += promo_out
            total_promo_cached += promo_cached
            
            if is_promotional:
                log_msg(f"🚫 Promotional/Coming Soon image detected on first image - returning 'Image Not Clear'", worker_id)
                return [("Image Not Clear", 100.0)], 0, 0, 0, total_promo_in, total_promo_out, total_promo_cached
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
            
            res, cls_in, cls_out, cls_cached, p_in, p_out, p_cached = classify_with_gemini(breadcrumb, category_data, mosaic_bytes, yoda_instance, key_queue, worker_id, status_queue, ad_id, skip_promo_check=True)
            total_classify_in += cls_in
            total_classify_out += cls_out
            total_classify_cached += cls_cached
            total_promo_in += p_in
            total_promo_out += p_out
            total_promo_cached += p_cached
            all_results.extend(res)
            
        except Exception as e:
            log_msg(f"⚠️ Mosaic failed ({e}). Falling back to single image.", worker_id)
            # Fallback to single image
            res, cls_in, cls_out, cls_cached, p_in, p_out, p_cached = classify_with_gemini(breadcrumb, category_data, img_bytes_list[0], yoda_instance, key_queue, worker_id, status_queue, ad_id, skip_promo_check=True)
            total_classify_in += cls_in
            total_classify_out += cls_out
            total_classify_cached += cls_cached
            total_promo_in += p_in
            total_promo_out += p_out
            total_promo_cached += p_cached
            all_results.extend(res)
    else:
        # Single Image Case
        log_msg(f"📸 Single Image Classification...", worker_id)
        res, cls_in, cls_out, cls_cached, p_in, p_out, p_cached = classify_with_gemini(breadcrumb, category_data, img_bytes_list[0], yoda_instance, key_queue, worker_id, status_queue, ad_id, skip_promo_check=True)
        total_classify_in += cls_in
        total_classify_out += cls_out
        total_classify_cached += cls_cached
        total_promo_in += p_in
        total_promo_out += p_out
        total_promo_cached += p_cached
        all_results.extend(res)
    # -----------------------------

    if not all_results:
        return [], total_classify_in, total_classify_out, total_classify_cached, total_promo_in, total_promo_out, total_promo_cached

    combined = {cat: score for cat, score in all_results if cat and (cat not in (c:={}) or score > c[cat])}
    normalized = {cat: round(score - (score - 90) * 0.8, 1) if score > 95 else round(score, 1) for cat, score in combined.items()}
    final_res = sorted(normalized.items(), key=lambda x: x[1], reverse=True)[:3]
    
    # Return: (results, classify_in, classify_out, classify_cached, promo_in, promo_out, promo_cached)
    return final_res, total_classify_in, total_classify_out, total_classify_cached, total_promo_in, total_promo_out, total_promo_cached

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
        return False, 0.0, 0, 0, 0
    
    # Create mosaic from all available images for efficiency (reduces tokens significantly)
    mosaic_image = create_image_mosaic_multi(valid_images)
    image_count_text = f"{len(valid_images)} image{'s' if len(valid_images) > 1 else ''}"
    
    # Dually verification prompt (compressed)
    mosaic_text = f"MOSAIC {image_count_text} side-by-side. Examine ALL views" if len(valid_images) > 1 else "1 image"
    
    prompt_text = f"""Expert wheel configuration analyst. Determine: DUAL REAR WHEELS (Dually) on SINGLE REAR AXLE?

Image: {mosaic_text}. ⚠️False negatives critical-check ALL indicators.

DUALLY DEFINITION: 2 wheels/tires per side (4 rear total). ONE rear axle only. Hip bulge. Flared fenders.

EXCLUSION: Multiple axles (tandem/tri-axle)=NO. Tires lengthwise (row behind row)=NO. Want 4 rear tires only, not 8+.

PRIMARY CUES: 1)Dual pattern-TWO circles/rims per side, gap/shadow, sandwiched 2)Fender flare-hip bulge, rear wider than front 3)Dual rim dish-concave/recessed 4)Wider rear wheel wells vs front

SECONDARY: Front hub extensions protruding. Shadows/gaps between rear wheels. Vehicle types: Box/Straight(90%), Cutaway-Cube(90%), Stepvan(95%), Cabover/COE(80%), Pickup F-350/RAM3500(30%). Utility/Service: check rear wheels below body, fender at axle. Rear wider/taller side profile.

UNCERTAINTY: Use ALL views. If rear unclear→check other angles/views. Wide body but unclear→check fender/wells/type. Box/Cutaway/Stepvan→assume YES unless single tire visible OR multi-axle. Cross-reference views for consistency.

FALSE POSITIVES: Multi-axle=NO. Wide body alone≠dually. Single wheel+hub cap≠dual. Dirt/shadow≠tire.

DECISION: YES if: 2 wheels/rims rear visible OR fender flare+typical type AND single axle only
NO if: Single tire clear OR same width front/rear OR multi-axle (lengthwise/tandem)

FORMAT: "YES - [reason]" or "NO - [reason]"
Examples: "YES-two rims each side with gap"|"YES-Box truck fender flare beyond cab"|"NO-single tire visible"|"NO-tandem axles"
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

            log_msg(f"🔍 Verifying Dually for Ad {ad_id} (Key #{_current_key_info['original_index']}) [Model: {config.gemini_model_dually_verification}]...", worker_id)
            model = setup_genai_client(config.gemini_model_dually_verification)
            
            t_start = time.time()
            with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
                response = model.generate_content(parts, request_options={'timeout': 45})  # OPTIMIZED: 90s -> 45s
            duration = time.time() - t_start

            key_idx = _current_key_info['original_index']
            _key_usage_stats.setdefault(key_idx, {'success': 0, 'quota_failure': 0})['success'] += 1
            
            in_tok = getattr(response.usage_metadata, 'prompt_token_count', 0)
            out_tok = getattr(response.usage_metadata, 'candidates_token_count', 0)
            cached_tok = getattr(response.usage_metadata, 'cached_content_token_count', 0) or 0
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
            
            log_msg(f"📥 Dually Verification Result: {'✅ CONFIRMED' if is_dually else '❌ NOT DUALLY'} ({duration:.1f}s, Cached:{cached_tok})", worker_id)
            
            return is_dually, confidence, in_tok, out_tok, cached_tok

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
    
    return False, 0.0, 0, 0, 0

# new code for classification

