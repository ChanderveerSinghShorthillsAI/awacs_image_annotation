# ai_tool/smart_image_selector.py
from typing import List
import numpy as np

# Try importing OpenCV safely
try:
    import cv2
    OPENCV_AVAILABLE = True
except ImportError:
    OPENCV_AVAILABLE = False

def _clarity_score(img_bytes: bytes) -> float:
    """Fast blur detection – higher = sharper"""
    if not OPENCV_AVAILABLE:
        return 0.0
    try:
        arr = np.frombuffer(img_bytes, np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
        if img is None: return 0.0
        return cv2.Laplacian(img, cv2.CV_64F).var()
    except:
        return 0.0

def select_best_images(img_bytes_list: List[bytes], quick_guess: str = "") -> List[bytes]:
    """
    Vision v2 – picks the 1–3 most useful images.
    """
    # Fallback if OpenCV is not installed or too few images
    if not OPENCV_AVAILABLE or len(img_bytes_list) <= 2:
        return img_bytes_list

    guess = quick_guess.lower()
    scores = []

    for i, img_bytes in enumerate(img_bytes_list):
        # Base score on clarity
        base_score = _clarity_score(img_bytes)
        # Normalize roughly to 0-10 range for clarity
        score = base_score / 1000.0 

        try:
            arr = np.frombuffer(img_bytes, np.uint8)
            img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            if img is None:
                scores.append((i, score))
                continue
            
            h, w = img.shape[:2]

            # 1. Dually / Dual Rear Wheel detection (Cluster of edges in lower half)
            if any(k in guess for k in ["dually", "drw", "dual"]):
                lower_half = img[int(h*0.6):, :]
                gray = cv2.cvtColor(lower_half, cv2.COLOR_BGR2GRAY)
                edges = cv2.Canny(gray, 50, 150)
                if np.mean(edges) > 28:
                    score += 6.0

            # 2. Crane / Boom (Long straight lines)
            if "crane" in guess or "boom" in guess:
                gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
                edges = cv2.Canny(gray, 50, 100)
                # Probabilistic Hough Line Transform
                lines = cv2.HoughLinesP(edges, 1, np.pi/180, threshold=100, minLineLength=int(w*0.35), maxLineGap=20)
                if lines is not None and len(lines) >= 2:
                    score += 7.0

            # 3. Dump bed (Raised bed often implies high contrast between top and bottom)
            if "dump" in guess:
                upper = img[:h//3, :]
                lower = img[2*h//3:, :]
                if np.mean(upper) > np.mean(lower) + 25:
                    score += 4.0

            # 4. Side Profile (Landscape aspect ratio preferred for Flatbeds)
            if "flatbed" in guess or "stake" in guess:
                if w > h * 1.2:  # Distinctly landscape
                    score += 3.0

        except Exception:
            pass # Keep base score if CV analysis fails

        scores.append((i, score))

    # Sort by score descending and pick top 3
    best_indices = [i for i, s in sorted(scores, key=lambda x: x[1], reverse=True)[:3]]
    
    # Return the bytes in the order of their "best-ness"
    return [img_bytes_list[i] for i in best_indices]