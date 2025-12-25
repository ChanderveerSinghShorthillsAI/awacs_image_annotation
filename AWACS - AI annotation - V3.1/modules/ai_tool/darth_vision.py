import cv2
import numpy as np
from .config_loader import config

def inspect_for_dually(img_bytes, debug=False):
    """
    ADVANCED Dually Detection using multiple detection methods:
    
    A DUALLY truck has these distinctive features:
    1. DUAL REAR WHEELS - Two wheels "sandwiched" together on each side (4 rear tires total)
    2. FLARED REAR FENDERS - The "hips" that bulge out past the cab width  
    3. WIDER REAR PROFILE - Rear is ~8ft wide vs ~6.5ft for standard trucks
    
    This algorithm uses MULTIPLE detection methods:
    - Method 1: Ellipse detection for wheels (handles perspective better than circles)
    - Method 2: Contour analysis for fender bulge detection
    - Method 3: Width profile analysis at different heights
    - Method 4: Edge density in wheel well areas
    - Method 5: Aspect ratio analysis of vehicle silhouette
    
    Returns: (True/False, Confidence_Score 0-100)
    """
    try:
        threshold = getattr(config, 'darth_cv2_dually_threshold', 50)
        
        nparr = np.frombuffer(img_bytes, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if image is None:
            return False, 0.0

        h, w = image.shape[:2]
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        
        # Initialize scores for each detection method
        scores = {
            'ellipse_wheels': 0,      # Ellipse-based wheel pair detection
            'contour_bulge': 0,       # Fender bulge in contour shape
            'width_profile': 0,       # Width analysis at different heights
            'edge_density': 0,        # Edge complexity in wheel areas
            'silhouette': 0           # Overall vehicle silhouette analysis
        }
        
        # Initialize variables for false positive checks (will be updated by detection methods)
        ellipses = []
        cab_width = 0
        rear_width = 0
        avg_density = 0
        left_density = 0
        right_density = 0
        aspect = 1.0
        
        # =================================================================
        # METHOD 1: ELLIPSE DETECTION FOR WHEELS
        # Wheels appear as ellipses in photos (better than circle detection)
        # =================================================================
        wheel_region = gray[int(h*0.5):, :]  # Bottom half where wheels are
        
        # Apply adaptive thresholding for better edge detection
        blurred = cv2.GaussianBlur(wheel_region, (5, 5), 0)
        edges = cv2.Canny(blurred, 30, 100)
        
        # Find contours that could be wheels
        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        ellipses = []
        for cnt in contours:
            if len(cnt) >= 5:  # Minimum points needed to fit ellipse
                try:
                    ellipse = cv2.fitEllipse(cnt)
                    (cx, cy), (ma, MA), angle = ellipse
                    
                    # Filter for wheel-like ellipses
                    aspect = min(ma, MA) / (max(ma, MA) + 1e-5)
                    area = np.pi * ma * MA / 4
                    
                    # Wheels are roughly circular (aspect 0.5-1.0) and reasonable size
                    if 0.4 < aspect < 1.0 and 500 < area < 50000:
                        ellipses.append((int(cx), int(cy), int(ma), int(MA)))
                except:
                    pass
        
        # Look for PAIRS of ellipses close together (dual wheel pattern)
        if len(ellipses) >= 2:
            ellipses.sort(key=lambda e: e[0])  # Sort by x-coordinate
            
            dual_pairs = 0
            for i in range(len(ellipses) - 1):
                x1, y1, _, _ = ellipses[i]
                x2, y2, _, _ = ellipses[i + 1]
                
                # Check if ellipses are at similar height and close together
                y_diff = abs(y1 - y2)
                x_diff = abs(x1 - x2)
                
                # Dual wheels are close together horizontally but aligned vertically
                if y_diff < 50 and 20 < x_diff < 150:
                    dual_pairs += 1
            
            scores['ellipse_wheels'] = min(25, dual_pairs * 15)
            
            if debug:
                print(f"  Ellipses found: {len(ellipses)}, Dual pairs: {dual_pairs}, Score: {scores['ellipse_wheels']}")
        
        # =================================================================
        # METHOD 2: CONTOUR BULGE DETECTION (Fender Flares)
        # Look for the distinctive "hip" shape in the vehicle contour
        # =================================================================
        
        # Get the main vehicle contour
        _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        contours, _ = cv2.findContours(binary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        if contours:
            # Get the largest contour (likely the vehicle)
            main_contour = max(contours, key=cv2.contourArea)
            
            # Analyze the contour width at different heights
            x, y, cw, ch = cv2.boundingRect(main_contour)
            
            if ch > 50:  # Ensure contour is large enough
                # Measure width at different vertical positions
                widths = []
                for row_pct in [0.3, 0.5, 0.7, 0.9]:  # 30%, 50%, 70%, 90% from top
                    row = int(y + ch * row_pct)
                    row_pixels = binary[row, x:x+cw] if row < h else []
                    if len(row_pixels) > 0:
                        non_zero = np.where(row_pixels > 0)[0]
                        if len(non_zero) > 0:
                            widths.append(non_zero[-1] - non_zero[0])
                        else:
                            widths.append(0)
                    else:
                        widths.append(0)
                
                # Check for "bulge" pattern: bottom wider than middle/top
                if len(widths) >= 4 and widths[0] > 0:
                    top_width = widths[0]
                    mid_width = widths[1]
                    lower_width = widths[2]
                    bottom_width = widths[3]
                    
                    # Dually pattern: bottom is significantly wider
                    if bottom_width > top_width * 1.1:  # 10% wider at bottom
                        bulge_ratio = bottom_width / top_width
                        scores['contour_bulge'] = min(25, int((bulge_ratio - 1.0) * 80))
                    
                    # Also check for "hip" pattern (widest in lower-middle area)
                    if lower_width > mid_width * 1.05 and lower_width > top_width * 1.05:
                        scores['contour_bulge'] += 5
                    
                    if debug:
                        print(f"  Width profile [top→bottom]: {widths}, Score: {scores['contour_bulge']}")
        
        # =================================================================
        # METHOD 3: WIDTH PROFILE ANALYSIS
        # Compare width at cab level vs rear fender level
        # =================================================================
        
        # Use edge detection on full image
        edges_full = cv2.Canny(gray, 50, 150)
        
        # Analyze width at different height bands
        def get_row_width(edge_img, row_start_pct, row_end_pct):
            r_start = int(edge_img.shape[0] * row_start_pct)
            r_end = int(edge_img.shape[0] * row_end_pct)
            region = edge_img[r_start:r_end, :]
            
            cols_with_edges = np.any(region > 0, axis=0)
            if np.any(cols_with_edges):
                left = np.argmax(cols_with_edges)
                right = len(cols_with_edges) - np.argmax(cols_with_edges[::-1]) - 1
                return right - left
            return 0
        
        cab_width = get_row_width(edges_full, 0.1, 0.3)      # Top portion (cab)
        mid_width = get_row_width(edges_full, 0.4, 0.6)      # Middle portion
        rear_width = get_row_width(edges_full, 0.7, 0.95)    # Lower portion (rear fenders)
        
        if cab_width > 50:  # Ensure we have valid measurements
            rear_to_cab_ratio = rear_width / cab_width
            
            # Duallys typically have rear 15-30% wider than cab
            if rear_to_cab_ratio > 1.1:
                scores['width_profile'] = min(25, int((rear_to_cab_ratio - 1.0) * 100))
            
            if debug:
                print(f"  Cab width: {cab_width}, Rear width: {rear_width}, Ratio: {rear_to_cab_ratio:.2f}, Score: {scores['width_profile']}")
        
        # =================================================================
        # METHOD 4: EDGE DENSITY IN WHEEL AREAS
        # Dual wheels create more edge complexity than single wheels
        # =================================================================
        
        # Define wheel well regions (bottom corners of image)
        ww_height = int(h * 0.35)  # Bottom 35%
        ww_width = int(w * 0.35)   # Side 35%
        
        left_wheel_area = edges_full[h-ww_height:, :ww_width]
        right_wheel_area = edges_full[h-ww_height:, w-ww_width:]
        
        # Calculate edge density
        left_density = np.sum(left_wheel_area > 0) / (left_wheel_area.size + 1)
        right_density = np.sum(right_wheel_area > 0) / (right_wheel_area.size + 1)
        avg_density = (left_density + right_density) / 2
        
        # Higher edge density in wheel areas suggests dual wheels
        # Typical values: single wheel ~0.05-0.10, dual wheels ~0.10-0.20
        if avg_density > 0.08:
            scores['edge_density'] = min(15, int((avg_density - 0.05) * 200))
        
        if debug:
            print(f"  Edge density (L/R): {left_density:.3f}/{right_density:.3f}, Score: {scores['edge_density']}")
        
        # =================================================================
        # METHOD 5: SILHOUETTE ASPECT RATIO
        # Duallys have a wider, more "squat" appearance
        # =================================================================
        
        if contours:
            main_contour = max(contours, key=cv2.contourArea)
            x, y, cw, ch = cv2.boundingRect(main_contour)
            
            # Calculate aspect ratio (width/height)
            aspect = cw / (ch + 1e-5)
            
            # Duallys tend to have wider aspect ratio (>1.5) due to fender flares
            if aspect > 1.3:
                scores['silhouette'] = min(10, int((aspect - 1.2) * 30))
            
            if debug:
                print(f"  Silhouette aspect ratio: {aspect:.2f}, Score: {scores['silhouette']}")
        
        # =================================================================
        # FALSE POSITIVE REDUCTION CHECKS
        # =================================================================
        
        false_positive_penalty = 0
        fp_reasons = []
        
        # FP CHECK 1: Width ratio too extreme (>1.4 is suspicious - might be a trailer or wide body)
        if cab_width > 50 and rear_width > 0:
            ratio = rear_width / cab_width
            if ratio > 1.4:
                false_positive_penalty += 15
                fp_reasons.append(f"extreme_width_ratio:{ratio:.2f}")
            elif ratio < 1.02:  # Almost same width = NOT a dually
                false_positive_penalty += 20
                fp_reasons.append(f"no_width_diff:{ratio:.2f}")
        
        # FP CHECK 2: No wheel pairs found but other scores high (suspicious)
        if scores['ellipse_wheels'] == 0 and (scores['contour_bulge'] + scores['width_profile']) > 30:
            false_positive_penalty += 15
            fp_reasons.append("no_wheels_but_high_other_scores")
        
        # FP CHECK 3: Edge density too high (might be complex background, not wheels)
        if avg_density > 0.25:
            false_positive_penalty += 10
            fp_reasons.append(f"excessive_edge_density:{avg_density:.3f}")
        
        # FP CHECK 4: Very low ellipse count but claiming dually (need at least some wheel evidence)
        if len(ellipses) < 2 and scores['ellipse_wheels'] == 0:
            # If we can't find at least 2 wheel-like shapes, reduce confidence
            false_positive_penalty += 10
            fp_reasons.append(f"too_few_ellipses:{len(ellipses)}")
        
        # FP CHECK 5: Aspect ratio too extreme (very wide image might be panoramic/multiple vehicles)
        if aspect > 2.5:
            false_positive_penalty += 15
            fp_reasons.append(f"extreme_aspect:{aspect:.2f}")
        
        # FP CHECK 6: Uniform edge density (single wheels have similar density on both sides)
        # Duallys should have HIGHER density specifically in wheel areas
        if left_density > 0 and right_density > 0:
            density_ratio = max(left_density, right_density) / (min(left_density, right_density) + 0.001)
            if density_ratio > 3.0:  # Very uneven = likely not symmetric dually
                false_positive_penalty += 10
                fp_reasons.append(f"uneven_density:{density_ratio:.2f}")
        
        if debug and fp_reasons:
            print(f"  FALSE POSITIVE CHECKS: {fp_reasons}, Penalty: -{false_positive_penalty}")
        
        # =================================================================
        # CONSENSUS REQUIREMENT (Multiple methods must agree)
        # =================================================================
        
        # Count how many methods scored above minimum threshold
        # ENHANCED: Lowered thresholds to reduce false negatives
        method_minimums = {
            'ellipse_wheels': 8,    # Lowered from 10 (more sensitive to wheel detection)
            'contour_bulge': 5,     # Lowered from 8 (more sensitive to fender bulge)
            'width_profile': 5,     # Lowered from 8 (more sensitive to width differences)
            'edge_density': 3,      # Lowered from 5 (more sensitive to edge patterns)
            'silhouette': 2         # Lowered from 3 (more sensitive to aspect ratio)
        }
        
        methods_agreeing = sum(1 for k in scores if scores[k] >= method_minimums[k])
        
        # ENHANCED: More lenient consensus to reduce false negatives
        # We prefer false positives over false negatives (LLM will verify later)
        consensus_bonus = 0
        consensus_penalty = 0
        
        if methods_agreeing >= 3:
            consensus_bonus = 15  # Strong agreement bonus (increased)
        elif methods_agreeing == 2:
            consensus_bonus = 5   # Two methods agreeing is good, give bonus
        elif methods_agreeing == 1:
            consensus_penalty = 5  # Only 1 method agrees = reduced penalty (was 15)
        else:
            consensus_penalty = 10  # No methods strongly agree = reduced penalty (was 25)
        
        if debug:
            print(f"  CONSENSUS: {methods_agreeing}/5 methods agree (min thresholds)")
            if consensus_bonus > 0:
                print(f"    Bonus: +{consensus_bonus}")
            if consensus_penalty > 0:
                print(f"    Penalty: -{consensus_penalty}")
        
        # =================================================================
        # COMBINE SCORES WITH PENALTIES
        # =================================================================
        
        raw_total = sum(scores[k] for k in scores)
        
        # Apply penalties and bonuses
        adjusted_total = raw_total - false_positive_penalty - consensus_penalty + consensus_bonus
        
        # Ensure score is within bounds
        total_score = max(0, min(100, adjusted_total))
        
        if debug:
            print(f"  SCORES: {scores}")
            print(f"  RAW: {raw_total}, FP_PENALTY: -{false_positive_penalty}, CONSENSUS: {consensus_bonus - consensus_penalty:+d}")
            print(f"  FINAL SCORE: {total_score}")
            print(f"  Threshold: {threshold}, Is Dually: {total_score >= threshold}")
        
        is_dually = total_score >= threshold
        
        return is_dually, round(total_score, 2)

    except Exception as e:
        if debug:
            print(f"  Error in dually detection: {e}")
        return False, 0.0


def inspect_for_dually_multi_angle(img_bytes, debug=False):
    """
    Try detection from multiple perspectives:
    1. Original image
    2. Horizontally flipped (truck facing other direction)
    3. Focus on left side only
    4. Focus on right side only
    
    Returns the highest confidence score found.
    """
    try:
        results = []
        
        # 1. Original image
        is_dually, score = inspect_for_dually(img_bytes, debug)
        results.append(('original', is_dually, score))
        
        # Decode image for variations
        nparr = np.frombuffer(img_bytes, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if image is None:
            return is_dually, score
        
        h, w = image.shape[:2]
        
        # 2. Horizontally flipped
        flipped = cv2.flip(image, 1)
        _, flipped_bytes = cv2.imencode('.jpg', flipped)
        is_dually_f, score_f = inspect_for_dually(flipped_bytes.tobytes(), False)
        results.append(('flipped', is_dually_f, score_f))
        
        # 3. Right half focus (if truck is on right side of image)
        right_half = image[:, w//2:]
        if right_half.size > 0:
            _, right_bytes = cv2.imencode('.jpg', right_half)
            is_dually_r, score_r = inspect_for_dually(right_bytes.tobytes(), False)
            results.append(('right_half', is_dually_r, score_r))
        
        # 4. Left half focus
        left_half = image[:, :w//2]
        if left_half.size > 0:
            _, left_bytes = cv2.imencode('.jpg', left_half)
            is_dually_l, score_l = inspect_for_dually(left_bytes.tobytes(), False)
            results.append(('left_half', is_dually_l, score_l))
        
        # Return the best result
        best = max(results, key=lambda x: x[2])
        
        if debug:
            print(f"  Multi-angle results: {results}")
            print(f"  Best: {best[0]} with score {best[2]}")
        
        return best[1], best[2]

    except Exception as e:
        if debug:
            print(f"  Error in multi-angle detection: {e}")
        return False, 0.0


# Main function used by the system - uses multi-angle detection
def inspect_for_dually_enhanced(img_bytes, debug=False):
    """
    Enhanced dually detection using multi-angle analysis.
    This is the recommended function for production use.
    """
    return inspect_for_dually_multi_angle(img_bytes, debug)


# Legacy function for backward compatibility
def inspect_for_dually_legacy(img_bytes, debug=False):
    """
    LEGACY: Original simple circle-counting approach.
    Kept for reference/comparison only.
    """
    try:
        threshold = getattr(config, 'darth_cv2_dually_threshold', 2)
        
        nparr = np.frombuffer(img_bytes, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if image is None:
            return False, 0.0

        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        edges = cv2.Canny(gray, threshold1=50, threshold2=150)
        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        tire_count = 0
        for cnt in contours:
            x, y, cw, ch = cv2.boundingRect(cnt)
            aspect_ratio = cw / (ch + 1e-5)
            if 0.7 < aspect_ratio < 1.4 and 20 < cw < 150 and 20 < ch < 150:
                tire_count += 1
        
        is_dually = tire_count >= threshold
        return is_dually, float(tire_count)

    except Exception as e:
        return False, 0.0
