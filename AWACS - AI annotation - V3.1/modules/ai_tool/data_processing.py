import os
import re
import json
import base64
import sys
from .utils import log_msg

def load_json_file(file_path, default_data):
    if not os.path.exists(file_path):
        return default_data
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"\n❌ CRITICAL ERROR: Your '{os.path.basename(file_path)}' file is broken!")
        print(f"   Error details: {e}")
        sys.exit(1)

def load_category_data(json_path: str) -> dict:
    categories = {}
    try:
        data = load_json_file(json_path, {})
        for cat_name, details in data.items():
            image_base64 = details.get("image_base64")
            categories[cat_name] = {
                "definition": details.get("definition", ""),
                "image_bytes": base64.b64decode(image_base64) if image_base64 else None
            }
    except Exception as e:
        log_msg(f"❌ Error loading categories: {e}", -1)
    return categories

def load_rules(json_path: str) -> dict:
    default_rules = {"normalize_map": {}, "exclusion_rules": [], "truck_overlaps": []}
    data = load_json_file(json_path, default_rules)
    return {
        "normalize_map": data.get("normalize_map", {}),
        "exclusion_rules": data.get("exclusion_rules", []),
        "truck_overlaps": data.get("truck_overlaps", [])
    }

def normalize_text(text: str, normalize_map: dict, worker_id: int = -1) -> str:
    """
    Cleans and standardizes a category name string using Fuzzy Matching.
    Logs when a rule triggers a change.
    """
    if not text: return ""
    txt = str(text).strip()
    txt_lower = txt.lower()
    
    # 1. Exact Match
    for k, v in normalize_map.items():
        if txt_lower == k.lower(): 
            if txt != v and worker_id > 0:
                log_msg(f"   📏 Rule Triggered: Exact Map '{txt}' -> '{v}'", worker_id)
            return v

    # 2. Fuzzy Match (Smart)
    sorted_keys = sorted(normalize_map.keys(), key=len, reverse=True)
    
    for k in sorted_keys:
        if k.lower() in txt_lower:
            # Check if we are actually changing something significant
            if normalize_map[k] != txt and worker_id > 0:
                 log_msg(f"   📏 Rule Triggered: Fuzzy Map '{txt}' -> '{normalize_map[k]}' (matched '{k}')", worker_id)
            return normalize_map[k]

    # 3. Hardcoded Cleanups
    if "cab chassis" in txt_lower or "chassis cab" in txt_lower: 
        if worker_id > 0: log_msg(f"   📏 Rule Triggered: Hardcoded 'Cab-Chassis'", worker_id)
        return "Cab-Chassis"
    
    if "dually" in txt_lower: 
        return "Dually"
    
    return txt

def find_overlap_rule(classifications: list, overlap_rules: list, worker_id: int = -1) -> tuple | None:
    if not classifications: return None
    top_one_cat = classifications[0][0].lower()
    
    # --- 1. AGGRESSIVE DUALLY SAFEGUARD (Updated for HD Trucks) ---
    # If Dually is mentioned OR if it's a truck type that is ALMOST ALWAYS a dually
    # (Box Trucks, Cutaways, etc.), we force a visual wheel check.
    hd_trucks = ["box truck - straight truck", "cutaway-cube van", "stepvan", "cabover truck - coe"]
    is_hd_without_dually = (top_one_cat in hd_trucks and not any("dually" in c[0].lower() for c in classifications))
    
    if "dually" in str(classifications).lower() or is_hd_without_dually:
        other_cat = classifications[0][0]
        if worker_id > 0:
             log_msg(f"   ⚔️ Rule Triggered: Dually/HD Wheel Verification for '{other_cat}'", worker_id)
        
        dually_rule = {
            "decision_rule": (
                f"🔍 ENHANCED DUALLY WHEEL CHECK: You are analyzing a '{other_cat}'. Determine if it has DUAL REAR WHEELS (Dually).\n\n"
                "⚠️ CRITICAL: False negatives are a major issue - look carefully for ALL dually indicators!\n\n"
                "=== PRIMARY CHECKS (Look for ANY of these) ===\n"
                "1. **REAR WHEELS**: Can you see TWO separate wheels/rims on each rear side? (Two wheels sandwiched together)\n"
                "2. **REAR FENDERS**: Are the rear fenders noticeably WIDER than the front, creating a 'hip' bulge?\n"
                "3. **DUAL RIM PATTERN**: Do the rear rims appear deeply concave (dish-shaped) or show dual wheel assembly?\n"
                "4. **FRONT HUB EXTENSIONS**: Do the FRONT wheels have large protruding metal hub extensions? (Common on Duallys)\n"
                "5. **WHEEL WELL WIDTH**: Are the rear wheel wells noticeably wider/taller than front?\n\n"
                "=== VEHICLE TYPE CONTEXT ===\n"
                f"Vehicle Type: '{other_cat}'\n"
                "- Box Truck / Straight Truck: 90% are Duallys\n"
                "- Cutaway-Cube Van: 90% are Duallys\n"
                "- Stepvan: 95% are Duallys\n"
                "- Cab-Chassis w/ body: 70% are Duallys\n\n"
                "=== DECISION LOGIC ===\n"
                "✅ Answer 'Dually' if:\n"
                "  - You see TWO distinct wheels on rear (per side), OR\n"
                "  - You see rear fender flare + it's a commercial truck type, OR\n"
                "  - It's a Box Truck/Cutaway/Stepvan AND you don't see a single thin tire\n\n"
                "❌ Answer 'Single Wheel' ONLY if:\n"
                "  - You clearly see a SINGLE thin rear tire with no dual pattern\n\n"
                "When uncertain, default to Dually for commercial truck types.\n\n"
                "Output ONLY: Category Name + 'Dually' (if confirmed) OR just Category Name (if single wheel confirmed)"
            )
        }
        return dually_rule, [other_cat, "Dually"]

    # --- 2. STANDARD JSON OVERLAP RULES ---
    if len(classifications) < 2: return None
    top_two_cat = classifications[1][0]
    top_pair_set = {top_one_cat, top_two_cat.lower()}
    
    for rule in overlap_rules:
        if {p.lower() for p in rule.get("pair", [])} == top_pair_set:
            if worker_id > 0:
                log_msg(f"   ⚔️ Rule Triggered: Overlap Conflict {rule.get('pair')}", worker_id)
            return rule, [classifications[0][0], classifications[1][0]]
            
    return None

def handle_dually_logic(classifications: list, worker_id: int = 0) -> list:
    if len(classifications) < 2: return classifications
    dually_index = next((i for i, (cat, _) in enumerate(classifications) if cat.lower() == 'dually'), -1)
    
    if dually_index == 0:
        log_msg(f"   📏 Rule Triggered: Dually Demotion (Rank 1 -> Rank 2)", worker_id)
        classifications[0], classifications[1] = classifications[1], classifications[0]
        
    return classifications

def apply_refinement_fix(annotated_norm: list, refined_cat_norm: str, ambiguous_pair: list, worker_id: int = 0) -> list:
    if refined_cat_norm.lower() == 'dually':
        log_msg(f"   ✅ Refinement Result: Dually Confirmed.", worker_id)
        primary_cat = next((c for c in ambiguous_pair if c.lower() != 'dually'), None)
        if not primary_cat: primary_cat = "Cab-Chassis"

        new_results = []
        new_results.append((primary_cat, 99.9))
        new_results.append((refined_cat_norm, 99.8))
        
        ambiguous_lower = {p.lower() for p in ambiguous_pair}
        new_results.extend([(c, s) for c, s in annotated_norm if c.lower() not in ambiguous_lower])
        return new_results
    else:
        log_msg(f"   ✅ Refinement Result: Selected '{refined_cat_norm}'.", worker_id)
        ambiguous_lower = {c.lower() for c in ambiguous_pair}
        new_results = [(refined_cat_norm, 99.9)]
        new_results.extend([(cat, score) for cat, score in annotated_norm if cat.lower() not in ambiguous_lower])
        new_results.sort(key=lambda x: x[1], reverse=True)
        return new_results[:3]

def filter_by_exclusion_rules(annotated_norm: list, exclusion_rules: list, worker_id: int = 0) -> list:
    filtered = list(annotated_norm)
    i = 0
    while i < len(filtered):
        cat1 = filtered[i][0]
        j = i + 1
        while j < len(filtered):
            cat2 = filtered[j][0]
            conflict = False
            for rule in exclusion_rules:
                rule_cat = rule.get("category", "").strip()
                not_with = [x.strip() for x in rule.get("not_with", [])]
                
                if (cat1 == rule_cat and cat2 in not_with) or (cat2 == rule_cat and cat1 in not_with):
                    conflict = True
                    break 
            
            if conflict:
                log_msg(f"   🚫 Rule Triggered: EXCLUSION. Removing '{cat2}' because '{cat1}' is present.", worker_id)
                filtered.pop(j)
            else:
                j += 1
        i += 1
    return filtered

def determine_status(breadcrumb_list, filtered_annotated, original_annotated, has_images: bool = True):
    """
    Determine the status of an annotation.
    
    Args:
        breadcrumb_list: List of breadcrumb categories
        filtered_annotated: Filtered annotation results
        original_annotated: Original annotation results before filtering
        has_images: Whether images were available for this listing (default: True)
    """
    # If no images were available, set appropriate status
    if not has_images:
        # If we got a category despite no images, it's an error
        if filtered_annotated and filtered_annotated[0][0] != "Image Not Clear":
            return "No Images Present"
        # If AI correctly identified no images
        if filtered_annotated and filtered_annotated[0][0] == "Image Not Clear":
            return "No Images Present"
        # If no result at all
        return "No Images Present"
    
    # Handle exclusion rule conflicts
    if not filtered_annotated and original_annotated: 
        return "Exclusion rule conflict"
    
    # Handle "Image Not Clear" when images were present (this is an error case)
    if filtered_annotated and filtered_annotated[0][0] == "Image Not Clear": 
        return "Image not clear"
    
    # Normal status comparison
    bc_top_norm = [b.lower() for b in breadcrumb_list[:3] if b]
    annotated_cats_norm = [a.lower() for a, _ in filtered_annotated[:3] if a]
    if not bc_top_norm and not annotated_cats_norm: 
        return "No change"
    return "No change" if not set(bc_top_norm).symmetric_difference(set(annotated_cats_norm)) else "Require Update"