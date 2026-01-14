# ai_tool/main_processor.py — FINAL FULL & COMPLETE VERSION
import os
import glob
import time
import pandas as pd
from datetime import datetime
from multiprocessing import Queue, Manager
import queue

from .config_loader import config
# ADDED darth_vision TO IMPORTS
from . import classification, web_utils, data_processing, utils, darth_vision
from ai_tool.rate_limiter import Yoda 


def merge_all_session_reports(run_ts):
    """Merge all per-worker reports into one final session report."""
    try:
        pattern = os.path.join(config.key_report_dir, f"*_{run_ts}.xlsx")
        worker_files = [f for f in glob.glob(pattern) if "worker" in os.path.basename(f)]
        if not worker_files:
            return

        key_dfs = []
        token_dfs = []
        for f in worker_files:
            try:
                key_df = pd.read_excel(f, sheet_name='Key Usage')
                key_df['Worker ID'] = os.path.basename(f).split('_worker_')[1].split('_')[0]
                key_dfs.append(key_df)
                try:
                    token_dfs.append(pd.read_excel(f, sheet_name='Token Usage'))
                except:
                    pass
            except:
                pass

        if key_dfs:
            final_key_df = pd.concat(key_dfs, ignore_index=True)
            final_path = os.path.join(config.key_report_dir, f"Session_Report_{run_ts}.xlsx")
            with pd.ExcelWriter(final_path) as writer:
                final_key_df.to_excel(writer, sheet_name='Key Usage Summary', index=False)
                if token_dfs:
                    pd.concat(token_dfs, ignore_index=True).to_excel(writer, sheet_name='Token Usage Summary', index=False)
            utils.log_msg(f"FINAL MERGED Session Report created: {os.path.basename(final_path)}")
    except Exception as e:
        utils.log_msg(f"Could not merge session reports: {e}")


def save_checkpoint(run_ts: str, results_so_far: list, input_df: pd.DataFrame):
    """Saves progress to the UNIQUE file for this specific run."""
    if not results_so_far:
        return

    try:
        result_df = pd.DataFrame(results_so_far)
        result_df["Ad ID"] = result_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        clean_input_df = input_df[["Ad ID"]].copy()
        clean_input_df["Ad ID"] = clean_input_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

        merged = pd.merge(clean_input_df, result_df, on="Ad ID", how="inner")

        final_columns = [
            "Ad ID", "Breadcrumb_Top1", "Breadcrumb_Top2", "Breadcrumb_Top3",
            "Annotated_Top1", "Annotated_Top2", "Annotated_Top3",
            "Annotated_Top1_Score", "Annotated_Top2_Score", "Annotated_Top3_Score",
            "Image_Count", "Image_URLs", "Status", "Cost_Cents"
        ]
        for col in final_columns:
            if col not in merged.columns: merged[col] = ""
        merged = merged.reindex(columns=final_columns)

        filename = f"output_annotated_{run_ts}.xlsx"
        file_path = os.path.join(config.output_dir, filename)

        merged.to_excel(file_path, index=False)
        utils.log_msg(f"💾 Saved progress to '{filename}' ({len(merged)} ads)")

    except PermissionError:
        utils.log_msg(f"⚠️ Save Skipped: You have '{filename}' open! Close it to allow saving.")
    except Exception as e:
        utils.log_msg(f"⚠️ Save Failed: {e}")


def _process_single_ad(ad_row: dict, category_data: dict, rules: dict, 
                       high_accuracy: bool, worker_id: int, key_queue: Queue,
                       status_queue: Queue, results_queue: Queue, 
                       use_vision_v2: bool = False, yoda_instance=None):
    
    ad_id = str(ad_row.get("Ad ID", "")).strip()
    if not ad_id: return None

    # --- COST TRACKING ---
    total_in_tokens = 0
    total_out_tokens = 0
    # ---------------------

    breadcrumb_raw = [ad_row.get(f"Breadcrumb_Top{i}", "") for i in range(1, 4)]
    if any("inactive" in str(b).lower() for b in breadcrumb_raw):
        final_row = {"Ad ID": ad_id, "Status": "Inactive ad", "Cost_Cents": 0}
        results_queue.put(final_row)
        return final_row

    breadcrumb = [data_processing.normalize_text(str(x), rules['normalize_map'])
                  for x in breadcrumb_raw if pd.notna(x) and str(x).strip()]

    raw_urls = str(ad_row.get("Image_URLs", "")).strip()
    image_urls = raw_urls.split(",") if raw_urls else []
    img_bytes_list = web_utils.get_images_with_caching(image_urls)
    
    # Check if we have any valid images
    has_valid_images = bool(img_bytes_list and any(img_bytes_list))
    
    # Early return if no images available - set appropriate status
    if not has_valid_images:
        final_row = {
            "Ad ID": ad_id,
            "Breadcrumb_Top1": breadcrumb[0] if len(breadcrumb) > 0 else "",
            "Breadcrumb_Top2": breadcrumb[1] if len(breadcrumb) > 1 else "",
            "Breadcrumb_Top3": breadcrumb[2] if len(breadcrumb) > 2 else "",
            "Annotated_Top1": "",
            "Annotated_Top2": "",
            "Annotated_Top3": "",
            "Annotated_Top1_Score": 0,
            "Annotated_Top2_Score": 0,
            "Annotated_Top3_Score": 0,
            "Image_Count": len(image_urls),
            "Image_URLs": ", ".join(image_urls) if image_urls else "",
            "Status": "No Images Present",
            "Cost_Cents": 0
        }
        results_queue.put(final_row)
        return final_row

    # === VISION V2 (OPTIMIZED) ===
    vision_result = None
    
    if use_vision_v2 and img_bytes_list and len(img_bytes_list) >= 2:
        from ai_tool.smart_image_selector import select_best_images
        try:
            # Pass Yoda
            quick_guess, t_in, t_out = classification.classify_with_gemini_multi(
                ", ".join(breadcrumb), category_data, [img_bytes_list[0]],
                fast_mode=True, key_queue=key_queue, worker_id=worker_id,
                status_queue=status_queue, ad_id=ad_id, yoda_instance=yoda_instance
            )
            total_in_tokens += t_in
            total_out_tokens += t_out
            
            if quick_guess and quick_guess[0][1] >= 98.0:
                utils.log_msg(f" [W-{worker_id}] ⚡ Vision V2 confident. Skipping 2nd call.", worker_id)
                vision_result = quick_guess
            else:
                predicted = quick_guess[0][0] if quick_guess else ""
                img_bytes_list = select_best_images(img_bytes_list, predicted)
                utils.log_msg(f" [W-{worker_id}] Vision v2 sorting for: {predicted}", worker_id)
            
        except Exception as e:
            utils.log_msg(f" [W-{worker_id}] Vision v2 failed: {e}", worker_id)

    # === CLASSIFICATION ===
    if vision_result:
        result = vision_result
    elif high_accuracy:
        utils.log_msg(f" [W-{worker_id}] HIGH ACCURACY → using 2 images", worker_id)
        result, t_in, t_out = classification.classify_with_gemini_multi(
            ", ".join(breadcrumb), category_data, img_bytes_list or [b''],
            fast_mode=False, key_queue=key_queue, worker_id=worker_id,
            status_queue=status_queue, ad_id=ad_id, yoda_instance=yoda_instance
        )
        total_in_tokens += t_in
        total_out_tokens += t_out
    else:
        result, t_in, t_out = classification.classify_with_gemini_multi(
            ", ".join(breadcrumb), category_data, [img_bytes_list[0]] if img_bytes_list else [b''],
            fast_mode=True, key_queue=key_queue, worker_id=worker_id,
            status_queue=status_queue, ad_id=ad_id, yoda_instance=yoda_instance
        )
        total_in_tokens += t_in
        total_out_tokens += t_out

    if not result:
        final_row = {"Ad ID": ad_id, "Status": "AI Error", "Cost_Cents": 0}
        results_queue.put(final_row)
        return final_row

    annotated = [(data_processing.normalize_text(c, rules['normalize_map'], worker_id), s) for c, s in result]
    
    # 🛡️ PLACEHOLDER/COMING SOON SAFEGUARD 🛡️
    # If AI detected "Image Not Clear" (which includes placeholder/coming soon images), 
    # skip all further processing and set status appropriately
    if annotated and annotated[0][0] == "Image Not Clear":
        utils.log_msg(f" [W-{worker_id}] 🚫 Placeholder/Coming Soon detected - skipping classification", worker_id)
        filtered = annotated  # Keep as-is, no filtering needed
        status = data_processing.determine_status(breadcrumb, filtered, annotated, has_images=has_valid_images)
        
        # Calculate cost before returning
        cost_cents = utils.calculate_cost_cents(total_in_tokens, total_out_tokens, config.gemini_model)
        
        final_row = {
            "Ad ID": ad_id,
            "Breadcrumb_Top1": breadcrumb[0] if len(breadcrumb) > 0 else "",
            "Breadcrumb_Top2": breadcrumb[1] if len(breadcrumb) > 1 else "",
            "Breadcrumb_Top3": breadcrumb[2] if len(breadcrumb) > 2 else "",
            "Annotated_Top1": filtered[0][0] if len(filtered) >= 1 else "",
            "Annotated_Top2": filtered[1][0] if len(filtered) >= 2 else "",
            "Annotated_Top3": filtered[2][0] if len(filtered) >= 3 else "",
            "Annotated_Top1_Score": round(filtered[0][1], 1) if len(filtered) >= 1 else 0,
            "Annotated_Top2_Score": round(filtered[1][1], 1) if len(filtered) >= 2 else 0,
            "Annotated_Top3_Score": round(filtered[2][1], 1) if len(filtered) >= 3 else 0,
            "Image_Count": len(image_urls),
            "Image_URLs": ", ".join(image_urls) if image_urls else "",
            "Status": status,
            "Cost_Cents": cost_cents
        }
        results_queue.put(final_row)
        return final_row
    
    # 🚀 AI RE-CHECK FOR "JUST DUALLY" 🚀
    if img_bytes_list and len(annotated) == 1 and annotated[0][0].lower() == "dually":
        utils.log_msg(f" [W-{worker_id}] ⚠️ AI only saw 'Dually'. Forcing Body check...", worker_id)
        
        all_categories = list(category_data.keys())
        body_options = [c for c in all_categories if c.lower() != "dually"]
        options_str = ", ".join(body_options)
        
        body_check_rule = {
            "decision_rule": (
                f"You identified this chassis as a Dually. Dually is an attribute, not a specific body type. "
                f"Analyze the rear body configuration carefully. "
                f"From the following valid categories, select the one that best describes the truck body:\n"
                f"[{options_str}]\n"
                f"Output ONLY the specific Category Name from this list."
            )
        }
        
        found_body, t_in, t_out = classification.classify_with_refinement(
            body_options, body_check_rule, img_bytes_list[0], 
            yoda_instance, key_queue, worker_id, ad_id, status_queue
        )
        total_in_tokens += t_in
        total_out_tokens += t_out
        
        if found_body:
            norm_body = data_processing.normalize_text(found_body, rules['normalize_map'], worker_id)
            utils.log_msg(f" [W-{worker_id}] -> AI Found Body: {norm_body}", worker_id)
            annotated = [(norm_body, 95.0), annotated[0]]
        else:
            annotated = [("Cab-Chassis", 50.0), annotated[0]]

    annotated = data_processing.handle_dually_logic(annotated, worker_id)

    # REFINEMENT (Overlap Rules)
    if img_bytes_list and len(annotated) > 1 and annotated[0][1] < 95.0:
        if overlap_result := data_processing.find_overlap_rule(annotated, rules.get('truck_overlaps', []), worker_id):
            rule_dict, pair = overlap_result
            
            refined, t_in, t_out = classification.classify_with_refinement(
                pair, rule_dict, img_bytes_list[0],
                yoda_instance, key_queue, worker_id, ad_id, status_queue
            )
            total_in_tokens += t_in
            total_out_tokens += t_out
            
            if refined:
                refined_norm = data_processing.normalize_text(refined, rules['normalize_map'], worker_id)
                annotated = data_processing.apply_refinement_fix(annotated, refined_norm, pair, worker_id)

    filtered = data_processing.filter_by_exclusion_rules(annotated, rules['exclusion_rules'], worker_id)
    
    # =================================================================================
    # 🚀 ENHANCED DUALLY DETECTION - TWO-STAGE VERIFICATION 🚀
    # Stage 1: CV2 local detection (Darth Vader)
    # Stage 2: LLM verification for high-probability dually types
    # =================================================================================
    has_dually_before = any("dually" in c[0].lower() for c in filtered)
    
    # STAGE 1: CV2 Detection (if enabled)
    if config.enable_darth_cv2_dually and img_bytes_list and not has_dually_before:
        try:
            # Check Image 0
            is_dually_cv2, score = darth_vision.inspect_for_dually(img_bytes_list[0])
            if is_dually_cv2:
                utils.log_msg(f" [W-{worker_id}] 🌑 Darth (CV2) found Dually! (Score: {score})", worker_id)
                filtered.append(("Dually", 90.0))
                # Re-sort so Dually isn't #1
                filtered = data_processing.handle_dually_logic(filtered, worker_id)
                has_dually_before = True  # Update flag
        except Exception as e:
            utils.log_msg(f" [W-{worker_id}] Darth CV2 error: {e}", worker_id)
    
    # STAGE 2: LLM Verification for high-probability dually types (if still no dually found)
    # This catches duallys that CV2 missed
    if img_bytes_list and not has_dually_before and len(filtered) >= 1:
        top_category = filtered[0][0].lower()
        # These vehicle types are very commonly duallys - verify with LLM if not yet detected
        high_dually_probability_types = [
            "box truck - straight truck", 
            "cutaway-cube van", 
            "stepvan",
            "cabover truck - coe",
            "cab-chassis",
            "pickup truck",  # Heavy-duty pickups (F-350, RAM 3500, etc.) are commonly Duallys
            # "utility truck - service truck"  # Service trucks on heavy-duty chassis are often Duallys
        ]
        
        if any(hd_type in top_category for hd_type in high_dually_probability_types):
            try:
                utils.log_msg(f"[W-{worker_id}] 🔍 LLM Dually Verification for '{filtered[0][0]}' (high-probability type)", worker_id)
                is_dually_llm, confidence, t_in, t_out = classification.verify_dually_with_llm(
                    img_bytes_list[0], yoda_instance, key_queue, worker_id, ad_id, status_queue
                )
                total_in_tokens += t_in
                total_out_tokens += t_out
                
                if is_dually_llm:
                    utils.log_msg(f" [W-{worker_id}] ✅ LLM Confirmed Dually! (Confidence: {confidence})", worker_id)
                    filtered.append(("Dually", confidence))
                    # Re-sort so Dually isn't #1
                    filtered = data_processing.handle_dually_logic(filtered, worker_id)
                else:
                    utils.log_msg(f" [W-{worker_id}] ❌ LLM: Not a Dually", worker_id)
            except Exception as e:
                utils.log_msg(f" [W-{worker_id}] LLM Dually verification error: {e}", worker_id)
    # =================================================================================

    # Determine status - pass has_images flag to properly handle no-image cases
    status = data_processing.determine_status(breadcrumb, filtered, annotated, has_images=has_valid_images)

    # --- CALCULATE COST ---
    cost_cents = utils.calculate_cost_cents(total_in_tokens, total_out_tokens, config.gemini_model)

    final_row = {
        "Ad ID": ad_id,
        "Breadcrumb_Top1": breadcrumb[0] if len(breadcrumb) > 0 else "",
        "Breadcrumb_Top2": breadcrumb[1] if len(breadcrumb) > 1 else "",
        "Breadcrumb_Top3": breadcrumb[2] if len(breadcrumb) > 2 else "",
        "Annotated_Top1": filtered[0][0] if len(filtered) >= 1 else "",
        "Annotated_Top2": filtered[1][0] if len(filtered) >= 2 else "",
        "Annotated_Top3": filtered[2][0] if len(filtered) >= 3 else "",
        "Annotated_Top1_Score": round(filtered[0][1], 1) if len(filtered) >= 1 else 0,
        "Annotated_Top2_Score": round(filtered[1][1], 1) if len(filtered) >= 2 else 0,
        "Annotated_Top3_Score": round(filtered[2][1], 1) if len(filtered) >= 3 else 0,
        "Image_Count": len(image_urls),
        "Image_URLs": ", ".join(image_urls) if image_urls else "",
        "Status": status,
        "Cost_Cents": cost_cents
    }

    results_queue.put(final_row)
    return final_row


def run_worker_process(worker_id, run_ts, job_queue: Queue, results_queue: Queue,
                       status_queue: Queue, key_queue: Queue,
                       high_accuracy: bool = False, use_vision_v2: bool = False, yoda_instance=None):
    from . import config_loader
    config_loader.load_config()   
    utils.initialize_logging(run_ts, worker_id)
    classification.initialize_all_trackers()

    category_data = data_processing.load_category_data(config.category_json)
    rules = data_processing.load_rules(config.rules_json)
    processed = 0

    try:
        status_queue.put({"worker_id": worker_id, "state": "WAITING", "ad_id": None, "progress": 0})

        while True:
            ad_row = None
            try:
                ad_row = job_queue.get(timeout=2)
                ad_id = str(ad_row.get("Ad ID", "")).strip()

                status_queue.put({
                    "worker_id": worker_id,
                    "state": "PROCESSING",
                    "ad_id": ad_id,
                    "progress": processed
                })

                row = _process_single_ad(
                    ad_row, category_data, rules,
                    high_accuracy=high_accuracy,
                    worker_id=worker_id,
                    key_queue=key_queue,
                    status_queue=status_queue,
                    results_queue=results_queue,
                    use_vision_v2=use_vision_v2,
                    yoda_instance=yoda_instance
                )

                processed += 1
                status_queue.put({
                    "worker_id": worker_id,
                    "state": "WAITING",
                    "ad_id": ad_id,
                    "progress": processed
                })

            except queue.Empty:
                status_queue.put({"worker_id": worker_id, "state": "FINISHED", "progress": processed})
                break
            except classification.AllKeysExhaustedError:
                if ad_row and ad_row.get("Ad ID"):
                    job_queue.put(ad_row)
                    results_queue.put({"Ad ID": ad_row.get("Ad ID"), "Status": "Re-queued (key exhausted)"})
                status_queue.put({"worker_id": worker_id, "state": "ERROR", "progress": processed})
                break
            except Exception as e:
                utils.log_msg(f"[W-{worker_id}] Unexpected error: {e}", worker_id)
                if ad_row and ad_row.get("Ad ID"):
                    results_queue.put({"Ad ID": str(ad_row.get("Ad ID")), "Status": f"System Error: {str(e)[:50]}"})
                processed += 1
                status_queue.put({"worker_id": worker_id, "state": "ERROR", "progress": processed})

    finally:
        utils.generate_session_reports(
            classification.get_key_usage_stats(),
            classification.get_token_usage_stats(),
            run_ts,
            worker_id
        )


def run_single_process(input_file, fast_mode=False):
    """
    Runs the AI processing in a single thread (for debugging or slow mode).
    """
    run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    utils.initialize_logging(run_ts, 0)
    classification.initialize_all_trackers()
    
    print(f"Starting Single Process Mode on: {os.path.basename(input_file)}")
    
    df = pd.read_excel(input_file, dtype={"Ad ID": str})
    df["Ad ID"] = df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    category_data = data_processing.load_category_data(config.category_json)
    rules = data_processing.load_rules(config.rules_json)
    
    m = Manager()
    key_queue = m.Queue()
    for k in config.gemini_api_keys_info: key_queue.put(k)
    status_queue = m.Queue()
    results_queue = m.Queue()
    
    yoda = Yoda(config.gemini_api_keys_info, config.rate_limit_rpm, m)
    
    results = []
    
    for i, row in df.iterrows():
        ad_id = str(row.get("Ad ID", "")).strip()
        print(f"[{i+1}/{len(df)}] Processing {ad_id}...")
        
        try:
            _process_single_ad(
                row.to_dict(), category_data, rules,
                high_accuracy=not fast_mode,
                worker_id=0,
                key_queue=key_queue,
                status_queue=status_queue,
                results_queue=results_queue,
                use_vision_v2=False,
                yoda_instance=yoda
            )
            
            while not results_queue.empty():
                res = results_queue.get()
                results.append(res)
                print(f"   -> Result: {res.get('Annotated_Top1')} ({res.get('Status')}) | Cost: {res.get('Cost_Cents')}¢")
        
        except Exception as e:
            print(f"Error on {ad_id}: {e}")
            
    save_checkpoint(run_ts, results, df)
    print("\nSingle Process Run Completed.")