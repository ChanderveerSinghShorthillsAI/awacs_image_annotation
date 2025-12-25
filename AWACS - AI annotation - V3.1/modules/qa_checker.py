import os
import sys
import glob
import time
import pandas as pd
from datetime import datetime

# --- PATH SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if current_dir not in sys.path: sys.path.append(current_dir)
if parent_dir not in sys.path: sys.path.append(parent_dir)

# --- IMPORTS ---
try:
    from ai_tool.config_loader import config, load_config
    from ai_tool.web_utils import setup_driver
    from ai_tool.data_processing import load_rules, normalize_text
    from ai_tool.utils import fmt_secs
    
    # LINKING TO SCRAPER MODULE DIRECTLY
    import scraper_module 
except ImportError as e:
    print(f"\n❌ CRITICAL IMPORT ERROR: {e}")
    input("Press Enter to exit...")
    sys.exit(1)

def run_qa_check():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("============================================")
    print("          QUALITY ASSURANCE (QA) CHECKER    ")
    print("============================================")

    # 1. Setup Folders
    base_qa_dir = os.path.abspath(os.path.join(config.project_root, "Ready for QA"))
    output_dir = os.path.join(base_qa_dir, "QA feedback")
    os.makedirs(base_qa_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    print(f"📂 Looking for files in: {base_qa_dir}")

    # 2. Find Input File
    files = glob.glob(os.path.join(base_qa_dir, "*.xlsx"))
    files = [f for f in files if "QA feedback" not in f and "~$" not in os.path.basename(f)]
    
    if not files:
        print(f"\n❌ No Input Excel files found in 'Ready for QA'.")
        return

    input_file = max(files, key=os.path.getmtime)
    print(f"📄 Found File: {os.path.basename(input_file)}")

    # 3. Load Resources
    try:
        df = pd.read_excel(input_file, dtype={"Ad ID": str})
        df["Ad ID"] = df["Ad ID"].astype(str).str.strip()
        rules = load_rules(config.rules_json)
        norm_map = rules['normalize_map']
        
        # Initialize Output Columns
        new_cols = ["live breadcrum 1", "live breadcrum 2", "live breadcrum 3", "QA Status"]
        for col in new_cols:
            if col not in df.columns: df[col] = ""
            
    except Exception as e:
        print(f"❌ Error loading Excel file: {e}")
        return

    total = len(df)
    print(f"📊 Validating {total} ads... (Press Ctrl+C to stop)")
    
    # 4. Output Path
    run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_filename = f"QA {run_ts}.xlsx"
    output_path = os.path.join(output_dir, output_filename)

    # 5. Initialize Driver
    driver = setup_driver(headless=True)
    t0 = time.time()

    try:
        for idx, row in df.iterrows():
            if pd.notna(row.get("QA Status")) and str(row.get("QA Status")).strip() != "":
                continue

            ad_id = row.get("Ad ID")
            if not ad_id or str(ad_id).lower() == "nan": continue
            
            # --- FILE DATA ---
            file_bcs = [row.get("Breadcrumb_Top1"), row.get("Breadcrumb_Top2"), row.get("Breadcrumb_Top3")]
            # Normalize file data for comparison (Set for order independence)
            file_set = {normalize_text(str(b), norm_map).lower() for b in file_bcs if pd.notna(b) and str(b).strip()}
            file_set.discard("inactive ad")
            
            # --- LIVE DATA (USING SCRAPER MODULE) ---
            # using the exact same function the scraper uses
            scrape_result = scraper_module.scrape_ad_data(driver, ad_id)
            
            status = "UNKNOWN"
            l1, l2, l3 = "", "", ""
            
            if scrape_result["status"] == "Inactive":
                status = "⚠️ QA INACTIVE"
                l1 = "Ad Inactive"
            else:
                live_raw = scrape_result["breadcrumbs"]
                
                # Assign Columns exactly like Scraper does
                l1 = live_raw[0] if len(live_raw) > 0 else ""
                l2 = live_raw[1] if len(live_raw) > 1 else ""
                l3 = live_raw[2] if len(live_raw) > 2 else ""

                # Normalize for Logic Check
                live_set = {normalize_text(str(b), norm_map).lower() for b in live_raw}

                # Comparison Logic (Subset)
                if not file_set:
                    status = "⚠️ NO FILE DATA"
                elif file_set.issubset(live_set):
                    status = "✅ QA PASS"
                else:
                    status = "❌ QA FAIL"

            # --- UPDATE DATAFRAME ---
            df.at[idx, "live breadcrum 1"] = l1
            df.at[idx, "live breadcrum 2"] = l2
            df.at[idx, "live breadcrum 3"] = l3
            df.at[idx, "QA Status"] = status

            # --- ETA ---
            processed = idx + 1
            elapsed = time.time() - t0
            avg_time = elapsed / (processed if processed > 0 else 1)
            remaining = avg_time * (total - processed)
            eta_str = fmt_secs(remaining)

            print(f"[{processed}/{total}] {ad_id}: {status} | ETA: {eta_str}")
            
            if processed % 50 == 0:
                try: df.to_excel(output_path, index=False)
                except: pass

    except KeyboardInterrupt:
        print("\n🛑 Stopping...")
    except Exception as e:
        print(f"\n❌ Unexpected Crash: {e}")
    finally:
        if 'driver' in locals(): driver.quit()

    try:
        df.to_excel(output_path, index=False)
        print(f"\n✅ QA Complete! Output saved to:\n   {output_path}")
    except Exception as e:
        print(f"❌ Error saving final file: {e}")

if __name__ == "__main__":
    try:
        load_config()
        run_qa_check()
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        input("Press Enter to exit...")