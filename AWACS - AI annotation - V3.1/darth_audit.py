import os
import sys
import glob
import pandas as pd
import requests
import time
from tqdm import tqdm

# Ensure we can import ai_tool
current_dir = os.path.dirname(os.path.abspath(__file__))
if os.path.exists(os.path.join(current_dir, 'ai_tool')):
    sys.path.append(current_dir)
elif os.path.exists(os.path.join(current_dir, 'modules', 'ai_tool')):
    sys.path.append(os.path.join(current_dir, 'modules'))

try:
    from ai_tool import darth_vision
    from ai_tool.config_loader import config, load_config
except ImportError:
    print("❌ Error: Could not import 'ai_tool'. Run this from the project root.")
    sys.exit(1)

def run_darth_audit():
    load_config()
    os.system('cls' if os.name == 'nt' else 'clear')
    print("============================================")
    print("       DARTH VADER (CV2) AUDIT TOOL         ")
    print("============================================")

    # 1. Find Latest Output File
    ai_dir = config.output_dir
    files = glob.glob(os.path.join(ai_dir, "output_annotated_*.xlsx"))
    
    if not files:
        print("❌ No AI output files found.")
        return

    latest_file = max(files, key=os.path.getmtime)
    print(f"📂 Analyzing: {os.path.basename(latest_file)}")
    
    df = pd.read_excel(latest_file, dtype={"Ad ID": str})
    
    # 2. Filter: Only check ads where AI did NOT say Dually
    # We want to see what we MISSED.
    # Convert all columns to string to search for "Dually"
    df["Combined_Cats"] = df["Annotated_Top1"].astype(str) + " " + df["Annotated_Top2"].astype(str)
    
    # Filter rows that do NOT contain "Dually" and have Images
    candidates = df[
        (~df["Combined_Cats"].str.contains("Dually", case=False)) & 
        (df["Image_URLs"].notna()) & 
        (df["Image_URLs"] != "")
    ].copy()

    print(f"🔍 Checking {len(candidates)} non-dually ads for missed Duallys...")
    print("   (This downloads 1 image per ad. Press Ctrl+C to stop early.)\n")

    results = []

    try:
        # Loop with progress bar
        for idx, row in tqdm(candidates.iterrows(), total=len(candidates)):
            ad_id = row["Ad ID"]
            img_urls = str(row["Image_URLs"]).split(",")
            
            if not img_urls: continue
            
            # Download first image (First image is usually the best angle)
            try:
                r = requests.get(img_urls[0], timeout=5)
                if r.status_code == 200:
                    img_bytes = r.content
                    
                    # --- ASK DARTH ---
                    is_dually, score = darth_vision.inspect_for_dually(img_bytes)
                    
                    if is_dually:
                        results.append({
                            "Ad ID": ad_id,
                            "Current AI": row["Annotated_Top1"],
                            "Darth Says": "Dually Detected",
                            "Darth Score": score,
                            "Image URL": img_urls[0]
                        })
            except:
                pass
                
    except KeyboardInterrupt:
        print("\n🛑 Stopped early.")

    # 3. Save Report
    if results:
        print(f"\n🚨 Darth found {len(results)} potential missed Duallys!")
        
        report_df = pd.DataFrame(results)
        # Sort by score (Highest confidence first)
        report_df = report_df.sort_values(by="Darth Score", ascending=False)
        
        timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
        save_path = f"Darth_Audit_{timestamp}.xlsx"
        
        report_df.to_excel(save_path, index=False)
        print(f"✅ Report saved to: {save_path}")
        print("👉 Open this file and check the Image URLs. If the high scores are real Duallys, we enable the code!")
    else:
        print("\n✅ Darth found nothing new. Your current AI is doing great (or threshold is too high).")

if __name__ == "__main__":
    run_darth_audit()