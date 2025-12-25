import os
import sys
import glob
import pandas as pd
from datetime import datetime

# --- ROBUST PATH SETUP ---
current_script_path = os.path.dirname(os.path.abspath(__file__))

if os.path.exists(os.path.join(current_script_path, 'ai_tool')):
    if current_script_path not in sys.path:
        sys.path.append(current_script_path)
elif os.path.exists(os.path.join(current_script_path, 'modules', 'ai_tool')):
    modules_path = os.path.join(current_script_path, 'modules')
    if modules_path not in sys.path:
        sys.path.append(modules_path)
else:
    parent_dir = os.path.dirname(current_script_path)
    if os.path.exists(os.path.join(parent_dir, 'ai_tool')):
        if parent_dir not in sys.path:
            sys.path.append(parent_dir)

# --- IMPORTS ---
try:
    from ai_tool.config_loader import config, load_config
    from ai_tool.data_processing import load_rules, normalize_text
except ImportError as e:
    print(f"\n❌ CRITICAL IMPORT ERROR: {e}")
    print(f"   Current Path: {current_script_path}")
    input("Press Enter to exit...")
    sys.exit(1)

def get_normalized_set(row, cols, norm_map):
    """Helper to extract columns, normalize them, and return a set."""
    res_set = set()
    for c in cols:
        val = row.get(c)
        if pd.notna(val) and str(val).strip() != "":
            norm_val = normalize_text(str(val), norm_map)
            if norm_val:
                res_set.add(norm_val.lower())
    return res_set

def run_audit():
    load_config()
    os.system('cls' if os.name == 'nt' else 'clear')
    print("============================================")
    print("          AI ACCURACY AUDIT TOOL            ")
    print("============================================")

    # 1. SETUP PATHS
    ai_dir = os.path.abspath(config.output_dir) # Force absolute path
    manual_dir = os.path.join(config.project_root, "Manual Feedback")
    audit_dir = os.path.join(config.project_root, "Audit Reports")
    os.makedirs(audit_dir, exist_ok=True)

    # 2. LOAD RULES
    try:
        rules = load_rules(config.rules_json)
        norm_map = rules['normalize_map']
    except:
        print("❌ Could not load Rules.json.")
        return

    # 3. LOAD AI DATA
    print("\n1️⃣  Loading AI Output Data...")
    print(f"   📂 Looking in: {ai_dir}")
    
    # Grab ALL Excel files
    all_files = glob.glob(os.path.join(ai_dir, "*.xlsx"))
    
    # Filter out temp files and Archive folder
    ai_files = [f for f in all_files if not os.path.basename(f).startswith("~$")]
    
    if not ai_files:
        print(f"❌ No Excel files found in: {ai_dir}")
        return

    ai_dfs = []
    print(f"   Scanning {len(ai_files)} files...")
    
    for f in ai_files:
        try:
            # Read file
            temp_df = pd.read_excel(f, dtype={"Ad ID": str})
            
            # Check for Ad ID column variants
            cols_lower = {c.lower(): c for c in temp_df.columns}
            if 'ad id' in cols_lower:
                col_name = cols_lower['ad id']
            elif 'ad_id' in cols_lower:
                col_name = cols_lower['ad_id']
            else:
                continue # Skip files without Ad ID (logs, etc.)

            # Standardize
            temp_df.rename(columns={col_name: "Ad ID"}, inplace=True)
            temp_df["Ad ID"] = temp_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            ai_dfs.append(temp_df)
        except: pass
    
    if not ai_dfs:
        print("❌ Failed to read valid data from AI files.")
        return

    master_ai = pd.concat(ai_dfs, ignore_index=True)
    # Deduplicate: Keep the last occurrence (newest)
    master_ai = master_ai.drop_duplicates(subset=["Ad ID"], keep='last')
    print(f"   -> Loaded {len(master_ai)} unique AI annotations.")

    # 4. LOAD MANUAL FEEDBACK
    print("\n2️⃣  Loading Manual Feedback Data...")
    manual_files = glob.glob(os.path.join(manual_dir, "*.xlsx"))
    # Filter temp files
    manual_files = [f for f in manual_files if not os.path.basename(f).startswith("~$")]

    if not manual_files:
        print(f"❌ No files found in '{manual_dir}'.")
        return
    
    latest_manual = max(manual_files, key=os.path.getmtime)
    print(f"   -> Using: {os.path.basename(latest_manual)}")
    
    try:
        human_df = pd.read_excel(latest_manual, dtype=str)
        
        human_cols_lower = {c.lower(): c for c in human_df.columns}
        col_map = {'ad id': 'Ad ID', 'ad_id': 'Ad ID'}
        for k, v in col_map.items():
            if k in human_cols_lower:
                human_df.rename(columns={human_cols_lower[k]: v}, inplace=True)
                break
            
        if "Ad ID" not in human_df.columns:
            print("❌ Error: Manual Feedback file must have an 'Ad ID' column.")
            return

        human_df["Ad ID"] = human_df["Ad ID"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    except Exception as e:
        print(f"❌ Error reading manual file: {e}")
        return

    # 5. MERGE DATA
    print("\n3️⃣  Comparing Data...")
    merged = pd.merge(master_ai, human_df, on="Ad ID", how="inner", suffixes=('', '_manual'))
    
    if merged.empty:
        print("❌ No matching Ad IDs found between AI Output and Manual Feedback.")
        return

    # 6. COMPARISON LOGIC
    audit_results = []
    
    ai_cols = ["Annotated_Top1", "Annotated_Top2", "Annotated_Top3"]
    human_keys = ["Primary Category", "Add'l Category 1", "Add'l Category 2"]
    
    found_human_cols = [c for c in human_keys if c in human_df.columns]

    for idx, row in merged.iterrows():
        ai_set = get_normalized_set(row, ai_cols, norm_map)
        ai_status = str(row.get("Status", "")).lower()
        
        human_set = get_normalized_set(row, found_human_cols, norm_map)
        
        status = "Rejected" # Default

        if ai_set == human_set:
            status = "Accepted"
        
        elif len(human_set) == 0:
            if "image not clear" in ai_set:
                status = "Accepted"
            elif "inactive ad" in ai_status or "inactive" in ai_status:
                status = "Accepted"
            elif "inactive ad" in ai_set:
                status = "Accepted"

        audit_results.append({
            "Ad ID": row["Ad ID"],
            "Feedback Status": status,
            "AI Categories": ", ".join(sorted(ai_set)),
            "Manual Categories": ", ".join(sorted(human_set))
        })

    audit_df = pd.DataFrame(audit_results)
    final_output = pd.merge(merged, audit_df[["Ad ID", "Feedback Status"]], on="Ad ID", how="left")

    # 7. GENERATE SUMMARY (UPDATED)
    total = len(final_output)
    
    # Identify Inactive Rows
    is_inactive = final_output['Status'].astype(str).str.contains('inactive', case=False, na=False)
    inactive_count = is_inactive.sum()
    
    active_total = total - inactive_count
    
    accepted_mask = (final_output["Feedback Status"] == "Accepted")
    rejected_mask = (final_output["Feedback Status"] == "Rejected")
    
    total_accepted = len(final_output[accepted_mask])
    total_rejected = len(final_output[rejected_mask])
    
    active_accepted = len(final_output[accepted_mask & (~is_inactive)])

    global_acc_pct = (total_accepted / total) * 100 if total > 0 else 0
    active_acc_pct = (active_accepted / active_total) * 100 if active_total > 0 else 0
    
    summary_data = [
        {"Metric": "Total Ads Audited", "Value": total},
        {"Metric": "Total Inactive Ads", "Value": inactive_count},
        {"Metric": "Total Active Ads", "Value": active_total},
        {"Metric": "---", "Value": "---"},
        {"Metric": "Global Accuracy (Including Inactive)", "Value": f"{global_acc_pct:.2f}%"},
        {"Metric": "⚠️ Active Accuracy (Excluding Inactive)", "Value": f"{active_acc_pct:.2f}%"},
        {"Metric": "---", "Value": "---"},
        {"Metric": "Total Accepted", "Value": total_accepted},
        {"Metric": "Total Rejected", "Value": total_rejected}
    ]
    summary_df = pd.DataFrame(summary_data)

    # Hall of Shame
    failures = audit_df[audit_df["Feedback Status"] == "Rejected"].copy()
    if not failures.empty:
        failures["Mismatch Pattern"] = "AI: [" + failures["AI Categories"] + "] vs Manual: [" + failures["Manual Categories"] + "]"
        hall_of_shame = failures["Mismatch Pattern"].value_counts().reset_index()
        hall_of_shame.columns = ["Mismatch Scenario", "Count"]
    else:
        hall_of_shame = pd.DataFrame([{"Message": "No Rejections!"}])

    # 8. SAVE REPORT
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_path = os.path.join(audit_dir, f"Audit_Report_{timestamp}.xlsx")
    
    try:
        with pd.ExcelWriter(report_path) as writer:
            final_output.to_excel(writer, sheet_name="Detailed Audit", index=False)
            summary_df.to_excel(writer, sheet_name="Summary", index=False, startrow=0, startcol=0)
            hall_of_shame.to_excel(writer, sheet_name="Summary", index=False, startrow=len(summary_df)+3, startcol=0)
            
        print(f"\n✅ Audit Complete!")
        print(f"   Global Accuracy: {global_acc_pct:.2f}%")
        print(f"   Active Accuracy: {active_acc_pct:.2f}%")
        print(f"   Report Saved: {os.path.basename(report_path)}")
        
    except Exception as e:
        print(f"❌ Error saving report: {e}")

if __name__ == "__main__":
    run_audit()
    input("\nPress Enter to exit.")