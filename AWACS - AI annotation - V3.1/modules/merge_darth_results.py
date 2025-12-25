import os
import sys
import glob
import pandas as pd
from datetime import datetime

# --- PATH SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if current_dir not in sys.path: sys.path.append(current_dir)
if parent_dir not in sys.path: sys.path.append(parent_dir)

try:
    from ai_tool.config_loader import config, load_config
except ImportError:
    # Fallback if config can't be loaded, define minimal paths
    class Config: pass
    config = Config()
    config.output_dir = os.path.join(parent_dir, "AI output")
    config.project_root = parent_dir

def select_file(directory, pattern, prompt_name):
    files = glob.glob(os.path.join(directory, pattern))
    # Filter out temp files
    files = [f for f in files if not os.path.basename(f).startswith("~$")]
    
    if not files:
        print(f"❌ No {prompt_name} files found in: {directory}")
        return None
    
    # Sort by newest
    files.sort(key=os.path.getmtime, reverse=True)
    
    print(f"\nSelect {prompt_name}:")
    for i, f in enumerate(files[:5]): # Show top 5
        print(f"  {i+1}. {os.path.basename(f)}")
        
    choice = input(f"Enter choice (1-{len(files[:5])}) or 'q' to quit: ")
    if choice.lower() == 'q': return None
    
    try:
        idx = int(choice) - 1
        if 0 <= idx < len(files):
            return files[idx]
    except:
        pass
    print("Invalid choice.")
    return None

def apply_darth_merge():
    # Load Config
    try: load_config() 
    except: pass
    
    os.system('cls' if os.name == 'nt' else 'clear')
    print("============================================")
    print("      MERGE DARTH RESULTS INTO OUTPUT       ")
    print("============================================")

    # 1. Select Main AI Output File
    ai_file = select_file(config.output_dir, "output_annotated_*.xlsx", "Main AI Output")
    if not ai_file: return

    # 2. Select Darth Audit File
    # Assuming Audit files are in Root or AI Output. Checking Root first.
    darth_pattern = "Darth_Audit_*.xlsx"
    darth_file = select_file(config.project_root, darth_pattern, "Darth Audit Result")
    
    # Check AI output folder if not in root
    if not darth_file:
         darth_file = select_file(config.output_dir, darth_pattern, "Darth Audit Result")
         
    if not darth_file: return

    print(f"\n🔄 Merging '{os.path.basename(darth_file)}' into '{os.path.basename(ai_file)}'...")

    try:
        # Load Dataframes
        main_df = pd.read_excel(ai_file, dtype={"Ad ID": str})
        darth_df = pd.read_excel(darth_file, dtype={"Ad ID": str})
        
        # Clean IDs
        main_df["Ad ID"] = main_df["Ad ID"].str.strip()
        darth_df["Ad ID"] = darth_df["Ad ID"].str.strip()
        
        # Identify IDs to update
        darth_ids = set(darth_df["Ad ID"])
        count = 0
        
        for idx, row in main_df.iterrows():
            if row["Ad ID"] in darth_ids:
                # Update Logic: Insert "Dually" into Top 2
                
                # Get current values
                top1 = row.get("Annotated_Top1", "")
                top2 = row.get("Annotated_Top2", "")
                # top3 will be overwritten by old top2 if needed
                
                # Logic: Keep Top1, Insert Dually, Push Top2 to Top3
                main_df.at[idx, "Annotated_Top2"] = "Dually"
                main_df.at[idx, "Annotated_Top2_Score"] = 90.0 # Assign high confidence
                
                if pd.notna(top2) and str(top2).strip() != "":
                    main_df.at[idx, "Annotated_Top3"] = top2
                    main_df.at[idx, "Annotated_Top3_Score"] = row.get("Annotated_Top2_Score", 0)
                
                # Mark status updated so user knows
                current_status = str(row.get("Status", ""))
                if "Darth" not in current_status:
                    main_df.at[idx, "Status"] = f"{current_status} (Dually Added)"
                
                count += 1
        
        # Save Copy
        base_name = os.path.splitext(os.path.basename(ai_file))[0]
        new_filename = f"{base_name}_with_Darth.xlsx"
        save_path = os.path.join(config.output_dir, new_filename)
        
        main_df.to_excel(save_path, index=False)
        
        print("\n✅ Success!")
        print(f"   Updated {count} ads with 'Dually'.")
        print(f"   Saved new file: {new_filename}")
        
    except Exception as e:
        print(f"\n❌ Error during merge: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    apply_darth_merge()
    input("\nPress Enter to exit.")