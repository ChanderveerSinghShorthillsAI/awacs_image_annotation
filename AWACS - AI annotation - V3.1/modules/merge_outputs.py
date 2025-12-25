import os
import glob
import pandas as pd
from datetime import datetime

from ai_tool.config_loader import config

def merge_excel_files():
    """
    Finds AI output files, merges a user-selected number of them, and saves a
    single, de-duplicated file sorted to match the original Scrapper.xlsx input order.
    """
    source_dir = config.output_dir
    merged_dir = os.path.join(config.project_root, "Merged AI Outputs")
    ad_id_column = "Ad ID"

    os.makedirs(merged_dir, exist_ok=True)
    
    all_files = glob.glob(os.path.join(source_dir, "output_annotated_*.xlsx"))
    if not all_files:
        print(f"❌ No output files found in '{os.path.basename(source_dir)}'.")
        input("\nPress Enter to return to the main menu.")
        return

    all_files.sort(key=os.path.getmtime, reverse=True)
    
    print("Found the following output files (newest first):")
    for i, file_path in enumerate(all_files, 1):
        print(f"  {i}. {os.path.basename(file_path)}")

    num_to_merge = 0
    while True:
        try:
            num_str = input(f"\nHow many of the latest files do you want to merge? (1-{len(all_files)}): ")
            num_to_merge = int(num_str)
            if 1 <= num_to_merge <= len(all_files): break
            else: print(f"❗️Error: Please enter a number between 1 and {len(all_files)}.")
        except ValueError: print("❗️Error: Invalid input. Please enter a number.")
            
    files_to_merge = all_files[:num_to_merge]
    
    print("\nReading files...")
        
    df_list = []
    for file in files_to_merge:
        try:
            # Ensure Ad IDs are read as strings to prevent data type issues
            df = pd.read_excel(file, dtype={ad_id_column: str})
            df[ad_id_column] = df[ad_id_column].str.replace(r'\.0$', '', regex=True).str.strip()
            df_list.append(df)
        except Exception as e:
            print(f"⚠️ Warning: Could not read file '{os.path.basename(file)}'. Skipping. Error: {e}")
            
    if not df_list:
        print("❌ No valid files could be read. Aborting.")
        input("\nPress Enter to return to the main menu.")
        return

    # --- SORTING AND DEDUPLICATION ---

    # Step 1: Get the correct, de-duplicated data (prioritizing newest files).
    merged_for_dedupe = pd.concat(df_list, ignore_index=True)
    correct_data_df = merged_for_dedupe.drop_duplicates(subset=[ad_id_column], keep='first')

    # Step 2: Get the MASTER sort order from the original Scrapper.xlsx file.
    try:
        scraper_input_path = os.path.join(config.project_root, "Scrapper.xlsx")
        scraper_df = pd.read_excel(scraper_input_path, dtype={ad_id_column: str})
        # Create a clean DataFrame that only contains the master sort order.
        master_order_df = scraper_df[[ad_id_column]].copy()
        master_order_df[ad_id_column] = master_order_df[ad_id_column].str.replace(r'\.0$', '', regex=True).str.strip()
        master_order_df.dropna(inplace=True)
    except Exception as e:
        print(f"❌ CRITICAL ERROR: Could not read the master order from 'Scrapper.xlsx'. Error: {e}")
        input("\nPress Enter to exit.")
        return
        
    # Step 3: Use a LEFT MERGE to sort the data.
    final_df = pd.merge(master_order_df, correct_data_df, on=ad_id_column, how='left')

    print(f"\nTotal unique ads after merging and sorting: {len(final_df)}")
    
    run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_filename = f"Merged_{num_to_merge}_files_{run_ts}.xlsx"
    output_path = os.path.join(merged_dir, output_filename)
    
    try:
        # --- UPDATED COLUMN LIST INCLUDING 'Cost_Cents' ---
        final_columns = [
            "Ad ID", "Breadcrumb_Top1", "Breadcrumb_Top2", "Breadcrumb_Top3",
            "Annotated_Top1", "Annotated_Top2", "Annotated_Top3",
            "Annotated_Top1_Score", "Annotated_Top2_Score", "Annotated_Top3_Score",
            "Image_Count", "Image_URLs", "Status", "Cost_Cents"
        ]
        
        # Add missing columns with empty string to avoid KeyError
        for col in final_columns:
            if col not in final_df.columns:
                final_df[col] = ""

        # Reindex to ensure consistent order
        final_df = final_df.reindex(columns=final_columns)
        
        final_df.to_excel(output_path, index=False)
        print(f"\n✅ Merge complete! File saved to: {output_path}")
    except Exception as e:
        print(f"\n❌ An error occurred while saving the file: {e}")

    input("\nPress Enter to return to the main menu.")

if __name__ == "__main__":
    try:
        from ai_tool.config_loader import load_config
        load_config()
        merge_excel_files()
    except ImportError:
        print("This script is intended to be run from main.py.")