import os
import glob
import pandas as pd
from datetime import datetime
import sys

# Add the 'modules' directory to the Python path to import our tools
try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'modules'))
    from ai_tool.config_loader import config, load_config
    from ai_tool.data_processing import normalize_text, load_rules
    from ai_tool.awacs_logger import setup_logger
except ImportError:
    print("❌ Critical Error: Could not find the 'modules' folder or its contents.")
    input("\nPress Enter to exit.")
    sys.exit(1)

logger = setup_logger("awacs.update_status")

def select_file(directory, pattern, prompt_message):
    """Generic function to display a menu of files and get a user's choice."""
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        logger.warning("No files matching '%s' found in the '%s' folder.", pattern, os.path.basename(directory))
        return None

    files.sort(key=os.path.getmtime, reverse=True)
    
    logger.info(prompt_message)
    for i, file_path in enumerate(files, 1):
        logger.info("  %d. %s", i, os.path.basename(file_path))
    
    while True:
        try:
            choice_str = input(f"\nEnter your choice (1-{len(files)}), or 'q' to quit: ")
            if choice_str.lower() == 'q': return None
            choice = int(choice_str)
            if 1 <= choice <= len(files):
                return files[choice - 1]
            else:
                logger.warning("Invalid choice. Please enter a number between 1 and %d.", len(files))
        except ValueError:
            logger.warning("Invalid input. Please enter a number or 'q'.")

def run_status_updater():
    """
    Updates the 'Status' column of an AI Output file by comparing it against
    the breadcrumbs from a newer Scraper file.
    """
    os.system('cls' if os.name == 'nt' else 'clear')
    logger.info("======================================================")
    logger.info("        AI OUTPUT STATUS UPDATER UTILITY              ")
    logger.info("======================================================")

    # 1. Select the AI Output file to be updated
    ai_file_to_update = select_file(
        config.output_dir, 
        "output_annotated_*.xlsx",
        "\nPlease select the AI Output file you want to UPDATE:"
    )
    if not ai_file_to_update: logger.info("Operation cancelled."); return

    # 2. Select the Scraper file with the new breadcrumb data
    scraper_file_source = select_file(
        config.scrapper_output_dir,
        "Scrapper_*.xlsx",
        "\nNow, select the NEW Scraper file to use for comparison:"
    )
    if not scraper_file_source: logger.info("Operation cancelled."); return

    logger.info("Updating '%s' using breadcrumbs from '%s'...", os.path.basename(ai_file_to_update), os.path.basename(scraper_file_source))

    try:
        ai_df = pd.read_excel(ai_file_to_update)
        scraper_df = pd.read_excel(scraper_file_source)
        rules = load_rules(config.rules_json)
        ad_id_column = "Ad ID"

        # --- Data Preparation ---
        # Ensure Ad ID columns are clean strings for a reliable merge
        ai_df[ad_id_column] = ai_df[ad_id_column].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        scraper_df[ad_id_column] = scraper_df[ad_id_column].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # --- Merge the two dataframes ---
        # We merge the new breadcrumbs from the scraper file onto our AI file.
        # We use a left merge to keep all rows from the AI file.
        merged_df = pd.merge(
            ai_df,
            scraper_df[[ad_id_column, 'Breadcrumb_Top1', 'Breadcrumb_Top2', 'Breadcrumb_Top3']],
            on=ad_id_column,
            how='left',
            suffixes=('_old', '_new') # Suffixes for the breadcrumb columns
        )

        updated_statuses = []
        rows_updated = 0
        
        for _, row in merged_df.iterrows():
            current_status = row.get('Status', '')
            # Don't change statuses that indicate a hard failure
            if "Error" in str(current_status) or current_status == "Inactive ad":
                updated_statuses.append(current_status)
                continue

            # Use the NEW breadcrumbs for comparison
            new_breadcrumbs = [
                row.get('Breadcrumb_Top1_new', ''),
                row.get('Breadcrumb_Top2_new', ''),
                row.get('Breadcrumb_Top3_new', '')
            ]
            
            # If there are no new breadcrumbs for this Ad ID, keep the old status
            if all(pd.isna(b) or b == '' for b in new_breadcrumbs):
                updated_statuses.append(current_status)
                continue

            annotated_list = [row.get('Annotated_Top1', ''), row.get('Annotated_Top2', ''), row.get('Annotated_Top3', '')]

            bc_norm = {normalize_text(b, rules['normalize_map']).lower() for b in new_breadcrumbs if pd.notna(b) and b}
            annotated_norm = {normalize_text(a, rules['normalize_map']).lower() for a in annotated_list if pd.notna(a) and a}
            
            new_status = "No change" if bc_norm == annotated_norm else "Require Update"
            
            if new_status != current_status:
                rows_updated += 1
            
            updated_statuses.append(new_status)
        
        # Update the original AI DataFrame with the new statuses and breadcrumbs
        ai_df['Status'] = updated_statuses
        ai_df['Breadcrumb_Top1'] = merged_df['Breadcrumb_Top1_new']
        ai_df['Breadcrumb_Top2'] = merged_df['Breadcrumb_Top2_new']
        ai_df['Breadcrumb_Top3'] = merged_df['Breadcrumb_Top3_new']

        # Save the result to a new file
        original_name, ext = os.path.splitext(os.path.basename(ai_file_to_update))
        new_name = f"{original_name}_status_re-evaluated{ext}"
        output_path = os.path.join(config.output_dir, new_name)
        
        ai_df.to_excel(output_path, index=False)
        
        logger.info("✅ Processing complete!")
        logger.info("   %d rows had their status changed.", rows_updated)
        logger.info("   A new file has been saved as '%s' in your '%s' folder.", new_name, os.path.basename(config.output_dir))

    except Exception as e:
        logger.error("An error occurred during processing: %s", e)

if __name__ == "__main__":
    try:
        load_config()
        run_status_updater()
    except Exception as e:
        logger.error("A critical error occurred: %s", e)
    
    input("\nPress Enter to exit.")