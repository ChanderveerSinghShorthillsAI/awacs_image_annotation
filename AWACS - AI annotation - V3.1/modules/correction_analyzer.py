import os
import glob
import pandas as pd
from datetime import datetime

# --- Configuration ---
AI_OUTPUT_DIR = "AI output"
MANUAL_FEEDBACK_DIR = "Manual Feedback"  # <-- New folder for your corrected files
ANALYSIS_OUTPUT_DIR = "Analysis Reports" # <-- New folder for the Excel reports
AD_ID_COLUMN = "Ad ID"

AI_CATEGORY_COLUMNS = ["Annotated_Top1", "Annotated_Top2", "Annotated_Top3"]
HUMAN_CATEGORY_COLUMNS = ["Primary Category", "Add'l Category 1", "Add'l Category 2"]


def find_files(directory, prefix="*.xlsx"):
    """Finds all Excel files in a directory, sorted by newest first."""
    os.makedirs(directory, exist_ok=True) # Create dir if it doesn't exist
    search_pattern = os.path.join(directory, prefix)
    all_files = glob.glob(search_pattern)
    return sorted(all_files, key=os.path.getmtime, reverse=True) if all_files else []

def select_file(file_list, prompt_message):
    """Displays a menu of files and returns the user's choice."""
    if not file_list:
        return None
    
    print(prompt_message)
    for i, file_path in enumerate(file_list, 1):
        print(f"  {i}. {os.path.basename(file_path)}")
    
    while True:
        try:
            choice = int(input(f"Enter your choice (1-{len(file_list)}): "))
            if 1 <= choice <= len(file_list):
                return file_list[choice - 1]
            else:
                print(f"❗️Invalid choice. Please enter a number between 1 and {len(file_list)}.")
        except ValueError:
            print("❗️Invalid input. Please enter a number.")

def get_categories_from_row(row, columns):
    """Extracts, cleans, and sorts a set of categories from a DataFrame row."""
    categories = set()
    for col in columns:
        if col in row and pd.notna(row[col]):
            categories.add(str(row[col]).strip())
    # Return a sorted, comma-separated string for consistent grouping
    return ", ".join(sorted(list(categories))) if categories else "None"

def analyze_corrections():
    """Main function to run the comparison and generate an Excel report."""
    os.system('cls' if os.name == 'nt' else 'clear')
    print("============================================")
    print("        AI CORRECTION ANALYZER              ")
    print("============================================")
    
    os.makedirs(ANALYSIS_OUTPUT_DIR, exist_ok=True)

    # 1. Select the AI output file
    ai_files = find_files(AI_OUTPUT_DIR, "output_annotated_*.xlsx")
    if not ai_files:
        print(f"\n❌ No AI output files found in '{AI_OUTPUT_DIR}'. Please run the AI script first.")
        return
    ai_file_path = select_file(ai_files, "\nPlease select the AI Output file to analyze:")

    # 2. Select the manually corrected file from the new folder
    manual_files = find_files(MANUAL_FEEDBACK_DIR, "*.xlsx")
    if not manual_files:
        print(f"\n❌ No manual feedback files found in '{MANUAL_FEEDBACK_DIR}'. Please add your corrected file there.")
        return
    manual_file_path = select_file(manual_files, "\nSelect your Manually Corrected 'Final' file:")

    print("\n--- Loading and analyzing files... ---")

    try:
        ai_df = pd.read_excel(ai_file_path)
        human_df = pd.read_excel(manual_file_path)
    except Exception as e:
        print(f"❌ Error reading Excel files: {e}"); return

    merged_df = pd.merge(ai_df, human_df, on=AD_ID_COLUMN, suffixes=('_ai', '_human'), how='inner')

    if merged_df.empty:
        print("\n❌ No matching Ad IDs found between the two files. Cannot perform analysis."); return

    # 5. Analyze the differences
    mistake_rows = []
    perfect_matches = 0

    for _, row in merged_df.iterrows():
        ai_cats_str = get_categories_from_row(row, [f"{col}_ai" for col in AI_CATEGORY_COLUMNS])
        human_cats_str = get_categories_from_row(row, [f"{col}_human" for col in HUMAN_CATEGORY_COLUMNS])

        if ai_cats_str == human_cats_str:
            perfect_matches += 1
        else:
            mistake_rows.append({
                "AI Categories": ai_cats_str,
                "Human Corrected Categories": human_cats_str
            })
    
    # 6. Generate the report DataFrames
    total_compared = len(merged_df)
    errors = total_compared - perfect_matches
    accuracy = (perfect_matches / total_compared) * 100 if total_compared > 0 else 0

    # Create Summary DataFrame
    summary_data = {
        "Metric": ["Total Ads Compared", "✅ Perfect Matches", "❌ Found Errors", "🎯 AI Accuracy"],
        "Value": [total_compared, perfect_matches, errors, f"{accuracy:.2f}%"]
    }
    summary_df = pd.DataFrame(summary_data)

    # Create Mistakes DataFrame
    if mistake_rows:
        mistakes_df = pd.DataFrame(mistake_rows)
        # Group and count the occurrences of each unique mistake
        mistake_counts_df = mistakes_df.groupby(['AI Categories', 'Human Corrected Categories']).size().reset_index(name='Count')
        mistake_counts_df = mistake_counts_df.sort_values(by='Count', ascending=False).reset_index(drop=True)
    else:
        mistake_counts_df = pd.DataFrame([{"Message": "🎉 No mistakes found!"}])

    # 7. Save the report to an Excel file
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_filename = f"Analysis_Report_{ts}.xlsx"
    report_path = os.path.join(ANALYSIS_OUTPUT_DIR, report_filename)

    try:
        with pd.ExcelWriter(report_path) as writer:
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            mistake_counts_df.to_excel(writer, sheet_name='Mistake Details', index=False)
        
        print("\n============================================")
        print(f"           ANALYSIS COMPLETE              ")
        print("============================================")
        print(f"\n- Total Ads Compared: {total_compared}")
        print(f"- AI Accuracy: {accuracy:.2f}%")
        print(f"- Found {errors} ads with corrections.")
        print(f"\n✅ Full report saved to: {report_path}")

    except Exception as e:
        print(f"\n❌ An error occurred while saving the Excel report: {e}")

if __name__ == "__main__":
    analyze_corrections()
    input("\nPress Enter to exit.")