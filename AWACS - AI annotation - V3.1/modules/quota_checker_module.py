import os
import time
import sys
import glob
import pandas as pd
import contextlib
from datetime import datetime

# This must be set before the import to silence warnings
os.environ['GRPC_VERBOSITY'] = 'ERROR'
import google.generativeai as genai

from ai_tool.config_loader import config

def run_quota_check():
    """
    Tests each API key for real-time status and estimates remaining calls
    by aggregating historical usage from Key Reports.
    """
    os.system('cls' if os.name == 'nt' else 'clear')
    print("============================================")
    print("      GEMINI API KEY QUOTA ESTIMATOR        ")
    print("============================================")

    api_keys_info = config.gemini_api_keys_info
    gemini_model_name = config.gemini_model
    key_report_dir = config.key_report_dir
    daily_limit = config.api_key_daily_limit

    if not api_keys_info:
        print(f"\n❌ No API keys found in config.ini.")
        input("\nPress Enter to return to the main menu.")
        return

    print(f"\nFound {len(api_keys_info)} API keys to check against a daily limit of {daily_limit}.")
    print("Scanning historical usage from Key Reports...")

    usage_today = {f"Key {i+1}": 0 for i in range(len(api_keys_info))}
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    report_files = glob.glob(os.path.join(key_report_dir, "Session_Report_*.xlsx"))
    
    for f in report_files:
        if today_str in os.path.basename(f):
            try:
                # Check for both single-process and parallel report formats
                sheet_name = 'Key Usage' if 'worker' not in f else 'Key Usage Summary'
                df_report = pd.read_excel(f, sheet_name=sheet_name)
                
                # Determine the correct column name for the key identifier
                key_name_col = 'Original Key Name' if 'Original Key Name' in df_report.columns else 'Key Name'

                for _, row in df_report.iterrows():
                    key_name = row[key_name_col]
                    if key_name in usage_today:
                        usage_today[key_name] += row['Successful Calls']
            except Exception:
                pass

    print("Historical scan complete. Now performing live status checks...\n")
    
    report_data = []
    for key_info in api_keys_info:
        key = key_info['key']
        original_name = f"Key {key_info['original_index']}"
        status = ""
        
        print(f"Testing {original_name}...", end='\r', flush=True)
        
        try:
            genai.configure(api_key=key)
            model = genai.GenerativeModel(gemini_model_name)
            with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
                model.generate_content("test", request_options={'timeout': 10})
            status = "✅ Active"
            
        except Exception as e:
            error_message = str(e).lower()
            if "quota" in error_message or "resource has been exhausted" in error_message:
                status = "❌ Quota Exhausted"
            elif "api key not valid" in error_message:
                status = "❗️ Invalid Key"
            else:
                status = f"⚠️ Unknown Error"

        historical_usage = usage_today.get(original_name, 0)
        
        if status == "❌ Quota Exhausted":
            estimated_remaining = 0
        elif status == "✅ Active":
            estimated_remaining = max(0, daily_limit - historical_usage)
        else:
            estimated_remaining = "N/A"

        report_data.append({
            "Original Key Name": original_name,
            "Real-Time Status": status,
            "Historical Usage (Today)": historical_usage,
            "Estimated Calls Remaining": estimated_remaining
        })
        
        print(f"{original_name}: {status}, Estimated Remaining: {estimated_remaining}{' ' * 20}")
        time.sleep(0.2)

    print("\n--------------------------------------------")
    print("            Check complete.                 ")
    print("============================================")
    input("\nPress Enter to return to the main menu.")