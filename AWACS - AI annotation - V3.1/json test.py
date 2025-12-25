import json
import os
import sys

def check_file(filename):
    print(f"\n🔍 Checking '{filename}'...")
    
    # Try to find file in current dir or parent dir
    if os.path.exists(filename):
        path = filename
    elif os.path.exists(os.path.join("..", filename)):
        path = os.path.join("..", filename)
    else:
        print(f"❌ ERROR: File not found in {os.getcwd()}")
        return

    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        print(f"✅ VALID JSON SYNTAX.")
        
        # Specific Stats for Rules.json
        if "Rules" in filename:
            norm = len(data.get('normalize_map', {}))
            excl = len(data.get('exclusion_rules', []))
            over = len(data.get('truck_overlaps', []))
            print(f"   -> Normalize Map Entries: {norm}")
            print(f"   -> Exclusion Rules: {excl}")
            print(f"   -> Overlap Rules: {over}")
            
            if excl == 0:
                print("   ⚠️ WARNING: No Exclusion Rules found! Check structure.")

        # Specific Stats for Categories.json
        elif "Categories" in filename:
            print(f"   -> Total Categories: {len(data)}")

    except json.JSONDecodeError as e:
        print(f"❌ CRITICAL SYNTAX ERROR!")
        print(f"   Line: {e.lineno}")
        print(f"   Column: {e.colno}")
        print(f"   Error: {e.msg}")
        print("   -> Tip: Look for a missing comma ',' or an extra comma at the end of a list.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_file("Rules.json")
    check_file("Categories.json")
    input("\nPress Enter to exit...")