import sys
import os

# This helper script is called by the main quota checker.
# It tests a single key and prints a single-word result.

# --- DEFINITIVE FIX: Use a simpler, more robust silencing method ---
# Keep the original stdout/stderr streams safe.
original_stdout = sys.stdout
original_stderr = sys.stderr
# Open the "null" device once and keep it open.
devnull = open(os.devnull, 'w')

try:
    # Redirect both stdout and stderr to the null device.
    sys.stdout = devnull
    sys.stderr = devnull
    
    # Import the noisy library while everything is silenced.
    import google.generativeai as genai

finally:
    # Crucially, restore the original stdout and stderr streams.
    sys.stdout = original_stdout
    sys.stderr = original_stderr
# --- END FIX ---

def test_single_key(api_key: str, model_name: str):
    """Tests one API key and prints a single-word status."""
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_name)
        
        # --- DEFINITIVE FIX: Silence only the API call ---
        try:
            sys.stdout = devnull
            sys.stderr = devnull
            model.generate_content("test", request_options={'timeout': 10})
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr
        # --- END FIX ---
                
        # Print the final result to the now-restored, correct stdout
        print("Active")

    except Exception as e:
        error_message = str(e).lower()
        if "quota" in error_message or "resource has been exhausted" in error_message:
            print("Exhausted")
        elif "api key not valid" in error_message:
            print("Invalid")
        else:
            # Print the actual error to stderr so the main script can capture it
            print(f"Error in _key_tester: {e}", file=sys.stderr)
            print("Error")
    finally:
        # Final cleanup: close the null device when the script is done
        devnull.close()

if __name__ == "__main__":
    if len(sys.argv) == 3:
        test_single_key(api_key=sys.argv[1], model_name=sys.argv[2])