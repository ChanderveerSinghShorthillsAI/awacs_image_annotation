import sys

from cdc_pipeline.consumer import run
from cdc_pipeline.config import CDC_CONSUMER_TIMEOUT_MINUTES

auto = "--auto" in sys.argv
debug = "--debug" in sys.argv
fresh = "--fresh" in sys.argv

# In auto mode, use env var timeout as default; --timeout N overrides it
timeout_minutes = None
if auto:
    timeout_minutes = CDC_CONSUMER_TIMEOUT_MINUTES
    if "--timeout" in sys.argv:
        try:
            idx = sys.argv.index("--timeout")
            timeout_minutes = int(sys.argv[idx + 1])
        except (IndexError, ValueError):
            print("Error: --timeout requires an integer argument (minutes)")
            sys.exit(1)

matched = run(debug=debug, fresh=fresh, timeout_minutes=timeout_minutes)

if auto:
    if matched and matched > 0:
        print(f"\n{'=' * 60}")
        print(f"Auto mode: {matched} ads collected. Starting annotation pipeline...")
        print(f"{'=' * 60}\n")
        from cdc_pipeline.run_annotation import main as run_annotation
        run_annotation()
    else:
        print(f"\n{'=' * 60}")
        print("Auto mode: No ads collected. Skipping annotation.")
        print(f"{'=' * 60}")
