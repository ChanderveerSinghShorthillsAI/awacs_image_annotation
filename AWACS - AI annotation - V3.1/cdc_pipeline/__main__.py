import sys
from cdc_pipeline.consumer import run

run(debug="--debug" in sys.argv, fresh="--fresh" in sys.argv)
