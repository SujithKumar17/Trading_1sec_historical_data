
import subprocess
import sys
from datetime import datetime
from pathlib import Path

YEAR = 2026
PROJECT_ROOT = Path(__file__).resolve().parents[1]

months = range(1, 12)

for month in months:
    month_str = f"{YEAR}-{month:02d}"

    print("\n" + "=" * 65)
    print(f"STARTING EXTRACTION: {month_str}")
    print("=" * 65, flush=True)

    command = [
        sys.executable,
        "-m",
        "extraction.extractor",
        "--mode",
        "monthly",
        "--month",
        month_str,
        "--include-futures"
    ]

    start_time = datetime.now()

    try:
        result = subprocess.run(command, check=False, cwd=PROJECT_ROOT)

        elapsed = datetime.now() - start_time

        if result.returncode == 0:
            print(f"\nSUCCESS: {month_str} | Duration: {elapsed}")
        else:
            print(f"\nFAILED: {month_str} | Exit code: {result.returncode}")
            print("Stopping further monthly extraction.")
            break

    except KeyboardInterrupt:
        print("\nExtraction interrupted by user.")
        break

print("\n12-MONTH EXTRACTION SEQUENCE FINISHED.")
