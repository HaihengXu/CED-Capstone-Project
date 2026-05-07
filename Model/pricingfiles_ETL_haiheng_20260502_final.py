import os
import pandas as pd
from pathlib import Path

# Allow running standalone or via Streamlit (sys.path set by ETL.py)
try:
    from functions_haiheng_v2 import load_pricing_files, build_wide_matrix
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent.parent / "Functions"))
    from functions_haiheng_20260502_final.py import load_pricing_files, build_wide_matrix


def run_pricing_etl(folder_path: str, output_dir: str) -> dict:
    """
    Full ETL pipeline.

    Parameters
    ----------
    folder_path : str  — directory containing raw pricing xlsx/csv files
    output_dir  : str  — directory where output CSVs will be written

    Returns
    -------
    dict with keys:
        'long'      → path to long_format.csv
        'wide'      → path to wide_matrix.csv
        'log'       → path to load_log.csv
        'messages'  → list of per-file status strings
        'n_files'   → int, number of files processed
        'n_loaded'  → int, number of files successfully loaded
        'n_rows'    → int, total rows in combined df
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # 1. Load all files
    combined_df, messages = load_pricing_files(folder_path)

    # 2. Build wide matrix (only if we have data)
    wide_df = build_wide_matrix(combined_df) if not combined_df.empty else pd.DataFrame()

    # 3. Build log df
    log_records = []
    for msg in messages:
        if msg.startswith("✅"):
            status = "success"
        elif msg.startswith("⚠️"):
            status = "skipped"
        else:
            status = "error"
        log_records.append({"status": status, "message": msg})
    log_df = pd.DataFrame(log_records)

    # 4. Write outputs
    long_path = str(out / "long_format.csv")
    wide_path = str(out / "wide_matrix.csv")
    log_path  = str(out / "load_log.csv")

    combined_df.to_csv(long_path, index=False)
    wide_df.to_csv(wide_path, index=False)
    log_df.to_csv(log_path, index=False)

    n_loaded = sum(1 for m in messages if m.startswith("✅"))

    return {
        "long":     long_path,
        "wide":     wide_path,
        "log":      log_path,
        "messages": messages,
        "n_files":  len(messages),
        "n_loaded": n_loaded,
        "n_rows":   len(combined_df),
    }


# ---------------------------------------------------------------------------
# CLI usage
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python pricingfiles_ETL_v2.py <input_folder> <output_folder>")
        sys.exit(1)

    results = run_pricing_etl(sys.argv[1], sys.argv[2])
    print(f"\n{'='*60}")
    print(f"Files found:    {results['n_files']}")
    print(f"Files loaded:   {results['n_loaded']}")
    print(f"Total rows:     {results['n_rows']:,}")
    print(f"\nOutputs:")
    print(f"  Long format → {results['long']}")
    print(f"  Wide matrix → {results['wide']}")
    print(f"  Load log    → {results['log']}")
    print(f"\nPer-file log:")
    for msg in results["messages"]:
        print(f"  {msg}")
