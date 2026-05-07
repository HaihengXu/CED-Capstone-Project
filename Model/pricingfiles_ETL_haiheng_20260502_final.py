import pandas as pd
from pathlib import Path

try:
    from functions_haiheng_20260502_final import load_pricing_files, build_wide_matrix
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).parent.parent / "Functions"))
    from functions_haiheng_20260502_final import load_pricing_files, build_wide_matrix


def run_pricing_etl(folder_path: str, output_dir: str) -> dict:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    combined_df, messages = load_pricing_files(folder_path)
    wide_df  = build_wide_matrix(combined_df) if not combined_df.empty else pd.DataFrame()

    log_df = pd.DataFrame([
        {"status": ("success" if m.startswith("✅") else
                    "skipped" if m.startswith("⚠️") else "error"),
         "message": m}
        for m in messages
    ])

    long_path = str(out / "long_format.csv")
    wide_path = str(out / "wide_matrix.csv")
    log_path  = str(out / "load_log.csv")

    combined_df.to_csv(long_path, index=False)
    wide_df.to_csv(wide_path, index=False)
    log_df.to_csv(log_path, index=False)

    return {
        "long": long_path, "wide": wide_path, "log": log_path,
        "messages": messages,
        "n_files":  len(messages),
        "n_loaded": sum(1 for m in messages if m.startswith("✅")),
        "n_rows":   len(combined_df),
    }


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python functions_haiheng_20260502_final.py <input_folder> <output_folder>")
        sys.exit(1)
    r = run_pricing_etl(sys.argv[1], sys.argv[2])
    print(f"\nFiles found: {r['n_files']}  |  Loaded: {r['n_loaded']}  |  Rows: {r['n_rows']:,}")
    for msg in r["messages"]:
        print(f"  {msg}")
