import os
import sys
import shutil
import pandas as pd
import streamlit as st
from pathlib import Path

_root = Path(__file__).parent
sys.path.insert(0, str(_root / "Functions"))
sys.path.insert(0, str(_root / "Model"))

from functions_haiheng_20260502_final.py import (
    load_pricing_file, _location_from_filename, _detect_format
)
from pricingfiles_ETL_haiheng_20260502_final.py import run_pricing_etl

# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CED Pricing Analysis Tool",
    page_icon="📊",
    layout="wide",
)

st.title("📊 CED Pricing Analysis Tool")
st.caption(
    "Upload vendor pricing files to generate a consolidated, anonymised "
    "price comparison matrix."
)

# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("ℹ️ Supported File Formats")
    st.markdown("""
| Format | Description | Key price column |
|--------|-------------|-----------------|
| **A** | Bridgeport / multi-vendor | Distributor Price Each |
| **B** | Hubbell 3A cost sheets | PRICE |
| **C** | Midwest net price (multi-sheet) | NET_PRICE (SILVER) / NET |
| **D** | Stusser/Southwire SPA XLSX | Net Price |
| **E** | SpreadsheetML .xls | Price Break1 |
| **F** | TB/ABB SPA claimback | Final Net Price (1) |
| **G** | SPAPriceFile (Purchaser ID) | Net Price |

Files that don't match any format are skipped — they won't crash the run.
    """)

    st.divider()
    st.header("🔒 Privacy")
    st.markdown("""
All output files automatically strip:
- Company and customer names
- Account numbers
- Street addresses
    """)

# ─────────────────────────────────────────────────────────────────────────────
# File upload
# ─────────────────────────────────────────────────────────────────────────────
uploaded_files = st.file_uploader(
    "Choose pricing files",
    accept_multiple_files=True,
    type=["csv", "xlsx", "xls", "XLSX"],
)

if not uploaded_files:
    st.info("👆 Upload one or more pricing files to get started.")
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# Preview uploaded files
# ─────────────────────────────────────────────────────────────────────────────
st.subheader(f"📂 {len(uploaded_files)} file(s) uploaded")

FMT_LABELS = {
    "format_a": "✅ Format A (Bridgeport)",
    "format_b": "✅ Format B (Hubbell 3A)",
    "format_c": "✅ Format C (Midwest)",
    "format_d": "✅ Format D (Stusser SPA)",
    "format_e": "✅ Format E (SpreadsheetML)",
    "format_f": "✅ Format F (TB/ABB SPA)",
    "format_g": "✅ Format G (SPAPriceFile)",
    "unknown":  "⚠️ Unknown — will be skipped",
}

preview_data = []
tmp_dir = Path("/tmp/ced_preview")
tmp_dir.mkdir(exist_ok=True)

for uf in uploaded_files:
    tmp = tmp_dir / uf.name
    tmp.write_bytes(uf.getbuffer())
    loc = _location_from_filename(uf.name)
    fmt = _detect_format(str(tmp))
    preview_data.append({
        "File":     uf.name,
        "Location": loc,
        "Format":   FMT_LABELS.get(fmt, fmt),
    })

st.dataframe(pd.DataFrame(preview_data), use_container_width=True, hide_index=True)

unknown_count = sum(1 for r in preview_data if "Unknown" in r["Format"])
if unknown_count:
    st.warning(
        f"⚠️ {unknown_count} file(s) have an unrecognised format and will be skipped. "
        "Check that they contain a 'Price List' sheet or a recognisable header row."
    )

# ─────────────────────────────────────────────────────────────────────────────
# Run
# ─────────────────────────────────────────────────────────────────────────────
if st.button("🚀 Run Pricing Analysis", type="primary"):

    input_dir  = Path("temp_input")
    output_dir = Path("temp_output")
    if input_dir.exists():
        shutil.rmtree(input_dir)
    input_dir.mkdir(parents=True)

    for uf in uploaded_files:
        (input_dir / uf.name).write_bytes(uf.getbuffer())

    with st.spinner("Processing pricing data…"):
        try:
            results = run_pricing_etl(str(input_dir), str(output_dir))
        except Exception as e:
            st.error(f"❌ Unexpected error: {e}")
            st.stop()

    st.success(
        f"✅ Done!  **{results['n_loaded']} / {results['n_files']}** files loaded "
        f"— **{results['n_rows']:,}** total rows."
    )

    with st.expander("📋 Per-file load log", expanded=(unknown_count > 0)):
        for msg in results["messages"]:
            if msg.startswith("✅"):
                st.success(msg)
            elif msg.startswith("⚠️"):
                st.warning(msg)
            else:
                st.error(msg)

    st.divider()

    # ── Data previews ──────────────────────────────────────────────────────
    col_wide, col_long = st.columns(2)

    with col_wide:
        st.subheader("Wide Matrix (price comparison)")
        wide_df = pd.read_csv(results["wide"])
        if wide_df.empty:
            st.info("No data to display.")
        else:
            location_cols = [c for c in wide_df.columns
                             if c not in {"catalog_num","upc","description",
                                          "pricing_group","best_price",
                                          "best_location","price_spread","is_tied"}]

            def highlight_best(row):
                styles = [""] * len(row)
                if pd.notna(row.get("best_price")):
                    for i, col in enumerate(row.index):
                        if col in location_cols and row[col] == row["best_price"]:
                            styles[i] = "background-color:#d4edda;font-weight:bold"
                return styles

            st.dataframe(
                wide_df.head(300).style.apply(highlight_best, axis=1),
                use_container_width=True, height=420,
            )
            st.caption(
                f"{len(wide_df):,} unique products across "
                f"{len(location_cols)} location(s)"
            )

    with col_long:
        st.subheader("Long Format (all rows)")
        long_df = pd.read_csv(results["long"])
        if long_df.empty:
            st.info("No data to display.")
        else:
            locations = sorted(long_df["location"].unique())
            sel_loc = st.multiselect(
                "Filter by location", locations,
                default=locations[:4] if len(locations) > 4 else locations,
            )
            filtered = long_df[long_df["location"].isin(sel_loc)] if sel_loc else long_df
            st.dataframe(filtered.head(500), use_container_width=True, height=420)
            st.caption(f"Showing {min(500, len(filtered)):,} / {len(filtered):,} rows")

    # ── Downloads ──────────────────────────────────────────────────────────
    st.divider()
    st.subheader("📥 Download Results")
    dl1, dl2, dl3 = st.columns(3)

    with dl1:
        with open(results["wide"], "rb") as f:
            st.download_button(
                "⬇️ Wide Matrix CSV", f,
                file_name="wide_matrix.csv", mime="text/csv",
                use_container_width=True,
            )
    with dl2:
        with open(results["long"], "rb") as f:
            st.download_button(
                "⬇️ Long Format CSV", f,
                file_name="long_format.csv", mime="text/csv",
                use_container_width=True,
            )
    with dl3:
        with open(results["log"], "rb") as f:
            st.download_button(
                "⬇️ Load Log CSV", f,
                file_name="load_log.csv", mime="text/csv",
                use_container_width=True,
            )
