import os
import re
import warnings
import pandas as pd
from openpyxl import load_workbook
from pathlib import Path

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ---------------------------------------------------------------------------
# Canonical column names used throughout the pipeline
# ---------------------------------------------------------------------------
STANDARD_COLS = ["location", "upc", "catalog_num", "description",
                 "net_price_each", "pricing_group", "source_file", "format"]

# ---------------------------------------------------------------------------
# Column alias maps (lower-stripped keys → standard name)
# ---------------------------------------------------------------------------
_COL_ALIASES = {
    # UPC
    "upc code": "upc",
    "upc": "upc",
    "upc         ": "upc",   # Hubbell pads with spaces

    # Catalog / Item number
    "item": "catalog_num",
    "material": "catalog_num",
    "material        ": "catalog_num",
    "catalog #": "catalog_num",
    "catalog": "catalog_num",
    "mfr": "catalog_num",

    # Description
    "description": "description",
    "description                                          ": "description",

    # Net price
    "distributor price each": "net_price_each",
    "price": "net_price_each",
    "net price": "net_price_each",
    "net cost": "net_price_each",
    "net spa price": "net_price_each",
    "net": "net_price_each",

    # Pricing group / manufacturer
    "pricing group": "pricing_group",
    "prod group desc": "pricing_group",
    "mfrcode": "pricing_group",

    # Location  (parsed from filename / customer info sheet — not a column alias)
}


def _strip_alias(col: str) -> str:
    """Normalise a column header for alias lookup."""
    return str(col).strip().lower()


def _map_columns(df: pd.DataFrame) -> dict:
    """Return a dict of {standard_name: actual_col} for columns found in df."""
    mapping = {}
    for col in df.columns:
        alias = _strip_alias(col)
        if alias in _COL_ALIASES:
            std = _COL_ALIASES[alias]
            if std not in mapping:          # first match wins
                mapping[std] = col
    return mapping


# ---------------------------------------------------------------------------
# Location extraction from filename
# ---------------------------------------------------------------------------
def _location_from_filename(filename: str) -> str:
    """
    Extract a human-readable location tag from the filename.
    Examples:
      '2808_CED-GREA_02-09-2026__1_.xlsx'  → 'CED-GREA'
      '0757_209066_-_CED_-_BOISE__ID_-_3A.xlsx' → 'CED-BOISE-ID'
    Falls back to the stem if no pattern matches.
    """
    stem = Path(filename).stem

    # Pattern A: NNNN_CED-LOC_date…
    m = re.search(r'\d+_(CED-[A-Z]+)_', stem)
    if m:
        return m.group(1)

    # Pattern B: NNNN_ACCT_-_CED_-_CITY__STATE_-_…
    m = re.search(r'CED_-_([A-Z]+)__([A-Z]{2})', stem)
    if m:
        return f"CED-{m.group(1)}-{m.group(2)}"

    # Fallback: use whole stem, cleaned up
    return re.sub(r'[^A-Za-z0-9\-]', '-', stem)[:40]


# ---------------------------------------------------------------------------
# Format detectors
# ---------------------------------------------------------------------------
def _detect_format(filepath: str) -> str:
    """
    Peek at the file and decide which format it is.
    Returns: 'format_a', 'format_b', or 'unknown'
    """
    try:
        wb = load_workbook(filepath, read_only=True)
        sheets = wb.sheetnames
        wb.close()
    except Exception:
        return "unknown"

    if "Price List" not in sheets:
        return "unknown"

    try:
        wb = load_workbook(filepath, read_only=True)
        ws = wb["Price List"]
        rows = list(ws.iter_rows(max_row=10, values_only=True))
        wb.close()
    except Exception:
        return "unknown"

    # Format A: row 0 has "Pricing Group" or "Item"
    if rows:
        first_row = [str(v).strip().lower() if v else "" for v in rows[0]]
        if "pricing group" in first_row or "item" in first_row:
            return "format_a"

    # Format B: row 5 (index 5) has "UPC" / "MATERIAL" / "DESCRIPTION"
    if len(rows) > 5:
        row5 = [str(v).strip().lower() if v else "" for v in rows[5]]
        if "upc         " in row5 or "upc" in row5 or "material" in row5:
            return "format_b"

    return "unknown"


# ---------------------------------------------------------------------------
# Loaders per format
# ---------------------------------------------------------------------------
def _load_format_a(filepath: str, location: str) -> pd.DataFrame:
    """
    Format A: multi-vendor Bridgeport-style files.
    Sheet "Price List", headers on row 0.
    """
    df = pd.read_excel(filepath, sheet_name="Price List",
                       header=0, dtype=str, engine="openpyxl")
    df.columns = df.columns.str.strip()

    mapping = _map_columns(df)
    needed = {"upc", "catalog_num", "description", "net_price_each"}
    missing = needed - set(mapping.keys())
    if missing:
        raise ValueError(f"Format A missing columns: {missing}. "
                         f"Found: {list(df.columns)}")

    out = pd.DataFrame()
    out["upc"]           = df[mapping["upc"]].astype(str).str.strip()
    out["catalog_num"]   = df[mapping["catalog_num"]].astype(str).str.strip()
    out["description"]   = df[mapping["description"]].astype(str).str.strip()
    out["net_price_each"] = pd.to_numeric(
        df[mapping["net_price_each"]].astype(str).str.replace(r'[,$]', '', regex=True),
        errors="coerce")
    out["pricing_group"] = (df[mapping["pricing_group"]].astype(str).str.strip()
                            if "pricing_group" in mapping else "")
    out["location"]      = location
    out["source_file"]   = Path(filepath).name
    out["format"]        = "format_a"
    return out.dropna(subset=["net_price_each"])


def _load_format_b(filepath: str, location: str) -> pd.DataFrame:
    """
    Format B: Hubbell 3A distributor cost sheets.
    Sheet "Price List", real headers at row 5 (skiprows=5).
    """
    df = pd.read_excel(filepath, sheet_name="Price List",
                       skiprows=5, header=0, dtype=str, engine="openpyxl")
    # Drop the leading empty column (col A is blank in these files)
    df = df.dropna(axis=1, how="all")
    df.columns = df.columns.str.strip()

    mapping = _map_columns(df)
    needed = {"upc", "catalog_num", "description", "net_price_each"}
    missing = needed - set(mapping.keys())
    if missing:
        raise ValueError(f"Format B missing columns: {missing}. "
                         f"Found: {list(df.columns)}")

    out = pd.DataFrame()
    out["upc"]           = df[mapping["upc"]].astype(str).str.strip()
    out["catalog_num"]   = df[mapping["catalog_num"]].astype(str).str.strip()
    out["description"]   = df[mapping["description"]].astype(str).str.strip()
    out["net_price_each"] = pd.to_numeric(
        df[mapping["net_price_each"]].astype(str).str.replace(r'[,$]', '', regex=True),
        errors="coerce")
    out["pricing_group"] = (df[mapping["pricing_group"]].astype(str).str.strip()
                            if "pricing_group" in mapping else "")
    out["location"]      = location
    out["source_file"]   = Path(filepath).name
    out["format"]        = "format_b"
    return out.dropna(subset=["net_price_each"])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def load_pricing_file(filepath: str) -> tuple[pd.DataFrame, str]:
    """
    Load a single pricing file and return (dataframe, status_message).
    The dataframe always has STANDARD_COLS columns.
    On failure returns (empty_df, error_message).
    """
    filename = Path(filepath).name
    location = _location_from_filename(filename)
    fmt = _detect_format(filepath)

    try:
        if fmt == "format_a":
            df = _load_format_a(filepath, location)
            return df, f"✅ {filename} → {location} [{fmt}, {len(df):,} rows]"
        elif fmt == "format_b":
            df = _load_format_b(filepath, location)
            return df, f"✅ {filename} → {location} [{fmt}, {len(df):,} rows]"
        else:
            return (pd.DataFrame(columns=STANDARD_COLS),
                    f"⚠️  {filename} — unrecognised format, skipped")
    except Exception as e:
        return (pd.DataFrame(columns=STANDARD_COLS),
                f"❌ {filename} — error: {e}")


def load_pricing_files(folder_path: str) -> tuple[pd.DataFrame, list[str]]:
    """
    Load all .xlsx / .csv files in folder_path.
    Returns (combined_df, list_of_status_messages).
    """
    folder = Path(folder_path)
    files = list(folder.glob("*.xlsx")) + list(folder.glob("*.csv"))

    if not files:
        return pd.DataFrame(columns=STANDARD_COLS), ["No .xlsx/.csv files found."]

    frames, messages = [], []
    for fp in sorted(files):
        df, msg = load_pricing_file(str(fp))
        messages.append(msg)
        if not df.empty:
            frames.append(df)

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=STANDARD_COLS)
    return combined, messages


# ---------------------------------------------------------------------------
# Wide-format comparison matrix
# ---------------------------------------------------------------------------
def build_wide_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot the combined long DataFrame into a wide comparison matrix.

    Index: catalog_num  (primary), upc, description (carried along)
    Columns: one column per location → net_price_each

    Also computes: best_price, best_location, price_spread, is_tied.
    """
    if df.empty:
        return pd.DataFrame()

    # Deduplicate: if same location has same catalog_num, keep lowest price
    df_dedup = (df.sort_values("net_price_each")
                  .drop_duplicates(subset=["catalog_num", "location"], keep="first"))

    # Carry description and upc from the first occurrence of each catalog_num
    meta = (df_dedup.drop_duplicates(subset=["catalog_num"], keep="first")
                    [["catalog_num", "upc", "description", "pricing_group"]])

    pivot = df_dedup.pivot_table(
        index="catalog_num",
        columns="location",
        values="net_price_each",
        aggfunc="min"
    )
    pivot.columns.name = None
    pivot = pivot.reset_index()

    wide = meta.merge(pivot, on="catalog_num", how="right")

    # Price analytics
    price_cols = [c for c in wide.columns
                  if c not in {"catalog_num", "upc", "description",
                               "pricing_group", "best_price",
                               "best_location", "price_spread", "is_tied"}]

    wide["best_price"]    = wide[price_cols].min(axis=1)
    wide["best_location"] = wide[price_cols].idxmin(axis=1)
    wide["price_spread"]  = wide[price_cols].max(axis=1) - wide[price_cols].min(axis=1)
    wide["is_tied"]       = wide[price_cols].apply(
        lambda row: row.dropna().nunique() == 1 and row.notna().sum() > 1, axis=1)

    # Reorder: meta columns first, then location prices, then analytics
    meta_cols      = ["catalog_num", "upc", "description", "pricing_group"]
    analytic_cols  = ["best_price", "best_location", "price_spread", "is_tied"]
    col_order      = meta_cols + price_cols + analytic_cols
    col_order      = [c for c in col_order if c in wide.columns]
    return wide[col_order]


def get_standard_column(df: pd.DataFrame, standard_name: str) -> list:
    """Return all columns in df that map to the given standard name."""
    return [col for col in df.columns if _strip_alias(col) in _COL_ALIASES
            and _COL_ALIASES[_strip_alias(col)] == standard_name]
