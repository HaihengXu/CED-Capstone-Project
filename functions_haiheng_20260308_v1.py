import pandas as pd
from pathlib import Path
import warnings

# This function handles the "messy" reality of folder management
def load_pricing_files(folder_path):
    """
    Load and combine pricing files from a folder.
    
    Args:
        folder_path: Path to folder containing Excel/CSV files
        
    Returns:
        Combined DataFrame with all pricing data
        
    Raises:
        ValueError: If folder doesn't exist or no valid files found
        TypeError: If folder_path is not a valid path type
    """
    # Input validation
    if folder_path is None:
        raise ValueError("folder_path cannot be None")
    
    try:
        folder = Path(folder_path)
    except (TypeError, ValueError) as e:
        raise TypeError(f"Invalid folder_path type: {e}")
    
    # Check if folder exists
    if not folder.exists():
        raise ValueError(f"Folder does not exist: {folder_path}")
    
    if not folder.is_dir():
        raise ValueError(f"Path is not a directory: {folder_path}")
    
    dfs = []
    errors = []

    # Use a pattern that catches both extensions
    # .glob("*") or filtering within the loop is safest
    for file in folder.iterdir():
        # Skip directories and temporary Excel lock files
        if file.is_dir() or file.name.startswith("~$"):
            continue
            
        # Check the file extension
        ext = file.suffix.lower()
        
        try:
            if ext == ".xlsx":
                df = pd.read_excel(file, dtype=str)
            elif ext == ".csv":
                # CSVs often need an encoding specified if they have special characters
                try:
                    df = pd.read_csv(file, dtype=str, encoding='utf-8')
                except UnicodeDecodeError:
                    # Try alternative encodings
                    df = pd.read_csv(file, dtype=str, encoding='latin-1')
                    warnings.warn(f"Used latin-1 encoding for {file.name}")
            else:
                # Skip files that aren't Excel or CSV
                continue
            
            # Check if dataframe is empty
            if df.empty:
                warnings.warn(f"File {file.name} is empty, skipping")
                continue

            # Extract location code from filename
            location = file.stem.split(" - ")[-1] if " - " in file.stem else file.stem
            df["location"] = location

            dfs.append(df)
            
        except Exception as e:
            # Track errors but continue processing other files
            errors.append(f"{file.name}: {str(e)}")
            continue

    # Report any errors encountered
    if errors:
        warnings.warn(f"Errors encountered while loading {len(errors)} file(s): {errors}")
    
    if not dfs:
        raise ValueError(f"No valid Excel or CSV files found in the directory: {folder_path}")

    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Validate the combined dataframe has data
    if combined_df.empty:
        raise ValueError("Combined dataframe is empty")
    
    return combined_df

# This function standardizes column names to handle variations in input files
def get_standard_column(col_name):
    """
    Maps various incoming column names to standard internal keys.
    """
    name = str(col_name).lower().strip()
    # Logic for flexible naming
    if any(x in name for x in ["net price", "spa price", "net_price"]):
        return "Net Price"
    if any(x in name for x in ["dist cost", "dist_cost", "unit cost"]):
        return "Dist Cost"
    if "uom" in name:
        return "UOM"
    if "disc" in name:
        return "Disc"
    return None

# Data Cleaning
def clean_pricing_data(df):
    """
    Clean and validate pricing data.
    
    Args:
        df: DataFrame with pricing data
        
    Returns:
        Cleaned DataFrame
        
    Raises:
        ValueError: If required columns are missing or data is invalid
        TypeError: If input is not a DataFrame
    """
    # Input validation
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
    
    if df.empty:
        raise ValueError("Input DataFrame is empty")
    
    # Check required columns exist
    required_columns = ["UPC", "Catalog #", "Net Price"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}. Available columns: {list(df.columns)}")
    
    # Create a copy to avoid modifying original
    df = df.copy()
    
    original_count = len(df)
    
    # Rename key columns for processing (but keep originals)
    df["upc"] = df["UPC"].astype(str).str.strip()
    df["catalog_number"] = df["Catalog #"].astype(str).str.strip()

    # Clean and convert net price with error handling
    try:
        df["net_price"] = (
            df["Net Price"]
            .astype(str)
            .str.replace(r"[\$,]", "", regex=True)
            .replace("", pd.NA)
        )
        # Convert to float, coercing errors to NaN
        df["net_price"] = pd.to_numeric(df["net_price"], errors='coerce')
    except Exception as e:
        raise ValueError(f"Error converting Net Price to numeric: {e}")

    # Remove rows with missing UPC or invalid prices
    df = df.dropna(subset=["upc", "net_price"])
    df = df[df["net_price"] > 0]
    
    # Validate we still have data after cleaning
    if df.empty:
        raise ValueError(f"All {original_count} rows were removed during cleaning. Check your data for valid UPC and Net Price values.")
    rows_removed = original_count - len(df)

    if rows_removed > 0:
        warnings.warn(f"Removed {rows_removed} rows ({rows_removed/original_count*100:.1f}%) with missing or invalid data")
    return df

# The Analysis Engine
def create_price_matrix(df):
    """
    Create a price comparison matrix showing prices across different locations.
    
    Args:
        df: Cleaned DataFrame with pricing data
        
    Returns:
        Price matrix with best price analysis
        
    Raises:
        ValueError: If required columns missing or data invalid
        TypeError: If input is not a DataFrame
    """
    # Input validation
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
    
    if df.empty:
        raise ValueError("Input DataFrame is empty")
    
    # Check required columns
    required_columns = ["upc", "location", "net_price"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}. Available columns: {list(df.columns)}")
    
    # Check for at least one location
    unique_locations = df["location"].nunique()
    if unique_locations == 0:
        raise ValueError("No locations found in data")
    
    try:
        price_matrix = (
            df.pivot_table(
                index="upc",
                columns="location",
                values="net_price",
                aggfunc="min"
            )
            .reset_index()
        )
    except Exception as e:
        raise ValueError(f"Error creating pivot table: {e}")
    
    if price_matrix.empty:
        raise ValueError("Price matrix is empty after pivot operation")

    price_cols = price_matrix.columns.drop("upc")
    
    if len(price_cols) == 0:
        raise ValueError("No price columns found after pivot")

    price_matrix["best_price"] = price_matrix[price_cols].min(axis=1)
    price_matrix["best_location"] = price_matrix[price_cols].idxmin(axis=1)

    price_matrix["price_spread"] = (
        price_matrix[price_cols].max(axis=1) - price_matrix["best_price"]
    )

    price_matrix["tie_flag"] = (
        price_matrix[price_cols]
        .eq(price_matrix["best_price"], axis=0)
        .sum(axis=1) > 1
    )
    
    return price_matrix

# Final Merging
def combine_with_original_data(df_clean, price_matrix):
    """
    Merge the calculated price analysis columns back to the original data.
    This keeps all original columns and adds the new calculated columns.
    
    Args:
        df_clean: Cleaned pricing DataFrame
        price_matrix: Price matrix with calculated columns
        
    Returns:
        Combined DataFrame with original and calculated columns
        
    Raises:
        ValueError: If required columns missing
        TypeError: If inputs are not DataFrames
    """
    # Input validation
    if not isinstance(df_clean, pd.DataFrame):
        raise TypeError("df_clean must be a pandas DataFrame")
    
    if not isinstance(price_matrix, pd.DataFrame):
        raise TypeError("price_matrix must be a pandas DataFrame")
    
    if df_clean.empty:
        raise ValueError("df_clean is empty")
    
    if price_matrix.empty:
        raise ValueError("price_matrix is empty")
    
    # Check required columns exist
    if "upc" not in df_clean.columns:
        raise ValueError("df_clean missing 'upc' column")
    
    # Select only the calculated columns from price_matrix to merge
    calculated_cols = ["upc", "best_price", "best_location", "price_spread", "tie_flag"]
    missing_cols = [col for col in calculated_cols if col not in price_matrix.columns]
    
    if missing_cols:
        raise ValueError(f"price_matrix missing required columns: {missing_cols}")
    
    price_info = price_matrix[calculated_cols]
    
    # Merge back to original data based on UPC
    try:
        df_final = df_clean.merge(price_info, on="upc", how="left")
    except Exception as e:
        raise ValueError(f"Error merging dataframes: {e}")
    
    # Validate merge results
    if df_final.empty:
        raise ValueError("Merged dataframe is empty")
    
    # Check for unmatched rows
    unmatched = df_final["best_price"].isna().sum()
    if unmatched > 0:
        warnings.warn(f"{unmatched} rows ({unmatched/len(df_final)*100:.1f}%) did not match in merge")
    
    return df_final

# Helper function for column name standardization
def get_standard_column(column_name):
    """
    Standardize varying column names to consistent names.
    
    Args:
        column_name: Original column name from file
        
    Returns:
        Standardized column name, or None if not a recognized column
    """
    if not isinstance(column_name, str):
        return None
    
    # Convert to lowercase for case-insensitive matching
    col_lower = column_name.lower().strip()
    
    # Define mapping patterns for common variations
    standardization_map = {
        'net price': ['net price', 'net spa price', 'netprice', 'net_price'],
        'dist cost': ['dist cost', 'distributor cost', 'distcost', 'dist_cost'],
        'uom': ['uom', 'unit of measure', 'unit'],
        'disc': ['disc', 'discount', 'disc%', 'discount%'],
        'mfrcode': ['mfrcode', 'mfr code', 'manufacturer code', 'mfr_code'],
        'catalog #': ['catalog #', 'catalog', 'catalog number', 'catalog_number'],
        'upc': ['upc', 'upc code', 'upc_code'],
        'description': ['description', 'desc', 'product description']
    }
    
    # Check each standard name
    for standard_name, variations in standardization_map.items():
        if col_lower in variations:
            # Return with proper casing
            if standard_name == 'mfrcode':
                return 'MfrCode'
            elif standard_name == 'catalog #':
                return 'Catalog #'
            elif standard_name == 'upc':
                return 'UPC'
            elif standard_name == 'description':
                return 'Description'
            elif standard_name == 'net price':
                return 'Net Price'
            elif standard_name == 'dist cost':
                return 'Dist Cost'
            elif standard_name == 'uom':
                return 'UOM'
            elif standard_name == 'disc':
                return 'Disc'
    
    return None

# Wide Format Data Loader
def load_and_pivot_data(folder_path):
    """
    Load pricing files and create a wide-format comparison with MultiIndex columns.
    
    This function creates a side-by-side comparison where:
    - Rows are indexed by product identifiers (MfrCode, Catalog #, UPC, Description)
    - Columns are grouped by location with pricing details underneath
    - Allows easy visual comparison of the same product across locations
    
    Args:
        folder_path: Path to folder containing Excel/CSV files
        
    Returns:
        DataFrame with MultiIndex columns: (Location, PricingField)
        
    Raises:
        ValueError: If folder doesn't exist or no valid files found
        TypeError: If folder_path is not a valid path type
    """
    # Input validation
    if folder_path is None:
        raise ValueError("folder_path cannot be None")
    
    try:
        folder = Path(folder_path)
    except (TypeError, ValueError) as e:
        raise TypeError(f"Invalid folder_path type: {e}")
    
    # Check if folder exists
    if not folder.exists():
        raise ValueError(f"Folder does not exist: {folder_path}")
    
    if not folder.is_dir():
        raise ValueError(f"Path is not a directory: {folder_path}")
    
    all_dfs = []
    errors = []

    # 1. Identify first 4 fixed columns (product identifiers)
    fixed_cols = ["MfrCode", "Catalog #", "UPC", "Description"]
    
    for file in folder.iterdir():
        # Skip non-data files
        if file.suffix.lower() not in ['.xlsx', '.csv'] or file.name.startswith("~$"):
            continue
        
        try:
            # Load file based on extension
            if file.suffix.lower() == '.xlsx':
                df = pd.read_excel(file)
            else:
                try:
                    df = pd.read_csv(file, encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(file, encoding='latin-1')
                    warnings.warn(f"Used latin-1 encoding for {file.name}")
            
            # Check if dataframe is empty
            if df.empty:
                warnings.warn(f"File {file.name} is empty, skipping")
                continue
            
            # 2. Extract Vendor/Location from filename
            location_label = file.stem.split(" - ")[-1] if " - " in file.stem else file.stem
    
            # 3. Identify which columns to keep (WITHOUT renaming them)
            # We check the standardized version but keep the original column name
            keep_cols = []
            
            # First, add the fixed columns (product identifiers) if they exist
            for col in df.columns:
                std_name = get_standard_column(col)
                if std_name in fixed_cols and col not in keep_cols:
                    keep_cols.append(col)
            
            # Then, add pricing/data columns we want to track
            target_standard_cols = ["Dist Cost", "UOM", "Disc", "Net Price"]
            for col in df.columns:
                std_name = get_standard_column(col)
                if std_name in target_standard_cols and col not in keep_cols:
                    keep_cols.append(col)
            
            if not keep_cols:
                warnings.warn(f"File {file.name} has no recognizable columns, skipping")
                continue
            
            # Verify we have at least some fixed columns for indexing
            # Identify which of the kept columns are actually the fixed/index columns
            available_fixed = []
            for col in keep_cols:
                std_name = get_standard_column(col)
                if std_name in fixed_cols:
                    available_fixed.append(col)  # Use ORIGINAL column name
            
            if not available_fixed:
                warnings.warn(f"File {file.name} missing all index columns, skipping")
                continue
            
            # Filter dataframe to only the columns we want
            df = df[keep_cols]
    
            # 5. Set Index to the fixed columns (product identifiers)
            # Use the ORIGINAL column names that map to our fixed columns
            df = df.set_index(available_fixed)
    
            # 6. Add the Location label as a top-level column index (MultiIndex)
            df.columns = pd.MultiIndex.from_product([[location_label], df.columns])
            
            all_dfs.append(df)
            
        except Exception as e:
            # Track errors but continue processing other files
            errors.append(f"{file.name}: {str(e)}")
            continue
    
    # Report any errors encountered
    if errors:
        warnings.warn(f"Errors encountered while loading {len(errors)} file(s): {errors}")
    
    if not all_dfs:
        raise ValueError(f"No valid Excel or CSV files found in the directory: {folder_path}")
    
    # 7. Concatenate all DataFrames horizontally (side-by-side)
    try:
        wide_df = pd.concat(all_dfs, axis=1)
    except Exception as e:
        raise ValueError(f"Error combining dataframes: {e}")
    
    # Validate result
    if wide_df.empty:
        raise ValueError("Combined dataframe is empty")
    
    return wide_df

# Helper function for analyzing wide-format data
def get_columns_by_standard_name(df, standard_name):
    """
    Find all columns in a MultiIndex DataFrame that match a standard column type.
    
    Useful for analysis on wide-format data where column names may vary across locations.
    For example, find all "Net Price" columns even if some are named "Net SPA Price".
    
    Args:
        df: DataFrame with MultiIndex columns (Location, ColumnName)
        standard_name: The standard column name to search for (e.g., "Net Price")
        
    Returns:
        List of (location, original_column_name) tuples that match the standard name
        
    Example:
        >>> net_price_cols = get_columns_by_standard_name(df, "Net Price")
        >>> # Returns: [('NYC', 'Net Price'), ('LA', 'Net SPA Price'), ...]
        >>> # Now you can access: df[net_price_cols]
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
    
    if not isinstance(df.columns, pd.MultiIndex):
        raise ValueError("DataFrame must have MultiIndex columns (Location, ColumnName)")
    
    matching_cols = []
    
    # Iterate through all column combinations in the MultiIndex
    for location, col_name in df.columns:
        # Check if this column's standardized name matches what we're looking for
        std_name = get_standard_column(col_name)
        if std_name == standard_name:
            matching_cols.append((location, col_name))
    
    return matching_cols