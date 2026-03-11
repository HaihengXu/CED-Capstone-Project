Vendor Pricing ETL & Analysis
This project automates the extraction, transformation, and loading (ETL) of pricing data from multiple vendors. It standardizes inconsistent column naming conventions and provides a side-by-side "Wide Format" comparison matrix to identify the best prices and price spreads across different locations.

### Key Features
Inconsistent Data Handling: Automatically maps varying column names (e.g., "Net SPA Price" vs. "Net Price") to a standard schema.

Multi-Format Support: Processes both .xlsx and .csv files while handling encoding issues (UTF-8 and Latin-1).

Wide-Format Analysis: Creates a MultiIndex DataFrame that allows for direct side-by-side comparison of product attributes across different vendors.

Price Optimization: Calculates best_price, identifies the best_location, and flags pricing ties and spreads.

### Project Structure
functions.py: The core engine containing data loading, cleaning, and matrix generation logic.

pricingfiles_ETL.py: The execution script that orchestrates the workflow.

data/: (Not included in repo) The folder where vendor Excel/CSV files should be placed.

### Installation
Clone the repository:

Bash
git clone https://github.com/yourusername/vendor-pricing-etl.git
cd vendor-pricing-etl
Install dependencies:

Bash
pip install pandas openpyxl
### Usage
Place your vendor files in a folder. Ensure filenames follow the convention: VendorName - Location.xlsx (e.g., CED - Missoula.xlsx).

Run the ETL script:

Bash
python pricingfiles_ETL.py
Example Implementation
Python
from functions import load_and_pivot_data, get_columns_by_standard_name

# Load and create the wide-format matrix
df_wide = load_and_pivot_data("path/to/your/folder")

# Extract all 'Net Price' columns for comparison
net_price_cols = get_columns_by_standard_name(df_wide, "Net Price")
best_prices = df_wide[net_price_cols].min(axis=1)
### Data Standards
The system expects four primary product identifiers to align data across files:

MfrCode (Manufacturer Code)

Catalog #

UPC

Description

Note: The get_standard_column function in functions.py can be updated to include additional column aliases as you encounter new vendor formats.