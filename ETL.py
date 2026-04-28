import streamlit as st
import pandas as pd
import os
import shutil
from pathlib import Path
import sys

# Ensure folders are in the search path
sys.path.append('Functions')
sys.path.append('Model')

# Use standard 4-space indentation for the entire file
from functions_haiheng_20260308_v1 import load_pricing_files
from pricingfiles_ETL_haiheng_20260308_v1 import run_pricing_etl

st.set_page_config(page_title="CED Pricing Analysis Tool", layout="wide")

st.title("📊 Pricing ETL Analysis Tool")
st.markdown("Upload your pricing files (CSV or XLSX) to generate a consolidated price matrix.")

# File Uploader
uploaded_files = st.file_uploader(
    "Choose pricing files", 
    accept_multiple_files=True, 
    type=['csv', 'xlsx']
)

if uploaded_files:
    input_dir = Path("temp_input")
    output_dir = Path("temp_output")
    
    if input_dir.exists():
        shutil.rmtree(input_dir)
    input_dir.mkdir(exist_ok=True, parents=True)

    for uploaded_file in uploaded_files:
        with open(input_dir / uploaded_file.name, "wb") as f:
            f.write(uploaded_file.getbuffer())

    st.success(f"Successfully uploaded {len(uploaded_files)} files.")

    if st.button("🚀 Run Pricing Analysis"):
        with st.spinner("Processing data..."):
            try:
                results = run_pricing_etl(
                    folder_path=str(input_dir),
                    output_dir=str(output_dir)
                )
                
                st.balloons()
                st.divider()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Results Preview")
                    final_df = pd.read_csv(results['final'])
                    st.dataframe(final_df.head(10))
                
                with col2:
                    st.subheader("Download Reports")
                    for label, path in results.items():
                        with open(path, "rb") as f:
                            st.download_button(
                                label=f"Download {label.upper()} File",
                                data=f,
                                file_name=os.path.basename(path),
                                mime="text/csv"
                            )
                            
            except Exception as e:
                st.error(f"Analysis failed: {e}")
