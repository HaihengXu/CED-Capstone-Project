from pathlib import Path
import argparse
import sys

DEFAULT_FOLDER_PATH = r"C:/Users/haihe/Desktop/CED Capstone/PricingFiles"


# Make project root importable when running from Model/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
	sys.path.insert(0, str(PROJECT_ROOT))

from functions_haiheng_20260308_v1 import (
	load_pricing_files,
	clean_pricing_data,
	create_price_matrix,
	combine_with_original_data,
	load_and_pivot_data,
)


def run_pricing_etl(
	folder_path,
	output_dir=None,
	recursive=False,
	include_parent_in_location=False,
	save_wide=True,
):
	"""
	Run pricing ETL and save outputs.

	Args:
		folder_path: Input folder with pricing files
		output_dir: Folder to write outputs; defaults to <folder_path>/output
		recursive: If True, search all subfolders for input files
		include_parent_in_location: If True, include parent folder in location labels
		save_wide: If True, generate wide-format comparison output

	Returns:
		Dict with output file paths
	"""
	input_folder = Path(folder_path)
	out_dir = Path(output_dir) if output_dir else input_folder / "output"
	out_dir.mkdir(parents=True, exist_ok=True)

	print("[1/5] Loading pricing files...")
	df_raw = load_pricing_files(
		input_folder,
		recursive=recursive,
		include_parent_in_location=include_parent_in_location,
	)

	print("[2/5] Cleaning pricing data...")
	df_clean = clean_pricing_data(df_raw)

	print("[3/5] Building price matrix...")
	price_matrix = create_price_matrix(df_clean)

	print("[4/5] Merging matrix back to cleaned data...")
	df_final = combine_with_original_data(df_clean, price_matrix)

	raw_path = out_dir / "pricing_raw.csv"
	clean_path = out_dir / "pricing_clean.csv"
	matrix_path = out_dir / "price_matrix.csv"
	final_path = out_dir / "pricing_final.csv"

	df_raw.to_csv(raw_path, index=False)
	df_clean.to_csv(clean_path, index=False)
	price_matrix.to_csv(matrix_path, index=False)
	df_final.to_csv(final_path, index=False)

	results = {
		"raw": str(raw_path),
		"clean": str(clean_path),
		"matrix": str(matrix_path),
		"final": str(final_path),
	}

	if save_wide:
		print("[5/5] Creating wide-format comparison...")
		wide_df = load_and_pivot_data(
			input_folder,
			recursive=recursive,
			include_parent_in_location=include_parent_in_location,
		)
		wide_path = out_dir / "pricing_wide_comparison.xlsx"
		wide_df.to_excel(wide_path)
		results["wide"] = str(wide_path)
	else:
		print("[5/5] Skipping wide-format output.")

	return results


def parse_args():
	parser = argparse.ArgumentParser(description="Run pricing files ETL pipeline")
	parser.add_argument(
		"folder_path",
		nargs="?",
		default=None,
		help="Path to the folder that contains pricing files (optional if DEFAULT_FOLDER_PATH is set)",
	)
	parser.add_argument(
		"--output-dir",
		default=None,
		help="Optional output directory (default: <folder_path>/output)",
	)
	parser.add_argument(
		"--recursive",
		action="store_true",
		help="Search for pricing files in subfolders recursively",
	)
	parser.add_argument(
		"--include-parent-in-location",
		action="store_true",
		help="Prefix location with relative parent folder name",
	)
	parser.add_argument(
		"--skip-wide",
		action="store_true",
		help="Skip generating wide-format comparison output",
	)
	return parser.parse_args()


def main():
	args = parse_args()
	folder_to_use = args.folder_path or DEFAULT_FOLDER_PATH

	if not folder_to_use:
		raise ValueError("No folder path provided. Pass folder_path or set DEFAULT_FOLDER_PATH in this script.")

	try:
		output_files = run_pricing_etl(
			folder_path=folder_to_use,
			output_dir=args.output_dir,
			recursive=args.recursive,
			include_parent_in_location=args.include_parent_in_location,
			save_wide=not args.skip_wide,
		)

		print("ETL completed successfully. Output files:")
		for key, value in output_files.items():
			print(f"- {key}: {value}")
	except Exception as exc:
		print(f"ETL failed: {exc}")
		raise


if __name__ == "__main__":
	main()
