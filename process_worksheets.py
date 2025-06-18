import pandas as pd
from pathlib import Path
import argparse
import re

def sanitize_filename(name):
    """Sanitizes a string to be a valid filename."""
    # Replace path separators with underscore
    name = name.replace('/', '_').replace('\\', '_')
    # Remove other invalid characters
    name = re.sub(r'[<>:"|?*]', '', name)
    # Replace whitespace with underscores
    name = re.sub(r'\s+', '_', name)
    return name

def process_excel_files(input_dir: Path, output_dir: Path):
    """
    Processes all .xlsx files in the input directory, converting each sheet to a CSV file.
    The output filename is a concatenation of the relative path, original filename, and sheet name.
    """
    # Ensure the output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Searching for .xlsx files in: {input_dir}")
    excel_files = list(input_dir.rglob("*.xlsx"))
    print(f"Found {len(excel_files)} Excel files to process.")

    if not excel_files:
        print("No .xlsx files found in the specified directory.")
        return

    for excel_file in excel_files:
        # Skip macOS metadata files and directories
        if '__MACOSX' in excel_file.parts or excel_file.name.startswith('._'):
            continue

        try:
            # Get the relative path to create a unique prefix
            relative_path = excel_file.relative_to(input_dir)
            
            # Create a clean prefix from the path parts and filename stem
            # e.g., 'SubFolder/MyFile.xlsx' -> 'SubFolder_MyFile'
            path_prefix = '_'.join(relative_path.with_suffix('').parts)
            sanitized_path_prefix = sanitize_filename(path_prefix)

            xls = pd.ExcelFile(excel_file)
            if not xls.sheet_names:
                print(f"  Skipping empty file (no sheets): {excel_file.name}")
                continue

            for sheet_name in xls.sheet_names:
                print(f"  Processing file: {excel_file.name}, sheet: {sheet_name}")
                
                # Sanitize sheet name for the filename
                sanitized_sheet_name = sanitize_filename(sheet_name)
                
                # Construct the final output filename
                output_filename = f"{sanitized_path_prefix}_{sanitized_sheet_name}.csv"
                output_path = output_dir / output_filename
                
                # Read sheet and save as CSV
                df = pd.read_excel(xls, sheet_name=sheet_name)
                df.to_csv(output_path, index=False)
                
        except Exception as e:
            print(f"Could not process file {excel_file}: {e}")

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Convert Excel sheets to individual CSV files with unique names.")
    parser.add_argument("input_dir", type=str, help="The input directory containing .xlsx files.")
    parser.add_argument("output_dir", type=str, help="The directory where CSV files will be saved.")
    
    args = parser.parse_args()
    
    input_path = Path(args.input_dir)
    output_path = Path(args.output_dir)
    
    if not input_path.is_dir():
        print(f"Error: Input directory not found at {input_path}")
        return
        
    process_excel_files(input_path, output_path)
    print("\nProcessing complete.")
    print(f"CSV files saved in: {output_path}")

if __name__ == "__main__":
    main()
