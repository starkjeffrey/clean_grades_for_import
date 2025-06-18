#!/usr/bin/env python3
"""
Grade File Deduplicator and Normalizer
Converts all grade files to uniquely named CSVs while eliminating true duplicates
"""

import pandas as pd
import os
import hashlib
from pathlib import Path
import logging
from typing import Dict, Set, List
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FileDeduplicator:
    def __init__(self, input_directory: str, output_directory: str):
        self.input_dir = Path(input_directory)
        self.output_dir = Path(output_directory)
        self.output_dir.mkdir(exist_ok=True)
        
        # Track processed files and their hashes
        self.file_hashes: Dict[str, str] = {}  # hash -> original_path
        self.processed_files: Set[str] = set()
        self.duplicate_files: List[str] = []
        
    def clean_filename(self, text: str) -> str:
        """Clean text for use in filename"""
        # Remove or replace problematic characters
        text = re.sub(r'[<>:"/\\|?*]', '-', text)
        text = re.sub(r'[\s]+', '_', text)  # Replace spaces with underscores
        text = re.sub(r'[-_]+', '_', text)  # Collapse multiple separators
        return text.strip('_-')
    
    def get_csv_content_hash(self, filepath: Path) -> str | None:
        """Computes a SHA256 hash of the CSV content, ignoring header and column order."""
        try:
            df = pd.read_csv(filepath, header=0, on_bad_lines='skip')
            if df.empty:
                return "EMPTY_FILE"
            # Sort columns to ensure order doesn't affect hash
            df = df.reindex(sorted(df.columns), axis=1)
            # Convert to a canonical string representation
            data_string = df.to_string(index=False).encode('utf-8')
            return hashlib.sha256(data_string).hexdigest()
        except pd.errors.EmptyDataError:
            # Return a specific hash for files with only a header
            return "HEADER_ONLY_FILE"
        except Exception as e:
            logger.error(f"Could not read or process CSV for hashing {filepath}: {e}")
            return None

    def get_binary_hash(self, filepath: Path) -> str:
        """Generate SHA256 hash of file's binary content for duplicate detection."""
        try:
            hash_sha256 = hashlib.sha256()
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            logger.error(f"Error hashing file {filepath}: {e}")
            return ""

    def get_content_hash(self, filepath: Path) -> str | None:
        """Dispatches to the correct hashing function based on file type."""
        if filepath.suffix.lower() == '.csv':
            return self.get_csv_content_hash(filepath)
        else:
            return self.get_binary_hash(filepath)

    def generate_unique_filename(self, filepath: Path, sheet_name: str = None) -> str:
        """Generate unique filename based on full path structure (handles deep nesting)"""
        # Get relative path from input directory
        try:
            rel_path = filepath.relative_to(self.input_dir)
        except ValueError:
            # File is not under input_dir, use absolute path parts
            rel_path = Path(*filepath.parts[-5:])  # Take last 5 parts to avoid extremely long names
        
        # Build path components from ALL directory levels
        path_parts = []
        
        # Add ALL directory structure (excluding the filename)
        if len(rel_path.parts) > 1:
            dirs = rel_path.parts[:-1]  # All directory parts except filename
            # Clean each directory name and add to path
            for dir_part in dirs:
                clean_dir = self.clean_filename(dir_part)
                if clean_dir and clean_dir not in ['data', 'files', 'documents']:  # Skip generic folder names
                    path_parts.append(clean_dir)
        
        # Add filename (without extension)
        filename_base = self.clean_filename(rel_path.stem)
        if filename_base:
            path_parts.append(filename_base)
        
        # Add sheet name if provided and meaningful
        if sheet_name and sheet_name.lower() not in ['sheet1', 'sheet', 'data', 'main']:
            clean_sheet = self.clean_filename(sheet_name)
            if clean_sheet:
                path_parts.append(clean_sheet)
        
        # Join all parts with underscores
        unique_name = '_'.join(path_parts)
        
        # Handle edge cases
        if not unique_name:
            unique_name = f"unknown_file_{hash(str(filepath)) % 10000}"
        
        # Ensure reasonable length (filesystem limits)
        if len(unique_name) > 200:
            # Truncate middle parts, keep first and last
            if len(path_parts) > 2:
                truncated = [path_parts[0]] + ['...'] + path_parts[-2:]
                unique_name = '_'.join(truncated)
            if len(unique_name) > 200:
                unique_name = unique_name[:200]
        
        return f"{unique_name}.csv"

    def is_duplicate_file(self, filepath: Path) -> bool:
        """Check if file is a duplicate based on content hash."""
        file_hash = self.get_content_hash(filepath)
        if not file_hash:
            logger.warning(f"Could not generate hash for {filepath}. Skipping duplicate check.")
            return False
        
        if file_hash in self.file_hashes:
            logger.info(f"DUPLICATE FOUND: {filepath} is a duplicate of {self.file_hashes[file_hash]}")
            self.duplicate_files.append(str(filepath))
            return True
        
        self.file_hashes[file_hash] = str(filepath)
        return False
    
    def process_excel_file(self, filepath: Path) -> List[str]:
        """Process Excel file and split sheets into separate CSVs"""
        output_files = []
        
        try:
            # Read all sheets
            excel_file = pd.ExcelFile(filepath)
            
            for sheet_name in excel_file.sheet_names:
                try:
                    df = pd.read_excel(filepath, sheet_name=sheet_name)
                    
                    # Skip empty sheets
                    if df.empty or df.shape[0] == 0:
                        logger.info(f"Skipping empty sheet '{sheet_name}' in {filepath}")
                        continue
                    
                    # Generate unique filename
                    output_filename = self.generate_unique_filename(filepath, sheet_name)
                    output_path = self.output_dir / output_filename
                    
                    # Handle filename conflicts
                    counter = 1
                    original_output_path = output_path
                    while output_path.exists():
                        name_parts = original_output_path.stem
                        output_path = self.output_dir / f"{name_parts}_v{counter}.csv"
                        counter += 1
                    
                    # Save as CSV
                    df.to_csv(output_path, index=False)
                    output_files.append(str(output_path))
                    
                    logger.info(f"Converted: {filepath} (sheet: {sheet_name}) -> {output_path}")
                    
                except Exception as e:
                    logger.error(f"Error processing sheet '{sheet_name}' in {filepath}: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"Error reading Excel file {filepath}: {e}")
        
        return output_files
    
    def process_csv_file(self, filepath: Path) -> List[str]:
        """Process CSV file (copy with unique name)"""
        output_files = []
        
        try:
            # Try reading to validate it's a proper CSV
            df = pd.read_csv(filepath)
            
            if df.empty:
                logger.info(f"Skipping empty CSV: {filepath}")
                return output_files
            
            # Generate unique filename
            output_filename = self.generate_unique_filename(filepath)
            output_path = self.output_dir / output_filename
            
            # Handle filename conflicts
            counter = 1
            original_output_path = output_path
            while output_path.exists():
                name_parts = original_output_path.stem
                output_path = self.output_dir / f"{name_parts}_v{counter}.csv"
                counter += 1
            
            # Copy with consistent format
            df.to_csv(output_path, index=False)
            output_files.append(str(output_path))
            
            logger.info(f"Converted: {filepath} -> {output_path}")
            
        except Exception as e:
            logger.error(f"Error processing CSV file {filepath}: {e}")
        
        return output_files
    
    def process_pdf_file(self, filepath: Path) -> List[str]:
        """Process PDF file (just rename for now - content extraction handled later)"""
        output_files = []
        
        try:
            # Generate unique filename but keep as PDF
            output_filename = self.generate_unique_filename(filepath).replace('.csv', '.pdf')
            output_path = self.output_dir / output_filename
            
            # Handle filename conflicts
            counter = 1
            original_output_path = output_path
            while output_path.exists():
                name_parts = original_output_path.stem
                ext = original_output_path.suffix
                output_path = self.output_dir / f"{name_parts}_v{counter}{ext}"
                counter += 1
            
            # Copy PDF file
            import shutil
            shutil.copy2(filepath, output_path)
            output_files.append(str(output_path))
            
            logger.info(f"Copied PDF: {filepath} -> {output_path}")
            
        except Exception as e:
            logger.error(f"Error processing PDF file {filepath}: {e}")
        
        return output_files
    
    def find_all_files(self) -> List[Path]:
        """Find all relevant files recursively"""
        supported_extensions = ['.xlsx', '.xls', '.csv', '.pdf']
        files = []
        
        for root, dirs, filenames in os.walk(self.input_dir):
            for filename in filenames:
                if any(filename.lower().endswith(ext) for ext in supported_extensions):
                    files.append(Path(root) / filename)
        
        logger.info(f"Found {len(files)} files to process")
        return files
    
    def process_all_files(self):
        """Main processing function"""
        files = self.find_all_files()
        all_output_files = []
        
        for filepath in files:
            logger.info(f"Processing: {filepath}")
            
            # Check for duplicates first
            if self.is_duplicate_file(filepath):
                continue
            
            # Process based on file type
            if filepath.suffix.lower() in ['.xlsx', '.xls']:
                output_files = self.process_excel_file(filepath)
            elif filepath.suffix.lower() == '.csv':
                output_files = self.process_csv_file(filepath)
            elif filepath.suffix.lower() == '.pdf':
                output_files = self.process_pdf_file(filepath)
            else:
                continue
            
            all_output_files.extend(output_files)
            self.processed_files.add(str(filepath))
        
        self.generate_summary_report(all_output_files)
    
    def generate_summary_report(self, output_files: List[str]):
        """Generate summary of processing"""
        summary = {
            'Total Input Files Found': len(self.find_all_files()),
            'Duplicate Files Skipped': len(self.duplicate_files),
            'Unique Files Processed': len(self.processed_files),
            'Output CSV Files Created': len([f for f in output_files if f.endswith('.csv')]),
            'Output PDF Files Created': len([f for f in output_files if f.endswith('.pdf')]),
            'Total Output Files': len(output_files)
        }
        
        # Create summary report
        summary_file = self.output_dir / "processing_summary.txt"
        with open(summary_file, 'w') as f:
            f.write("FILE DEDUPLICATION AND NORMALIZATION SUMMARY\n")
            f.write("=" * 50 + "\n\n")
            
            for key, value in summary.items():
                f.write(f"{key}: {value}\n")
            
            if self.duplicate_files:
                f.write(f"\nDUPLICATE FILES FOUND ({len(self.duplicate_files)}):\n")
                f.write("-" * 30 + "\n")
                for dup_file in self.duplicate_files:
                    f.write(f"  {dup_file}\n")
            
            f.write(f"\nOUTPUT FILES CREATED ({len(output_files)}):\n")
            f.write("-" * 30 + "\n")
            for output_file in sorted(output_files):
                f.write(f"  {Path(output_file).name}\n")
        
        logger.info(f"Summary report created: {summary_file}")
        
        # Print summary to console
        print("\n" + "=" * 50)
        print("PROCESSING COMPLETE")
        print("=" * 50)
        for key, value in summary.items():
            print(f"{key}: {value}")
        
        if self.duplicate_files:
            print(f"\nFound {len(self.duplicate_files)} duplicate files (see summary.txt for details)")

    def test_filename_generation(self):
        """Test function to show how nested paths are handled"""
        test_cases = [
            "Grades/2024/Spring/ENGL-101/Section-A/midterm_grades.xlsx",
            "Archives/Old_System/2022/Fall/Semester_1/MATH/Advanced/final_results.csv", 
            "Downloads/Moodle_Exports/July_2024/Course_Exports/Business/Accounting/Grade_Export.xlsx",
            "Backup/Very/Deep/Nested/Structure/With/Many/Levels/grades.xlsx",
            "Grade.xlsx",  # Simple case
            "2023/Grade.xlsx"  # Minimal nesting
        ]
        
        print("\nTesting filename generation for nested structures:")
        print("=" * 60)
        
        for test_path in test_cases:
            filepath = self.input_dir / test_path
            result = self.generate_unique_filename(filepath)
            print(f"Input:  {test_path}")
            print(f"Output: {result}")
            print("-" * 40)

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Deduplicate and normalize grade files')
    parser.add_argument('input_dir', help='Directory containing messy grade files')
    parser.add_argument('output_dir', help='Directory for normalized output files')
    parser.add_argument('--test', action='store_true', help='Test filename generation')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate directories
    input_path = Path(args.input_dir)
    if not input_path.exists():
        print(f"Error: Input directory '{args.input_dir}' does not exist")
        return
    
    processor = FileDeduplicator(args.input_dir, args.output_dir)
    
    if args.test:
        processor.test_filename_generation()
        return
    
    print(f"Starting file deduplication and normalization...")
    print(f"Input directory: {args.input_dir}")
    print(f"Output directory: {args.output_dir}")
    
    processor.process_all_files()

if __name__ == "__main__":
    main()