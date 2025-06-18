#!/usr/bin/env python3
"""
Moodle Grade Data Processor
Automatically processes various grade file formats and extracts data for SIS import
"""

import pandas as pd
import os
import re
from pathlib import Path
import logging
from dataclasses import dataclass
import openpyxl  # noqa: F401
import pypdf
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class GradeRecord:
    student_id: str
    student_name: str
    course_code: str
    grade: float
    term: str
    file_source: str

class GradeProcessor:
    def __init__(self, input_directory: str, output_directory: str):
        self.input_dir = Path(input_directory)
        self.output_dir = Path(output_directory)
        self.output_dir.mkdir(exist_ok=True)
        
        # Common column patterns to look for
        self.grade_column_patterns = [
            r'total.*course',
            r'course.*total',
            r'final.*grade',
            r'grade.*final',
            r'overall.*grade',
            r'total.*points',
            r'percentage',
            r'final.*score'
        ]
        
        self.student_id_patterns = [
            r'student.*id',
            r'id.*number',
            r'user.*id',
            r'login.*id',
            r'username'
        ]
        
        self.student_name_patterns = [
            r'first.*name',
            r'last.*name',
            r'full.*name',
            r'student.*name',
            r'name',
            r'surname'
        ]
        
        self.processed_records: list[GradeRecord] = []
        
    def find_files(self) -> list[Path]:
        """Find all grade files in directory structure"""
        supported_extensions = ['.xlsx', '.csv', '.xls', '.pdf']
        files = []
        
        for root, dirs, filenames in os.walk(self.input_dir):
            for filename in filenames:
                if any(filename.lower().endswith(ext) for ext in supported_extensions):
                    files.append(Path(root) / filename)
        
        logger.info(f"Found {len(files)} files to process")
        return files
    
    def detect_column(self, df: pd.DataFrame, patterns: list[str]) -> str | None:
        """Detect column using regex patterns"""
        for col in df.columns:
            col_lower = str(col).lower()
            for pattern in patterns:
                if re.search(pattern, col_lower):
                    return col
        return None
    
    def extract_course_info(self, filepath: Path) -> dict[str, str]:
        """Extract course and term info from file path and name"""
        path_parts = filepath.parts
        filename = filepath.stem
        
        # Try to extract course code from filename or path
        course_patterns = [
            r'([A-Z]{2,4}[-_]?\d{2,4})',  # ENGL-101, MATH101, etc.
            r'([A-Z]{2,4}\s?\d{2,4})',    # ENGL 101, MATH 101
        ]
        
        term_patterns = [
            r'(spring|summer|fall|winter)\s?(\d{4})',
            r'(\d{4})\s?(spring|summer|fall|winter)',
            r'(semester\s?\d)',
            r'(term\s?\d)'
        ]
        
        course_code = "UNKNOWN"
        term = "UNKNOWN"
        
        # Check filename and path for course code
        search_text = f"{filename} {' '.join(path_parts)}"
        for pattern in course_patterns:
            match = re.search(pattern, search_text, re.IGNORECASE)
            if match:
                course_code = match.group(1).upper().replace('_', '-')
                break
        
        # Check for term
        for pattern in term_patterns:
            match = re.search(pattern, search_text, re.IGNORECASE)
            if match:
                term = match.group(0).upper()
                break
        
        return {"course_code": course_code, "term": term}
    
    def process_excel_file(self, filepath: Path) -> list[GradeRecord]:
        """Process Excel files (.xlsx, .xls)"""
        records: list[GradeRecord] = []
        
        try:
            # Try reading with different engines
            try:
                df = pd.read_excel(filepath, engine='openpyxl')
            except Exception as e_openpyxl:
                logger.debug(f"Failed to read Excel {filepath} with openpyxl: {e_openpyxl}")
                df = pd.read_excel(filepath, engine='xlrd')
            
            course_info = self.extract_course_info(filepath)
            records.extend(self.extract_grades_from_dataframe(df, filepath, course_info))
            
        except Exception as e:
            logger.error(f"Error processing Excel file {filepath}: {e}")
        
        return records
    
    def process_csv_file(self, filepath: Path) -> list[GradeRecord]:
        """Process CSV files"""
        records: list[GradeRecord] = []
        
        try:
            # Try different encodings and separators
            encodings = ['utf-8', 'latin-1', 'cp1252']
            separators = [',', ';', '\t']
            
            df = None
            for encoding in encodings:
                for sep in separators:
                    try:
                        df = pd.read_csv(filepath, encoding=encoding, sep=sep)
                        if len(df.columns) > 1:  # Successfully parsed
                            break
                    except Exception as e:
                        logger.debug(f"CSV parsing attempt with encoding {encoding} and separator '{sep}' failed for {filepath}: {e}")
                        continue
                if df is not None and len(df.columns) > 1:
                    break
            
            if df is None:
                logger.error(f"Could not parse CSV file: {filepath}")
                return records

            # Log the number of columns in the CSV file
            num_columns = len(df.columns)
            logger.info(f"CSV file {filepath} has {num_columns} columns.")
            
            course_info = self.extract_course_info(filepath)
            records.extend(self.extract_grades_from_dataframe(df, filepath, course_info))
            
        except Exception as e:
            logger.error(f"Error processing CSV file {filepath}: {e}")
        
        return records
    
    def process_pdf_file(self, filepath: Path) -> list[GradeRecord]:
        """Process PDF files (basic text extraction)"""
        records: list[GradeRecord] = []
        
        try:
            with open(filepath, 'rb') as file:
                pdf_reader = pypdf.PdfReader(file)
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text()
            
            if not text:
                logger.warning(f"Could not extract text from PDF: {filepath}")
                return []

            # Simplified extraction - assumes one student per PDF or very simple structure
            # This part needs significant improvement for real-world PDFs
            # Example: Look for student ID and grade patterns in the whole text
            
            student_id_match = re.search(r"Student ID: (\w+)", text, re.IGNORECASE)
            grade_match = re.search(r"Final Grade: (\d+\.?\d*)", text, re.IGNORECASE)
            student_name_match = re.search(r"Student Name: ([\w\s]+)", text, re.IGNORECASE)

            student_id = student_id_match.group(1) if student_id_match else ""
            grade_str = grade_match.group(1) if grade_match else ""
            student_name = student_name_match.group(1).strip() if student_name_match else ""
            
            course_info = self.extract_course_info(filepath)

            if student_id and grade_str:
                try:
                    grade = float(grade_str)
                    records.append(GradeRecord(
                        student_id=student_id,
                        student_name=student_name,
                        course_code=course_info.get("course_code", "UNKNOWN"),
                        grade=grade,
                        term=course_info.get("term", "UNKNOWN"),
                        file_source=str(filepath)
                    ))
                except ValueError:
                    logger.warning(f"Could not parse grade '{grade_str}' as float in PDF {filepath}")
        except Exception as e:
            logger.error(f"Error processing PDF {filepath}: {e}")
        
        return records

    def extract_grades_from_dataframe(self, df: pd.DataFrame, filepath: Path, course_info: dict) -> list[GradeRecord]:
        """Extract grade records from a pandas DataFrame"""
        records: list[GradeRecord] = []
        
        # Clean column names
        df.columns = df.columns.astype(str).str.strip()
        
        # Find relevant columns
        grade_col = self.detect_column(df, self.grade_column_patterns)
        student_id_col = self.detect_column(df, self.student_id_patterns)
        
        # For student names, try to find first/last name or full name
        first_name_col = self.detect_column(df, [r'first.*name'])
        last_name_col = self.detect_column(df, [r'last.*name', r'surname'])
        full_name_col = self.detect_column(df, [r'full.*name', r'^name$'])
        
        if not grade_col:
            logger.warning(f"No grade column found in {filepath}")
            # List available columns for manual review
            logger.info(f"Available columns: {list(df.columns)}")
            return records
        
        logger.info(f"Processing {filepath}: Grade column='{grade_col}', ID column='{student_id_col}'")
        
        for index, row in df.iterrows():
            try:
                # Get grade
                grade_value = row[grade_col]
                if pd.isna(grade_value):
                    continue
                
                # Try to convert to float
                if isinstance(grade_value, str):
                    # Remove % sign and other characters
                    grade_clean = re.sub(r'[^\d.-]', '', grade_value)
                    if not grade_clean:
                        continue
                    grade_float = float(grade_clean)
                else:
                    grade_float = float(grade_value)
                
                # Get student ID
                student_id = ""
                if student_id_col and not pd.isna(row[student_id_col]):
                    student_id = str(row[student_id_col])
                
                # Get student name
                student_name = ""
                if first_name_col and last_name_col:
                    first = row.get(first_name_col, "")
                    last = row.get(last_name_col, "")
                    student_name = f"{first} {last}".strip()
                elif full_name_col and not pd.isna(row[full_name_col]):
                    student_name = str(row[full_name_col])
                
                if student_name or student_id:  # Need at least one identifier
                    records.append(GradeRecord(
                        student_id=student_id,
                        student_name=student_name,
                        course_code=course_info["course_code"],
                        grade=grade_float,
                        term=course_info["term"],
                        file_source=str(filepath)
                    ))
                
            except (ValueError, TypeError) as e:
                logger.debug(f"Skipping row {index} in {filepath}: {e}")
                continue
        
        return records
    
    def process_all_files(self):
        """Process all files in the input directory"""
        files = self.find_files()
        
        for filepath in files:
            logger.info(f"Processing: {filepath}")
            
            if filepath.suffix.lower() in ['.xlsx', '.xls']:
                records = self.process_excel_file(filepath)
            elif filepath.suffix.lower() == '.csv':
                records = self.process_csv_file(filepath)
            elif filepath.suffix.lower() == '.pdf':
                records = self.process_pdf_file(filepath)
            else:
                continue
            
            self.processed_records.extend(records)
            logger.info(f"Extracted {len(records)} grade records from {filepath}")
    
    def generate_reports(self):
        """Generate summary reports and export data"""
        if not self.processed_records:
            logger.warning("No records processed")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame([
            {
                'Student_ID': r.student_id,
                'Student_Name': r.student_name,
                'Course_Code': r.course_code,
                'Grade': r.grade,
                'Term': r.term,
                'Source_File': r.file_source
            }
            for r in self.processed_records
        ])
        
        # Export main data
        output_file = self.output_dir / f"processed_grades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        df.to_excel(output_file, index=False)
        logger.info(f"Exported {len(df)} records to {output_file}")
        
        # Generate summary report
        summary = {
            'Total Records': len(df),
            'Unique Courses': df['Course_Code'].nunique(),
            'Unique Terms': df['Term'].nunique(),
            'Files Processed': df['Source_File'].nunique(),
            'Records Missing Student ID': len(df[df['Student_ID'] == '']),
            'Records Missing Student Name': len(df[df['Student_Name'] == '']),
        }
        
        summary_df = pd.DataFrame(list(summary.items()), columns=['Metric', 'Value'])
        summary_file = self.output_dir / f"processing_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        with pd.ExcelWriter(summary_file) as writer:
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Course breakdown
            course_summary = df.groupby(['Course_Code', 'Term']).agg({
                'Grade': ['count', 'mean', 'min', 'max'],
                'Source_File': 'nunique'
            }).round(2)
            course_summary.to_excel(writer, sheet_name='Course_Breakdown')
            
            # Files processed
            file_summary = df.groupby('Source_File').agg({
                'Grade': 'count',
                'Course_Code': 'first',
                'Term': 'first'
            })
            file_summary.to_excel(writer, sheet_name='Files_Processed')
        
        logger.info(f"Generated summary report: {summary_file}")
        
        # Export for SIS import (clean format)
        sis_df = df[df['Student_ID'] != ''].copy()  # Only records with student IDs
        sis_file = self.output_dir / f"sis_import_ready_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        sis_df.to_csv(sis_file, index=False)
        logger.info(f"SIS-ready file: {sis_file} ({len(sis_df)} records)")

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process Moodle grade files for SIS import')
    parser.add_argument('input_dir', help='Directory containing grade files')
    parser.add_argument('output_dir', help='Directory for processed output')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    processor = GradeProcessor(args.input_dir, args.output_dir)
    processor.process_all_files()
    processor.generate_reports()
    
    print("\nProcessing complete!")
    print(f"Total records processed: {len(processor.processed_records)}")
    print(f"Output saved to: {args.output_dir}")

if __name__ == "__main__":
    main()