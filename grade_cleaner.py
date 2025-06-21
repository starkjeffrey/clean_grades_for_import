import os
import pandas as pd
import re
from dateutil.parser import parse
import shutil
import csv


def get_year_month_from_filename(filename):
    """
    Extracts the Year and Month from the filename.
    Returns a string in the format YYYY-MM.
    """
    # Regular expression to find dates in various formats
    # e.g., Oct_17_2022, November_2022, 03_April_2024
    match = re.search(r'(\d{1,2}[_\s-][A-Za-z]+[_\s-]\d{4})|([A-Za-z]+[_\s-]\d{4})', filename)
    if match:
        try:
            date_str = match.group(0).replace('_', ' ')
            # Parse the date string. If no day is present, it defaults to the 1st.
            dt = parse(date_str)
            return dt.strftime('%Y-%m')
        except ValueError:
            return None
    return None

def triage_interim_files(interim_path):
    """
    Sorts files from the interim directory into 'consolidated' and 'non-consolidated' subfolders.
    """
    consolidated_path = os.path.join(interim_path, 'consolidated')
    non_consolidated_path = os.path.join(interim_path, 'non-consolidated')
    os.makedirs(consolidated_path, exist_ok=True)
    os.makedirs(non_consolidated_path, exist_ok=True)

    print(f"\nTriaging files in {interim_path}...")
    for filename in os.listdir(interim_path):
        file_path = os.path.join(interim_path, filename)
        if os.path.isfile(file_path) and filename.endswith('.csv'):
            try:
                # Use pandas to robustly read the header and count columns
                df = pd.read_csv(file_path, nrows=0, encoding='utf-8', encoding_errors='ignore')
                num_columns = len(df.columns)

                if num_columns <= 10:
                    print(f"  -> Moving {filename} to consolidated.")
                    shutil.move(file_path, os.path.join(consolidated_path, filename))
                elif num_columns >= 20:
                    print(f"  -> Moving {filename} to non-consolidated.")
                    shutil.move(file_path, os.path.join(non_consolidated_path, filename))
                else:
                    print(f"  -> Skipping {filename} (has {num_columns} columns).")
            except Exception as e:
                print(f"    Could not process {filename}: {e}")

def find_data_start_and_id_column(file_path):
    """
    Scans a CSV file to find the first row of actual data and the column index of the student ID.
    It assumes the student ID is a 3-6 digit number and looks for a column where this pattern
    appears consistently.
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            lines = list(reader)
            if not lines:
                return None, None

            max_cols = max(len(r) for r in lines) if lines else 0

            for col_idx in range(max_cols):
                consecutive_matches = 0
                first_match_row = None

                for row_idx in range(len(lines)):
                    if col_idx < len(lines[row_idx]):
                        cell = lines[row_idx][col_idx].strip()
                        # Look for a 3-6 digit number as a likely student ID
                        if re.fullmatch(r'\d{3,6}', cell):
                            if consecutive_matches == 0:
                                first_match_row = row_idx
                            consecutive_matches += 1
                            if consecutive_matches >= 2:  # Found two consecutive rows with a valid ID
                                return first_match_row, col_idx
                        else:
                            consecutive_matches = 0
                            first_match_row = None
    except (FileNotFoundError, IndexError):
        return None, None
    return None, None

def find_matching_column_index(component_name, header_columns):
    """Finds the best matching column index for a given class component using heuristic rules."""
    component_name = component_name.upper()
    header_columns_upper = [str(h).upper().strip() for h in header_columns]

    # Heuristic rules to map component names to header names
    rules = {
        ('V-', 'VEN'): ['VENTURE', '3 DAY'],
        ('PRO-',): ['PROJECT', '2 DAY'],
        ('IEAP-2-WR', 'WR', 'W-'): ['WRITING'],
        ('INTER-2', 'G-'): ['GRAMMAR'],
        ('RE-', 'R-'): ['READING'],
        ('COMP-',): ['COMPUTER', 'COMP'],
        ('EW-',): ['ESSAY', 'WRITING'],
        ('BP-',): ['BP'],
        ('FC-',): ['FC', 'FOUR CORNERS'],
        ('EC-',): ['EC', 'ENGL IN COMMON'],
        ('HCOMP-',): ['COMPOSITION'],
    }

    # First, try for a direct match
    for i, header in enumerate(header_columns_upper):
        if component_name == header:
            return i

    # Second, apply heuristic rules
    for component_keys, header_keys in rules.items():
        if any(component_name.startswith(key) for key in component_keys):
            for h_key in header_keys:
                for i, header in enumerate(header_columns_upper):
                    if h_key in header:
                        return i
    return None


def process_consolidated_file(file_path, termid, legacy_df):
    """Processes a consolidated grade file, matching each class part to its specific grade column."""
    print(f"--- Processing consolidated file: {os.path.basename(file_path)} ---")
    start_row, id_col_idx = find_data_start_and_id_column(file_path)

    if start_row is None or id_col_idx is None:
        print(f"    SKIPPING: Could not determine data start row or student ID column.")
        return

    header_row_index = start_row - 1
    print(f"    Data found starting on row {start_row}, using column {id_col_idx} for student ID.")

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            all_rows = list(reader)
            header_columns = all_rows[header_row_index]

        # Skip files with ambiguous '2 day' and '3 day' headers pending clarification
        headers_upper = [h.upper().strip() for h in header_columns]
        if '2 DAY' in headers_upper and '3 DAY' in headers_upper:
            print(f"    SKIPPING: File contains ambiguous '2 day' and '3 day' headers. Needs manual review.")
            return

        stop_words = ['total', 'grade', 'unnamed', 'id', 'name', 'surname', 'first', 'last']
        grade_columns_with_indices = [
            (i, h) for i, h in enumerate(header_columns)
            if i != id_col_idx and not any(sw in h.lower() for sw in stop_words) and h.strip() != ''
        ]
        grade_column_headers = [h for _, h in grade_columns_with_indices]
        print(f"    Identified potential grade columns: {grade_column_headers}")

        df = pd.read_csv(file_path, header=header_row_index, encoding='utf-8', on_bad_lines='skip')
        df.columns = [str(c).strip() for c in df.columns]
        student_id_col_name = df.columns[id_col_idx]

        df = df[pd.to_numeric(df[student_id_col_name], errors='coerce').notna()].copy()
        df[student_id_col_name] = df[student_id_col_name].astype(str).str.strip()

    except Exception as e:
        print(f"    Error reading or processing file with pandas: {e}")
        return

    print("    -- Generating SQL UPDATE statements --")
    for _, row in df.iterrows():
        student_id = row.get(student_id_col_name)
        if not student_id:
            continue

        student_legacy_data = legacy_df[(legacy_df['student_id'] == student_id) & (legacy_df['termid'] == termid)]

        if student_legacy_data.empty:
            continue

        for _, legacy_row in student_legacy_data.iterrows():
            classid = legacy_row['classid']
            try:
                component_name = classid.split('!$')[-1]
            except IndexError:
                continue

            matched_header_idx = find_matching_column_index(component_name, grade_column_headers)

            if matched_header_idx is not None:
                original_grade_col_idx = grade_columns_with_indices[matched_header_idx][0]
                grade_col_header = df.columns[original_grade_col_idx]
                grade = row.get(grade_col_header)

                if pd.notna(grade) and str(grade).strip() != '':
                    try:
                        numeric_grade = float(str(grade).strip())
                        print(f"UPDATE grades_table SET grade = {numeric_grade:.3f} WHERE student_id = '{student_id}' AND classid = '{classid}';")
                    except (ValueError, TypeError):
                        pass


def main():
    """Main function to process grade files."""
    legacy_grades_path = 'data/all_ifl_to_update.csv'
    interim_path = 'data/interim/'

    # triage_interim_files(interim_path)

    print(f"Loading legacy grades from {legacy_grades_path}")
    legacy_df = pd.read_csv(legacy_grades_path)
    legacy_df['student_id'] = legacy_df['student_id'].astype(str).str.strip()
    legacy_df['term_startdate'] = pd.to_datetime(legacy_df['term_startdate'])

    consolidated_path = os.path.join(interim_path, 'consolidated')
    print(f"\nProcessing consolidated files in {consolidated_path}...")

    for filename in sorted(os.listdir(consolidated_path)):
        file_path = os.path.join(consolidated_path, filename)
        if not (os.path.isfile(file_path) and filename.endswith('.csv')):
            continue

        year_month = get_year_month_from_filename(filename)
        if not year_month:
            print(f"SKIPPING {filename}: Could not determine Year-Month from filename.")
            print("-" * 50)
            continue

        # Find all legacy records that match the year and month
        matching_term_df = legacy_df[legacy_df['term_startdate'].dt.strftime('%Y-%m') == year_month]
        if matching_term_df.empty:
            print(f"SKIPPING {filename}: No matching term found in legacy data for {year_month}.")
            print("-" * 50)
            continue

        # Process the file for each unique termid found for that month
        for termid in matching_term_df['termid'].unique():
            term_specific_legacy_df = matching_term_df[matching_term_df['termid'] == termid]
            process_consolidated_file(file_path, termid, term_specific_legacy_df)
        
        print("-" * 50)


if __name__ == '__main__':
    main()
