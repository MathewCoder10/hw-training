import os
import csv
import json
import sys
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s: %(message)s')

def warn_unwanted_characters(text, context, row_num=None, header=None):
    """
    Checks if the provided text contains:
      - HTML tags.
      - Tabs or newlines (including their escape sequences).
      - Unwanted extra whitespace (leading, trailing, excessive spaces between words).
    Logs a warning message if any unwanted patterns are found.
    """
    warnings = []
    
    # Detect HTML tags
    if re.search(r"<[^>]+>", text):
        warnings.append("HTML tags")
    
    # Detect actual tab or newline characters
    if re.search(r"[\t\n\r]", text):
        warnings.append("tabs/newlines")
    
    # Detect excessive spaces (more than one space between words)
    if re.search(r"\s{2,}", text):
        warnings.append("excessive whitespace (multiple spaces)")

    if warnings:
        location = []
        if row_num is not None:
            location.append(f"Row {row_num}")
        if header:
            location.append(f"Column/Key '{header}'")
        loc_str = ", ".join(location)
        logging.warning("In %s, %s contains %s.", context, loc_str, ', '.join(warnings))

def validate_header_allowed(header):
    """
    Validates that each header field contains only alphanumeric characters and underscores.
    Exits if a header field fails the allowed characters test.
    """
    allowed_pattern = re.compile(r'^[A-Za-z0-9_]+$')
    for index, col in enumerate(header, start=1):
        if not allowed_pattern.match(col):
            logging.error("Header error (Row 1, Column %d - '%s'): contains whitespace or special characters.", index, col)
            sys.exit(1)
        # Warn if header has unwanted characters
        warn_unwanted_characters(col, "header (Row 1)", header=col)

def infer_type_csv(value):
    """
    Infers the data type of a CSV cell.
    Returns 'int' if the value represents an integer,
    'float' if it represents a float (but not an int), otherwise 'str'.
    """
    try:
        int_val = int(value)
        if float(value) == int_val:
            return 'int'
    except ValueError:
        pass
    try:
        float(value)
        return 'float'
    except ValueError:
        return 'str'

def warn_datatypes_csv(header, rows):
    """
    Checks each column in CSV rows and logs a warning for each header with the detected datatypes.
    """
    for col_index, col_name in enumerate(header):
        types_found = {infer_type_csv(row[col_index]) for row in rows if col_index < len(row)}
        logging.warning("In CSV data (Column/Key '%s'): detected datatypes: %s", col_name, types_found)

def warn_empty_data_csv(header, rows):
    """
    Checks each column in CSV rows if all values are empty.
    Logs a warning for each header that contains only empty data.
    """
    for col_index, col_name in enumerate(header):
        if all(row[col_index].strip() == "" for row in rows if col_index < len(row)):
            logging.warning("In CSV data (Column/Key '%s'): is completely empty.", col_name)

def warn_datatypes_json(data, file_type):
    """
    For JSON/JSONL files where data is a list of dict objects, logs a warning for each key with the detected datatypes.
    """
    if isinstance(data, list) and data and isinstance(data[0], dict):
        header = list(data[0].keys())
        for key in header:
            types_found = {type(obj.get(key)).__name__ for obj in data if isinstance(obj, dict)}
            logging.warning("In %s data (Column/Key '%s'): detected datatypes: %s", file_type.upper(), key, types_found)
    elif isinstance(data, dict):
        # When data is a single dict, report datatype for each key.
        for key, value in data.items():
            logging.warning("In %s data (Column/Key '%s'): detected datatype: %s", file_type.upper(), key, type(value).__name__)

def warn_empty_data_json(data, file_type):
    """
    For JSON/JSONL files, checks if all string values for a key are empty.
    Logs a warning for each key that is completely empty.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, str) and value.strip() == "":
                logging.warning("In %s data (Column/Key '%s'): is empty.", file_type.upper(), key)
    elif isinstance(data, list) and data and isinstance(data[0], dict):
        header = list(data[0].keys())
        for key in header:
            if all(isinstance(obj.get(key), str) and obj.get(key).strip() == "" for obj in data if key in obj):
                logging.warning("In %s data (Column/Key '%s'): is completely empty.", file_type.upper(), key)

def check_csv_file(file_path, delimiter):
    """
    Checks CSV file for:
      - Unwanted double quotes in raw lines.
      - CSV structure and allowed header fields.
      - Unwanted characters in headers and cells.
      - Displays datatypes for each column and empty columns.
    """
    all_rows = []

    try:
        with open(file_path, 'r', newline='') as f:
            raw_lines = f.readlines()

        if not raw_lines:
            logging.error("CSV file error: File is empty. (No rows found)")
            sys.exit(1)

        # Check raw header for double quotes.
        if '"' in raw_lines[0]:
            logging.warning("CSV header raw line (Row 1) contains a double quote.")

        # Check raw data lines for double quotes (only first occurrence).
        for i, line in enumerate(raw_lines[1:], start=2):
            if '"' in line:
                logging.warning("CSV data (Row %d) contains a double quote.", i)
                break

        # Validate CSV structure using csv.reader.
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=delimiter)
            header = next(reader, None)
            if header is None:
                logging.error("CSV file error: File is empty. (No header found)")
                sys.exit(1)

            validate_header_allowed(header)
            for col in header:
                warn_unwanted_characters(col, "CSV header (Row 1)", header=col)

            expected_field_count = len(header)
            for row_num, row in enumerate(reader, start=2):
                if len(row) != expected_field_count:
                    logging.error("CSV structure error (Row %d): Found %d fields; expected %d. Header: %s", row_num, len(row), expected_field_count, header)
                    sys.exit(1)
                for col_index, cell in enumerate(row):
                    warn_unwanted_characters(cell, "CSV data", row_num=row_num, header=header[col_index])
                all_rows.append(row)

        # Log datatypes for every column
        warn_datatypes_csv(header, all_rows)
        warn_empty_data_csv(header, all_rows)
        return True

    except Exception as e:
        logging.error("CSV format error: %s. (File: %s)", e, file_path)
        sys.exit(1)

def validate_json_data(data, file_type):
    """
    Checks JSON (or JSONL) data for unwanted characters in header keys and string values.
    """
    header_warning_given = False

    def check_obj(obj, context, obj_index=None):
        nonlocal header_warning_given
        if isinstance(obj, dict):
            for key, value in obj.items():
                if not header_warning_given:
                    warn_unwanted_characters(key, f"{file_type.upper()} header (Row 1)", header=key)
                    header_warning_given = True
                if isinstance(value, str):
                    warn_unwanted_characters(value, f"{file_type.upper()} data", row_num=obj_index, header=key)

    if isinstance(data, dict):
        for key in data.keys():
            warn_unwanted_characters(key, f"{file_type.upper()} header (Row 1)", header=key)
        check_obj(data, "top-level object")
    elif isinstance(data, list):
        if data and isinstance(data[0], dict):
            for key in data[0].keys():
                warn_unwanted_characters(key, f"{file_type.upper()} header (Row 1)", header=key)
            for index, obj in enumerate(data, start=1):
                check_obj(obj, f"object at index {index}", obj_index=index)

def check_json_file(file_path, file_type):
    """
    Loads a JSON file, validates headers, checks unwanted characters in keys and string values,
    and displays datatypes and empty keys.
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        if isinstance(data, dict):
            validate_header_allowed(list(data.keys()))
        elif isinstance(data, list) and data and isinstance(data[0], dict):
            validate_header_allowed(list(data[0].keys()))
        validate_json_data(data, file_type)
        # Log detected datatypes for every key
        warn_datatypes_json(data, file_type)
        warn_empty_data_json(data, file_type)
        return True

    except Exception as e:
        logging.error("%s format error: %s. (File: %s)", file_type.upper(), e, file_path)
        sys.exit(1)

def check_jsonl_file(file_path):
    """
    Reads a JSONL file line by line.
    Prompts the user to skip empty lines and validates each JSON object.
    Also checks headers, displays datatypes, and warns for empty keys.
    """
    header_warning_given = False
    jsonl_data = []

    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, start=1):
                line = line.rstrip("\n")
                if not line.strip():
                    user_input = input(f"Empty line encountered at line {line_num}. Do you want to skip it? [Y/N]: ").strip().lower()
                    if user_input != 'y':
                        logging.error("JSONL format error: Empty line at Row %d not allowed.", line_num)
                        sys.exit(1)
                    continue

                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as jde:
                    logging.error("JSONL format error (Row %d): %s", line_num, jde)
                    sys.exit(1)

                if isinstance(obj, dict) and not header_warning_given:
                    validate_header_allowed(list(obj.keys()))
                    for key in obj.keys():
                        warn_unwanted_characters(key, "JSONL header (Row 1)", header=key)
                    header_warning_given = True

                if isinstance(obj, dict):
                    for key, value in obj.items():
                        if isinstance(value, str):
                            warn_unwanted_characters(value, "JSONL data", row_num=line_num, header=key)
                jsonl_data.append(obj)

        # Log detected datatypes for every key in JSONL data
        warn_datatypes_json(jsonl_data, 'jsonl')
        warn_empty_data_json(jsonl_data, 'jsonl')
        return True

    except Exception as e:
        logging.error("JSONL format error: %s. (File: %s)", e, file_path)
        sys.exit(1)

def check_file_format(file_path, delimiter=None):
    """
    Determines the file type (CSV, JSON, or JSONL) based on extension and checks the format accordingly.
    """
    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.csv':
        if delimiter is None:
            logging.error("CSV file error: Delimiter required for CSV file. (File: %s)", file_path)
            sys.exit(1)
        return check_csv_file(file_path, delimiter)
    elif ext == '.json':
        return check_json_file(file_path, 'json')
    elif ext == '.jsonl':
        return check_jsonl_file(file_path)
    else:
        logging.error("File error: Unsupported file type. (File: %s)", file_path)
        sys.exit(1)

def main():
    file_path = input("Enter the file path: ").strip()
    if not os.path.isfile(file_path):
        logging.error("File error: File not found. (Path: %s)", file_path)
        sys.exit(1)

    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.csv':
        delimiter = input("Enter the CSV delimiter (e.g., ',' or '|'): ").strip()
        if check_file_format(file_path, delimiter):
            logging.info("CSV file format is correct. (File: %s)", file_path)
    elif ext in ['.json', '.jsonl']:
        if check_file_format(file_path):
            logging.info("%s file format is correct. (File: %s)", ext.upper(), file_path)
    else:
        logging.error("File error: Unsupported file type. (File: %s)", file_path)
        sys.exit(1)

if __name__ == "__main__":
    main()
