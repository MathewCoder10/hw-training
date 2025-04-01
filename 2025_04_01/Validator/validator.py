import os
import csv
import json
import sys
import re

def validate_header_allowed(header):
    """
    Validates that each header field (as obtained after parsing) contains only alphanumeric characters and underscores.
    Exits if a header field fails this test.
    """
    pattern = re.compile(r'^[A-Za-z0-9_]+$')
    for index, col in enumerate(header, start=1):
        if not pattern.match(col):
            print(f"Header error: Column {index} ('{col}') contains whitespace or special characters.")
            sys.exit(1)

def infer_type_csv(value):
    """
    Attempts to infer the data type of a CSV cell.
    Returns 'int' if the value represents an integer,
    'float' if it represents a float (but not an int),
    otherwise 'str'.
    """
    try:
        int_val = int(value)
        # If value when converted to int equals value when converted to float,
        # consider it as int.
        if float(value) == int_val:
            return 'int'
    except ValueError:
        pass
    try:
        float(value)
        return 'float'
    except ValueError:
        return 'str'

def warn_multiple_types_csv(header, rows):
    """
    Checks each column in CSV rows for more than one inferred type.
    Issues a warning for the first header that shows mixed types.
    """
    for col_index, col_name in enumerate(header):
        types_found = set()
        for row in rows:
            if col_index < len(row):
                types_found.add(infer_type_csv(row[col_index]))
        if len(types_found) > 1:
            print(f"Warning: In CSV data, column '{col_name}' has mixed types: {types_found}")
            # Warn only for the first occurrence
            return

def warn_multiple_types_json(data, file_type):
    """
    For JSON/JSONL files where data is a list of dict objects, for each header (key)
    checks if the encountered data types (using type(value).__name__) are mixed.
    Issues a warning for the first header that shows mixed types.
    """
    if isinstance(data, list) and data:
        # Get all keys from first dict as header
        header = list(data[0].keys())
        for key in header:
            types_found = set()
            for obj in data:
                if isinstance(obj, dict) and key in obj:
                    types_found.add(type(obj[key]).__name__)
            if len(types_found) > 1:
                print(f"Warning: In {file_type.upper()} data, key '{key}' has mixed types: {types_found}")
                return

def check_csv_file(file_path, delimiter):
    """
    Checks CSV file in several steps:
      1. Reads the raw text lines and warns if any line contains a double quote (") anywhere.
      2. Uses csv.reader to validate that the CSV structure is correct and that the header fields pass the allowed-characters test.
      3. After reading all rows, checks for mixed data types in each column.
    """
    header_warning_given = False
    data_warning_given = False
    all_rows = []

    try:
        with open(file_path, 'r', newline='') as f:
            raw_lines = f.readlines()

        if not raw_lines:
            print("CSV file is empty.")
            sys.exit(1)

        # Check raw header line for double quotes.
        if '"' in raw_lines[0]:
            print("Warning: CSV header raw line contains a double quote.")
            header_warning_given = True

        # Check raw data lines for double quotes (only first occurrence).
        for i, line in enumerate(raw_lines[1:], start=2):
            if '"' in line:
                print(f"Warning: CSV data on line {i} contains a double quote.")
                data_warning_given = True
                break

        # Validate CSV structure using csv.reader.
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=delimiter)
            header = next(reader, None)
            if header is None:
                print("CSV file is empty.")
                sys.exit(1)

            # Validate header fields for allowed characters.
            validate_header_allowed(header)
            expected_field_count = len(header)
            for row_num, row in enumerate(reader, start=2):
                if len(row) != expected_field_count:
                    print(f"Row {row_num} has {len(row)} fields; expected {expected_field_count}.")
                    sys.exit(1)
                all_rows.append(row)

        # Warn if any column has mixed data types.
        warn_multiple_types_csv(header, all_rows)
        return True
    except Exception as e:
        print(f"CSV format error: {e}")
        sys.exit(1)

def check_json_file(file_path, file_type):
    """
    Loads a JSON file, validates header allowed characters (if applicable),
    checks for unwanted double quotes in keys/values,
    and warns if a header has mixed data types (if the JSON is a list of objects).
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        # Validate header allowed characters if applicable.
        if isinstance(data, dict):
            header = list(data.keys())
            validate_header_allowed(header)
        elif isinstance(data, list) and data and isinstance(data[0], dict):
            header = list(data[0].keys())
            validate_header_allowed(header)
        # Check for embedded double quotes in keys or values.
        validate_json_data(data, file_type)
        # Warn about mixed types if applicable.
        warn_multiple_types_json(data, file_type)
        return True
    except Exception as e:
        print(f"{file_type.upper()} format error: {e}")
        sys.exit(1)

def check_jsonl_file(file_path):
    """
    Reads a JSONL file line by line.
    If an empty line is encountered, asks the user whether to skip it.
    If the user answers 'N', the code prints an error message and stops.
    Also validates header keys (from the first object) and data values for unwanted double quotes.
    Finally, if the file contains multiple JSON objects, warns if any header has mixed data types.
    """
    header_warning_given = False
    data_warning_given = False
    jsonl_data = []

    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, start=1):
                line = line.rstrip("\n")
                if not line.strip():
                    # Ask user whether to skip the empty line
                    user_input = input(f"Empty line encountered at line {line_num}. Do you want to skip it? [Y/N]: ").strip().lower()
                    if user_input != 'y':
                        print(f"Error: Empty line at line {line_num} not allowed.")
                        sys.exit(1)
                    else:
                        continue

                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as jde:
                    print(f"JSONL format error on line {line_num}: {jde}")
                    sys.exit(1)

                if isinstance(obj, dict):
                    # For the first encountered object, validate header.
                    if not header_warning_given:
                        header = list(obj.keys())
                        validate_header_allowed(header)
                        for key in header:
                            if '"' in key:
                                print(f"Warning: In JSONL header, key '{key}' contains a double quote.")
                                header_warning_given = True
                                break
                    # Check data values.
                    for key, value in obj.items():
                        if isinstance(value, str) and '"' in value and not data_warning_given:
                            print(f"Warning: In JSONL data on line {line_num}, key '{key}' with value '{value}' contains a double quote.")
                            data_warning_given = True
                            break
                jsonl_data.append(obj)
        # Warn about mixed data types in JSONL if applicable.
        warn_multiple_types_json(jsonl_data, 'jsonl')
        return True
    except Exception as e:
        print(f"JSONL format error: {e}")
        sys.exit(1)

def validate_json_data(data, file_type):
    """
    For JSON (or JSONL) files, check for unwanted double quote characters
    in header keys (if applicable) and in string values.
    A warning is printed only on the first occurrence.
    """
    header_warning_given = False
    data_warning_given = False

    def check_obj(obj, context):
        nonlocal data_warning_given, header_warning_given
        # If the object is a dict, check its keys and string values.
        if isinstance(obj, dict):
            for key, value in obj.items():
                if not header_warning_given and '"' in key:
                    print(f"Warning: In {file_type.upper()} header, key '{key}' contains a double quote.")
                    header_warning_given = True
                if isinstance(value, str) and '"' in value and not data_warning_given:
                    print(f"Warning: In {file_type.upper()} data, key '{key}' with value '{value}' contains a double quote.")
                    data_warning_given = True
                    return

    if isinstance(data, dict):
        # Check header keys.
        for key in data.keys():
            if '"' in key:
                print(f"Warning: In {file_type.upper()} header, key '{key}' contains a double quote.")
                header_warning_given = True
                break
        check_obj(data, "top-level object")
    elif isinstance(data, list):
        if data and isinstance(data[0], dict):
            for key in data[0].keys():
                if '"' in key:
                    print(f"Warning: In {file_type.upper()} header, key '{key}' contains a double quote.")
                    header_warning_given = True
                    break
        for index, obj in enumerate(data, start=1):
            if isinstance(obj, dict):
                check_obj(obj, f"object at index {index}")
                if data_warning_given:
                    break

def check_file_format(file_path, delimiter=None):
    ext = os.path.splitext(file_path)[1].lower()

    if ext == '.csv':
        if delimiter is None:
            print("Delimiter required for CSV file.")
            sys.exit(1)
        return check_csv_file(file_path, delimiter)
    elif ext == '.json':
        return check_json_file(file_path, 'json')
    elif ext == '.jsonl':
        return check_jsonl_file(file_path)
    else:
        print("Error: File is not CSV, JSON, or JSONL.")
        sys.exit(1)

def main():
    file_path = input("Enter the file path: ").strip()
    
    if not os.path.isfile(file_path):
        print("Error: File not found.")
        sys.exit(1)
    
    ext = os.path.splitext(file_path)[1].lower()
    
    if ext == '.csv':
        delimiter = input("Enter the CSV delimiter (e.g., ',' or ';'): ").strip()
        if check_file_format(file_path, delimiter):
            print("CSV file format is correct.")
    elif ext in ['.json', '.jsonl']:
        if check_file_format(file_path):
            print(f"{ext.upper()} file format is correct.")
    else:
        print("Error: Unsupported file type.")
        sys.exit(1)

if __name__ == "__main__":
    main()
