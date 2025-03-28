import json
import csv
import re
import os

def validate_unwanted_chars(content, allowed_pattern=r"^[\x20-\x7E\n\r\t]+$"):
    """
    Checks if content only contains allowed printable ASCII characters (including whitespace).
    Returns True if valid; otherwise, prints a warning and returns False.
    """
    if not re.match(allowed_pattern, content):
        print("Warning: Unwanted or non-printable characters found.")
        return False
    return True

def validate_json(file_path):
    """
    Validates JSON file by checking for unwanted characters and then trying to parse it.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        # Check for unwanted characters
        validate_unwanted_chars(content)
        
        # Try to load as JSON
        data = json.loads(content)
        print("JSON format is valid.")
        return True, data
    except json.JSONDecodeError as e:
        print(f"JSON format error: {e}")
        return False, None
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return False, None

def parse_field_mapping(mapping_str):
    """
    Parses a string in the format: {name:string,price:float}
    Returns a dictionary mapping header names to Python types.
    Supported types: string, float, int.
    """
    mapping_str = mapping_str.strip()
    if mapping_str.startswith("{") and mapping_str.endswith("}"):
        mapping_str = mapping_str[1:-1]  # remove {}
    else:
        raise ValueError("Mapping must be provided in dictionary format, e.g., {name:string,price:float}")

    mapping = {}
    for pair in mapping_str.split(","):
        if ':' not in pair:
            continue
        key, val = pair.split(":", 1)
        key = key.strip()
        val = val.strip().lower()
        if val == "string":
            mapping[key] = str
        elif val == "float":
            mapping[key] = float
        elif val == "int":
            mapping[key] = int
        else:
            raise ValueError(f"Unsupported data type provided for field '{key}': {val}")
    return mapping

def validate_csv(file_path, delimiter, field_mapping):
    """
    Validates CSV file by:
      - Checking for unwanted characters.
      - Verifying the CSV is read with the correct delimiter.
      - Checking that the header matches the keys provided in field_mapping.
      - Validating that each field in subsequent rows can be converted to the expected data type.
    
    field_mapping: dict mapping header name to Python type.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        # Check unwanted characters in entire file content
        validate_unwanted_chars(content, allowed_pattern=rf"^[\x20-\x7E\n\r\t{re.escape(delimiter)}]+$")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=delimiter)
            rows = list(reader)
        
        if not rows:
            print("CSV file is empty.")
            return False, None
        
        header = rows[0]
        expected_fields = list(field_mapping.keys())
        if header != expected_fields:
            print(f"CSV header mismatch. Expected {expected_fields} but got {header}")
            return False, None
        
        # Validate each row's data types
        for row_index, row in enumerate(rows[1:], start=2):
            if len(row) != len(expected_fields):
                print(f"Row {row_index} has an incorrect number of fields. Expected {len(expected_fields)}, got {len(row)}")
                return False, None
            for field, expected_type in field_mapping.items():
                idx = expected_fields.index(field)
                value = row[idx].strip()
                try:
                    # For string, no conversion is needed; for numeric types, try to convert.
                    if expected_type != str:
                        expected_type(value)
                except ValueError:
                    print(f"Row {row_index} field '{field}' with value '{value}' is not of type {expected_type.__name__}.")
                    return False, None
        
        print("CSV format is valid.")
        return True, rows
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        return False, None

def main():
    file_path = input("Enter the path of the file to validate: ").strip()
    
    if not os.path.isfile(file_path):
        print("File does not exist. Please check the path and try again.")
        return
    
    ext = os.path.splitext(file_path)[1].lower()
    
    # JSON validation: no further input required
    if ext == ".json":
        valid, data = validate_json(file_path)
        if valid:
            print("JSON file validated successfully.")
        else:
            print("JSON file validation failed.")
    
    # CSV validation: ask for delimiter and field mapping
    elif ext == ".csv":
        delimiter = input("Enter the CSV delimiter (e.g., , or ;): ").strip()
        mapping_input = input("Enter field mapping as dictionary (e.g., {name:string,price:float}): ").strip()
        try:
            field_mapping = parse_field_mapping(mapping_input)
        except ValueError as e:
            print(f"Error parsing field mapping: {e}")
            return
        
        valid, rows = validate_csv(file_path, delimiter, field_mapping)
        if valid:
            print("CSV file validated successfully.")
        else:
            print("CSV file validation failed.")
    else:
        print("Unsupported file type. Only .json and .csv files are supported.")

if __name__ == "__main__":
    main()
