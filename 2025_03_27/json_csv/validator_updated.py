import json
import csv
import re
import os
import ast

# Global list to accumulate validation report messages
validation_report = []

def log(message):
    """Helper to print and log messages."""
    print(message)
    validation_report.append(message)

def validate_unwanted_chars(content, allowed_pattern=r"^[\x20-\x7E\n\r\t]+$"):
    """
    Checks if content only contains allowed printable ASCII characters (including whitespace).
    If not, logs a warning along with the specific unwanted characters found.
    Returns True if valid; otherwise, returns False.
    """
    if not re.match(allowed_pattern, content):
        # Try to extract allowed characters from the allowed_pattern.
        m = re.search(r"\[(.*?)\]", allowed_pattern)
        allowed_chars = m.group(1) if m else ""
        unwanted = set()
        for char in content:
            # Create a temporary pattern to test each character.
            if not re.match(rf"[{allowed_chars}]", char):
                unwanted.add(repr(char))
        log("Warning: Unwanted or non-printable characters found.")
        log("Unwanted characters detected: " + ", ".join(sorted(unwanted)))
        return False
    return True

def check_empty_quotes_in_json(data):
    """
    Recursively checks JSON data to ensure that string values are not quoted with content.
    An empty string, '' or "" is considered a valid null value.
    If a string is enclosed in quotes and contains any content, it is considered invalid.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, str):
                if value in ["", "''", '""']:
                    continue  # valid null value
                if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
                    if value not in ["", "''", '""']:
                        log(f"JSON validation error: Field '{key}' contains a quoted value with content: {value}")
                        return False
            if not check_empty_quotes_in_json(value):
                return False
    elif isinstance(data, list):
        for index, item in enumerate(data):
            if isinstance(item, str):
                if item in ["", "''", '""']:
                    continue  # valid null value
                if (item.startswith("'") and item.endswith("'")) or (item.startswith('"') and item.endswith('"')):
                    if item not in ["", "''", '""']:
                        log(f"JSON validation error: Item at index {index} contains a quoted value with content: {item}")
                        return False
            if not check_empty_quotes_in_json(item):
                return False
    return True

def validate_json(file_path):
    """
    Validates a JSON file by checking for unwanted characters and trying to parse it.
    Also checks that no field contains a quoted value (unless it is empty).
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        validate_unwanted_chars(content)
        data = json.loads(content)
        if not check_empty_quotes_in_json(data):
            return False, None
        
        log("JSON format is valid.")
        return True, data
    except json.JSONDecodeError as e:
        log(f"JSON format error: {e}")
        return False, None
    except Exception as e:
        log(f"Error reading JSON file: {e}")
        return False, None

def parse_field_mapping(mapping_str):
    """
    Parses a field mapping from a string.
    Acceptable formats:
      1. Python dictionary style with quotes:
         {'unique_id': 'int', 'competitor_name': 'string', ...}
      2. A simplified format without quotes:
         {unique_id:int,competitor_name:string,...}
    
    Returns a dictionary mapping header names to a list of Python type functions.
    An empty mapping value means no type‐checking for that field.
    Supported types: string, float, int.
    """
    mapping_str = mapping_str.strip()
    
    try:
        mapping_obj = ast.literal_eval(mapping_str)
        if isinstance(mapping_obj, dict):
            mapping = {}
            for key, val in mapping_obj.items():
                key = str(key).strip()
                if isinstance(val, str):
                    val = val.strip().lower()
                else:
                    val = str(val).strip().lower()
                if not val:
                    mapping[key] = []
                else:
                    type_list = []
                    for type_str in val.split(","):
                        type_str = type_str.strip()
                        if type_str == "string":
                            type_list.append(str)
                        elif type_str == "float":
                            type_list.append(float)
                        elif type_str == "int":
                            type_list.append(int)
                        else:
                            raise ValueError(f"Unsupported data type provided for field '{key}': {type_str}")
                    mapping[key] = type_list
            return mapping
    except Exception:
        pass

    if mapping_str.startswith("{") and mapping_str.endswith("}"):
        mapping_str = mapping_str[1:-1]
    else:
        raise ValueError("Mapping must be provided in dictionary format, e.g., {name:string,price:float}")

    mapping = {}
    pairs = re.findall(r'(\w+)\s*:\s*([^,}]+(?:,[^,}]+)*)', mapping_str)
    for key, val in pairs:
        key = key.strip()
        val = val.strip().lower()
        if not val:
            mapping[key] = []
        else:
            type_list = []
            for type_str in val.split(","):
                type_str = type_str.strip()
                if type_str == "string":
                    type_list.append(str)
                elif type_str == "float":
                    type_list.append(float)
                elif type_str == "int":
                    type_list.append(int)
                else:
                    raise ValueError(f"Unsupported data type provided for field '{key}': {type_str}")
            mapping[key] = type_list
    return mapping

def validate_csv(file_path, delimiter, field_mapping):
    """
    Validates a CSV file by:
      - Checking for unwanted characters.
      - Verifying the CSV is read with the correct delimiter.
      - Checking that the header matches the keys provided in field_mapping.
      - Validating that each field in subsequent rows can be converted to at least one of the expected data types.
      - For each field:
           * A cell exactly equal to '' or "" or an empty string is considered a valid null value.
           * Any cell whose raw value starts or ends with a single or double quote is flagged as invalid.
    field_mapping: A dict mapping header names to a list of Python type functions.
                   An empty list means no type‐checking for that field.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        allowed_pattern = rf"^[\x20-\x7E\n\r\t{re.escape(delimiter)}]+$"
        validate_unwanted_chars(content, allowed_pattern=allowed_pattern)
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=delimiter)
            rows = list(reader)
        
        if not rows:
            log("CSV file is empty.")
            return False, None
        
        header = rows[0]
        expected_fields = list(field_mapping.keys())
        if header != expected_fields:
            log(f"CSV header mismatch. Expected {expected_fields} but got {header}")
            return False, None
        
        for row_index, row in enumerate(rows[1:], start=2):
            if len(row) != len(expected_fields):
                log(f"Row {row_index} has an incorrect number of fields. Expected {len(expected_fields)}, got {len(row)}")
                return False, None
            for col_index, field in enumerate(expected_fields):
                value = row[col_index].strip()
                
                # Allow exactly empty quotes or an empty string as a valid null value.
                if value in ["", "''", '""']:
                    continue

                # Check if the value starts or ends with a single or double quote.
                if value and (value[0] in ('"', "'") or value[-1] in ('"', "'")):
                    log(f"Row {row_index} field '{field}' has a value with leading or trailing quotes which is not allowed: {value}")
                    return False, None

                allowed_types = field_mapping[field]
                if not allowed_types:
                    continue

                valid_conversion = False
                for typ in allowed_types:
                    try:
                        if typ is str:
                            valid_conversion = True
                            break
                        else:
                            typ(value)
                            valid_conversion = True
                            break
                    except ValueError:
                        continue
                if not valid_conversion:
                    type_names = ",".join([t.__name__ for t in allowed_types])
                    log(f"Row {row_index} field '{field}' with value '{value}' is not of type {type_names}.")
                    return False, None
        
        log("CSV format is valid.")
        return True, rows
    except Exception as e:
        log(f"Error processing CSV file: {e}")
        return False, None

def main():
    file_path = input("Enter the path of the file to validate: ").strip()
    
    if not os.path.isfile(file_path):
        log("File does not exist. Please check the path and try again.")
        return
    
    ext = os.path.splitext(file_path)[1].lower()
    
    if ext == ".json":
        valid, data = validate_json(file_path)
        if valid:
            log("JSON file validated successfully.")
        else:
            log("JSON file validation failed.")
    
    elif ext == ".csv":
        delimiter = input("Enter the CSV delimiter (e.g., , or ;): ").strip()
        mapping_input = input("Enter field mapping as dictionary (e.g., {'unique_id': 'int', 'competitor_name': 'string', ...}): ").strip()
        try:
            field_mapping = parse_field_mapping(mapping_input)
        except ValueError as e:
            log(f"Error parsing field mapping: {e}")
            return
        
        valid, rows = validate_csv(file_path, delimiter, field_mapping)
        if valid:
            log("CSV file validated successfully.")
        else:
            log("CSV file validation failed.")
    else:
        log("Unsupported file type. Only .json and .csv files are supported.")

    try:
        with open("validation_report.txt", "w", encoding="utf-8") as report_file:
            report_file.write("\n".join(validation_report))
        log("Validation report saved to validation_report.txt")
    except Exception as e:
        log(f"Error writing validation report: {e}")

if __name__ == "__main__":
    main()
