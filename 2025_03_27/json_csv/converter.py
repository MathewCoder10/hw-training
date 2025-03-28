import os
import json
import pandas as pd
import logging

# Configure logging to file
logging.basicConfig(
    level=logging.ERROR,
    filename='conversion_errors.log',
    filemode='a',  # Append to the log file
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def detect_json_format(file_path):
    """
    Detect whether a JSON file is in a standard JSON array format or JSON Lines (JSONL) format.
    Returns 'json' for standard JSON and 'jsonl' for JSON Lines.
    """
    try:
        with open(file_path, 'r') as f:
            # Read the first non-empty line
            for line in f:
                stripped = line.strip()
                if stripped:
                    # If the line starts with '[', assume standard JSON array
                    if stripped[0] == '[':
                        return 'json'
                    else:
                        return 'jsonl'
    except Exception as e:
        logging.error(f"Error detecting JSON format: {e}")
    return 'json'  # Default to standard JSON if undetectable

def json_to_csv(json_file, csv_file):
    try:
        fmt = detect_json_format(json_file)
        print(f"Detected JSON format: {fmt.upper()}")
        if fmt == 'jsonl':
            # Read JSON Lines file
            df = pd.read_json(json_file, lines=True)
        else:
            # Read standard JSON file (assumes a JSON array)
            df = pd.read_json(json_file)
        # Ask the user for the CSV delimiter for output
        delimiter = input("Enter the CSV delimiter for output (e.g., ',' or ';' or '\\t' for tab): ")
        # Convert DataFrame to CSV file with the chosen delimiter
        df.to_csv(csv_file, index=False, sep=delimiter)
        print(f"Successfully converted {json_file} to {csv_file} using delimiter '{delimiter}'")
    except Exception as e:
        logging.error(f"Error converting JSON to CSV: {e}")
        print("An error occurred during conversion. Please check the log for details.")

def csv_to_json(csv_file, json_file, delimiter, output_format):
    try:
        # Read CSV file using the specified delimiter
        df = pd.read_csv(csv_file, delimiter=delimiter)
        if output_format.lower() == 'jsonl':
            # Convert DataFrame to JSON Lines format
            df.to_json(json_file, orient='records', lines=True)
        else:
            # Convert DataFrame to standard JSON format with indentation
            df.to_json(json_file, orient='records', indent=4)
        print(f"Successfully converted {csv_file} to {json_file} in {output_format.upper()} format")
    except Exception as e:
        logging.error(f"Error converting CSV to JSON: {e}")
        print("An error occurred during conversion. Please check the log for details.")

def main():
    file_path = input("Enter the full path to the file: ").strip()
    
    if not os.path.exists(file_path):
        print("File does not exist.")
        return

    base_name, ext = os.path.splitext(file_path)
    
    if ext.lower() == '.csv':
        # Ask for the CSV delimiter used in the input file
        delimiter = input("Enter the CSV delimiter (e.g., ',' or ';' or '\\t' for tab): ")
        # Ask the user for the desired JSON output format
        output_format = input("Enter desired JSON output format ('json' for standard JSON, 'jsonl' for JSON Lines): ").strip().lower()
        if output_format not in ['json', 'jsonl']:
            print("Invalid JSON format selected. Defaulting to standard JSON.")
            output_format = 'json'
        # Set output file extension based on the selected format
        output_ext = '.jsonl' if output_format == 'jsonl' else '.json'
        output_file = base_name + output_ext
        csv_to_json(file_path, output_file, delimiter, output_format)
    
    elif ext.lower() in ['.json', '.jsonl']:
        # Detect input JSON format and inform the user
        fmt = detect_json_format(file_path)
        print(f"Detected {fmt.upper()} format for the input file.")
        output_file = base_name + '.csv'
        json_to_csv(file_path, output_file)
    
    else:
        print("Unsupported file type. Please provide a JSON or CSV file.")

if __name__ == "__main__":
    main()
