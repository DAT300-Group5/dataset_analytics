import csv
import os
import gzip
from typing import Dict, TextIO


raw_data_dir: str = "raw_data"


def split_file(input_file: str) -> str:
    """
    Split a CSV file based on the first column (deviceId).
    
    Save the split files into a folder named after the original file.
    Example: acc.csv.gz -> acc/acc_vs14.csv, acc/acc_vs15.csv etc.
    
    Args:
        input_file: Path to the input CSV file (can be compressed with .gz)
        
    Returns:
        str: Path to the directory containing the split files
    """
    writers: Dict[str, csv.writer] = {}
    files: Dict[str, TextIO] = {}

    # Get filename (without extension) and directory
    base_dir: str = os.path.dirname(input_file)
    filename: str = os.path.basename(input_file)
    
    # Handle .csv.gz files
    if filename.endswith('.csv.gz'):
        filename = filename[:-7]  # Remove .csv.gz
    elif filename.endswith('.gz'):
        filename = os.path.splitext(filename)[0]  # Remove .gz
        filename = os.path.splitext(filename)[0]  # Remove .csv
    else:
        filename = os.path.splitext(filename)[0]  # Remove .csv

    # Create a folder with the same name as the original file
    output_dir: str = os.path.join(base_dir, filename)
    os.makedirs(output_dir, exist_ok=True)

    print(f"Processing {input_file}...")
    print(f"Saving split files to {output_dir}/")

    # Determine if file is compressed
    is_compressed: bool = input_file.endswith('.gz')
    
    # Open file (compressed or uncompressed)
    if is_compressed:
        f = gzip.open(input_file, 'rt', encoding='utf-8')
    else:
        f = open(input_file, 'r', encoding='utf-8')
    
    try:
        reader: csv.reader = csv.reader(f)
        header: list = next(reader)

        for row in reader:
            if not row:  # Skip empty rows
                continue

            # Use the first column as device ID to split files
            device_id: str = row[0]
            if device_id not in writers:
                # Create new file: e.g. acc/acc_vs14.csv
                output_file: str = os.path.join(output_dir, f"{filename}_{device_id}.csv")
                files[device_id] = open(output_file, 'w', newline='', encoding='utf-8')
                writers[device_id] = csv.writer(files[device_id])
                writers[device_id].writerow(header)
            writers[device_id].writerow(row)
    finally:
        # Close the input file
        f.close()

    # Close all output files
    for file_handle in files.values():
        file_handle.close()

    return output_dir  # Return the path to the created folder


def main() -> None:
    """Main function to process all CSV files in the raw data directory."""
    # Check if raw data directory exists
    if not os.path.exists(raw_data_dir):
        print(f"Error: Raw data directory '{raw_data_dir}' not found.")
        exit(1)

    # Get list of CSV files (both .csv and .csv.gz)
    csv_files: list[str] = [f for f in os.listdir(raw_data_dir) if f.endswith(".csv") or f.endswith(".csv.gz")]

    if not csv_files:
        print(f"No CSV files found in {raw_data_dir}")
        exit(0)

    print(f"Found {len(csv_files)} CSV files to process")

    # Process each CSV file, split by device ID
    for file in csv_files:
        csv_file: str = os.path.join(raw_data_dir, file)
        print(f"\nProcessing file: {file}")
        output_dir: str = split_file(csv_file)
        print(f"Finished splitting {file}. Files saved to {output_dir}")

    print("\nAll CSV files have been split by device ID")


if __name__ == "__main__":
    main()
