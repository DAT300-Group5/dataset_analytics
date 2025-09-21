import csv
import os

raw_data_dir = "28509740/raw_data"
sqlite_file = "raw_data.sqlite"

def split_file(input_file):
    """
    Split a CSV file based on the first column (deviceId)
    Save the split files into a folder named after the original file
    Example: acc.csv -> acc/acc_vs14.csv, acc/acc_vs15.csv etc.
    """
    writers = {}
    files = {}
    
    # Get filename (without extension) and directory
    base_dir = os.path.dirname(input_file)
    filename = os.path.splitext(os.path.basename(input_file))[0]
    
    # Create a folder with the same name as the original file
    output_dir = os.path.join(base_dir, filename)
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Processing {input_file}...")
    print(f"Saving split files to {output_dir}/")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)

        for row in reader:
            if not row:  # Skip empty rows
                continue
                
            # Use the first column as device ID to split files
            device_id = row[0]
            if device_id not in writers:
                # Create new file: e.g. acc/acc_vs14.csv
                output_file = os.path.join(output_dir, f"{filename}_{device_id}.csv")
                files[device_id] = open(output_file, 'w', newline='', encoding='utf-8')
                writers[device_id] = csv.writer(files[device_id])
                writers[device_id].writerow(header)
            writers[device_id].writerow(row)
    
    # Close all files
    for f in files.values():
        f.close()
        
    return output_dir  # Return the path to the created folder

if __name__ == "__main__":
    # Check if raw data directory exists
    if not os.path.exists(raw_data_dir):
        print(f"Error: Raw data directory '{raw_data_dir}' not found.")
        exit(1)
    
    # Get list of CSV files
    csv_files = [f for f in os.listdir(raw_data_dir) if f.endswith(".csv")]
    
    if not csv_files:
        print(f"No CSV files found in {raw_data_dir}")
        exit(0)
        
    print(f"Found {len(csv_files)} CSV files to process")
    
    # Process each CSV file, split by device ID
    for file in csv_files:
        csv_file = os.path.join(raw_data_dir, file)
        print(f"\nProcessing file: {file}")
        output_dir = split_file(csv_file)
        print(f"Finished splitting {file}. Files saved to {output_dir}")
    
    print("\nAll CSV files have been split by device ID")

