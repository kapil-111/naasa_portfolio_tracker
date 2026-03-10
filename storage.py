import csv
import json
import os
from tempfile import NamedTemporaryFile
import shutil

def save_to_csv(data: list, filename="portfolio_data.csv"):
    """
    Saves a list of dictionaries to a CSV file.
    """
    if not data:
        print("No data to save to CSV.")
        return

    fieldnames = data[0].keys()
    
    # Use a temporary file to write data first to avoid corruption
    temp_file = NamedTemporaryFile(mode='w', delete=False, newline='', encoding='utf-8')
    
    try:
        with temp_file as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
            
        shutil.move(temp_file.name, filename)
        print(f"Data saved to {filename}")
        
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)

def save_to_json(data: dict, filename="portfolio_summary.json"):
    """
    Saves a dictionary to a JSON file.
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        print(f"Data saved to {filename}")
    except Exception as e:
        print(f"Error saving to JSON: {e}")
