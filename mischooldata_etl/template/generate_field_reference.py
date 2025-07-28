#!/usr/bin/env python3
"""
Script to generate field_reference.json template from a CSV file.
Inspects the CSV columns and creates a template with all columns in 'renames'.
"""

import json
import pandas as pd
import argparse
from pathlib import Path


def generate_field_reference(csv_path: str, output_path: str = None):
    """Generate field reference JSON from CSV file columns."""
    
    # Read just the header to get column names
    df = pd.read_csv(csv_path, nrows=0)
    columns = df.columns.tolist()
    
    # Create field reference template
    field_reference = {
        "renames": {col: col for col in columns},  # Start with identity mapping
        "suppressed_cols": [],  # User will need to populate this
        "in_types": {col: "str" for col in columns},  # Default all to string
        "out_types": {col: "str" for col in columns}   # Default all to string
    }
    
    # Determine output path
    if output_path is None:
        csv_stem = Path(csv_path).stem
        output_path = f"field_reference_{csv_stem}.json"
    
    # Write the JSON file
    with open(output_path, 'w') as f:
        json.dump(field_reference, f, indent=4)
    
    print(f"Generated field reference template: {output_path}")
    print(f"Found {len(columns)} columns:")
    for col in columns:
        print(f"  - {col}")
    print("\nNext steps:")
    print("1. Update 'renames' to map old column names to standardized names")
    print("2. Add columns with suppressed data to 'suppressed_cols'")
    print("3. Update 'in_types' and 'out_types' with correct data types")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate field reference JSON from CSV")
    parser.add_argument("csv_file", help="Path to CSV file to inspect")
    parser.add_argument("-o", "--output", help="Output JSON file path")
    
    args = parser.parse_args()
    generate_field_reference(args.csv_file, args.output)