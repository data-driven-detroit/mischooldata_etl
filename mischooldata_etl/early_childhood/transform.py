import json
from pathlib import Path
import pandas as pd
from inequalitytools import (
    parse_to_inequality
)

WORKING_DIR = Path(__file__).parent


def transform_process(frame, field_reference):
    frame = frame.rename(columns=field_reference["renames"])
    
    # Parse all suppressed fields to 'Inequality'
    for field in field_reference["suppressed_cols"]:
        frame[[field, f"{field}_error"]] = (
            frame[field]
            .apply(parse_to_inequality)
            .apply(lambda i: i.unwrap())
            .to_list() # Allows this to unpack to columns
        )

    return frame


def apply_padding(frame):
    for col, padding in [("isd_code", 2), ("district_code", 5), ("onsr", 9)]:
        frame[col] = frame[col].str.zfill(padding)

    return frame


def transform_early_childhood():
    output_dir = WORKING_DIR / "output" / "combined_years.csv" 
    if output_dir.exists():
        print("Files already compiled. To rerun complication script delete 'output/combined_years.csv'")
        return

    dataset_years = pd.read_csv(WORKING_DIR / "conf" / "dataset_years.csv")
    
    mode, header = "w", True
    for _, year in dataset_years.iterrows():
        print(f"Opening {year['source_file']}")

        field_reference = json.loads(
            (WORKING_DIR / "conf" / year["field_reference_file"]).read_text()
        )

        frame = (
            pd.read_csv(
                year["source_file"],  # type: ignore
                dtype=field_reference["in_types"],
                low_memory=False, 
            )
            .rename(columns=field_reference["renames"])
            .pipe(lambda frame: transform_process(frame, field_reference))
            .pipe(apply_padding)
            .assign(start_date=year["start_date"], end_date=year["end_date"])
        )[field_reference["out_cols"]]

        frame.to_csv(output_dir, mode=mode, header=header, index=False)
        mode, header = "a", False # Append all but the first


if __name__ == "__main__":
    transform_attendance()
