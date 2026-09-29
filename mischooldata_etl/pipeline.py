"""The shared transform/load steps the dataset modules build on."""
from pathlib import Path
import json
import pandas as pd
from inequalitytools import parse_to_inequality

from .config import get_config
from .db import get_db_engine


def load_field_reference(working_dir: Path, field_reference_file: str) -> dict:
    return json.loads((working_dir / "conf" / field_reference_file).read_text())


def load_output_schema(working_dir: Path) -> pd.DataFrame:
    return pd.read_csv(working_dir / "conf" / "output_schema.csv")


def unwrap_value(inequality):
    """The value half of a parsed Inequality -- see transform_process below."""
    value, _ = inequality.unwrap()
    return value


def unwrap_error(inequality):
    """The error half of a parsed Inequality -- see transform_process below."""
    _, error = inequality.unwrap()
    return error


def transform_process(frame, field_reference):
    frame = frame.rename(columns=field_reference["renames"])
    
    for field in field_reference["suppressed_cols"]:
        frame[[field, f"{field}_error"]] = (
            frame[field]
            .apply(parse_to_inequality)
            .apply(lambda i: i.unwrap())
            .to_list()
        )
    
    return frame


def apply_output_schema(frame, output_schema):
    type_mapping = {
        'str': 'object',
        'int': pd.Int64Dtype(),
        'float': 'float64'
    }

    for _, row in output_schema.iterrows():
        col_name = row['column_name']
        data_type = row['data_type']
        required = row['required']
        default_value = row['default_value']

        if col_name not in frame.columns:
            if pd.isna(default_value) or default_value == '' or default_value == 'NA':
                frame[col_name] = pd.NA
            else:
                frame[col_name] = default_value

        if data_type in type_mapping:
            frame[col_name] = frame[col_name].astype(type_mapping[data_type])

    schema_columns = output_schema['column_name'].tolist()
    return frame[schema_columns]


def apply_padding(frame):
    for col, padding in [("isd_code", 2), ("district_code", 5), ("building_code", 5)]:
        frame[col] = frame[col].str.zfill(padding)
    
    return frame


def generic_transform(module_name: str, working_dir: Path):
    """Rebuild output/combined_years.csv from every year in dataset_years.csv."""
    config = get_config()

    output_dir = working_dir / "output" / "combined_years.csv"
    output_dir.parent.mkdir(exist_ok=True)

    dataset_years = pd.read_csv(working_dir / "conf" / "dataset_years.csv")
    output_schema = load_output_schema(working_dir)

    print(f"Processing {len(dataset_years)} date ranges for {module_name}")

    mode, header = "w", True
    for _, year in dataset_years.iterrows():
        print(f"Opening {year['source_file']}")

        field_reference = json.loads(
            (working_dir / "conf" / year["field_reference_file"]).read_text()
        )

        frame = (
            pd.read_csv(
                Path(config["vault_location"]) / year["source_file"],
                dtype=field_reference["in_types"],
                low_memory=False, 
            )
            .rename(columns=field_reference["renames"])
            .pipe(lambda frame: transform_process(frame, field_reference))
            .pipe(apply_padding)
            .assign(start_date=year["start_date"], end_date=year["end_date"])
            .pipe(lambda frame: apply_output_schema(frame, output_schema))
        )

        frame.to_csv(output_dir, mode=mode, header=header, index=False)
        mode, header = "a", False


def generic_load(table_name: str, working_dir: Path, special_processing=None):
    field_reference_files = list((working_dir / "conf").glob("field_reference_*.json"))
    if not field_reference_files:
        raise FileNotFoundError(f"No field reference file found in {working_dir / 'conf'}")

    field_reference = load_field_reference(working_dir, field_reference_files[0].name)

    # Full replace in one transaction: if any chunk fails, the old table survives.
    if_exists = "replace"
    with get_db_engine().begin() as db:
        for i, portion in enumerate(pd.read_csv(
            working_dir / "output" / "combined_years.csv",
            chunksize=20_000,
            dtype=field_reference["out_types"],
        ), start=1):
            print(f"Loading chunk {i} into database.")

            if special_processing:
                portion = special_processing(portion)

            portion.to_sql(
                table_name, db, schema="education", if_exists=if_exists, index=False
            )
            if_exists = "append"
