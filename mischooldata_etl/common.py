from pathlib import Path
import json
import pandas as pd
from sqlalchemy import create_engine
import tomli
from inequalitytools import parse_to_inequality


def get_config():
    base_dir = Path(__file__).parent
    with open(base_dir / "config.toml", "rb") as f:
        return tomli.load(f)


def get_db_engine():
    config = get_config()
    return create_engine(
        f"postgresql+psycopg://{config['db']['user']}:{config['db']['password']}"
        f"@{config['db']['host']}:{config['db']['port']}/{config['db']['name']}",
        connect_args={'options': f'-csearch_path={config["app"]["name"]},public'},
    )


def load_field_reference(working_dir: Path, field_reference_file: str) -> dict:
    return json.loads((working_dir / "conf" / field_reference_file).read_text())


def load_output_schema(working_dir: Path) -> pd.DataFrame:
    return pd.read_csv(working_dir / "conf" / "output_schema.csv")


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
        'int': 'int64', 
        'float': 'float64'
    }
    
    for _, row in output_schema.iterrows():
        col_name = row['column_name']
        data_type = row['data_type']
        required = row['required']
        default_value = row['default_value']
        
        if col_name not in frame.columns:
            if pd.isna(default_value) or default_value == '':
                frame[col_name] = None
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
    output_dir = working_dir / "output" / "combined_years.csv" 
    
    dataset_years = pd.read_csv(working_dir / "conf" / "dataset_years.csv")
    
    # Check existing dates in the database table
    db_engine = get_db_engine()
    existing_dates = set()
    try:
        with db_engine.connect() as db:
            existing_df = pd.read_sql(
                f"SELECT DISTINCT start_date, end_date FROM education.{module_name}", 
                db
            )
            existing_dates = set(zip(existing_df['start_date'], existing_df['end_date']))
            print(f"Found {len(existing_dates)} existing date ranges in {module_name} table")
    except Exception as e:
        print(f"Table {module_name} doesn't exist or error querying: {e}")
        print("Will process all files")
    
    # Filter to only include years not already in database
    new_years = []
    for _, year in dataset_years.iterrows():
        date_pair = (year['start_date'], year['end_date'])
        if date_pair not in existing_dates:
            new_years.append(year)
        else:
            print(f"Skipping {year['start_date']} to {year['end_date']} - already exists in database")
    
    if not new_years:
        print("No new data to process - all years already exist in database")
        if output_dir.exists():
            print("Removing existing combined_years.csv since no new data to add")
            output_dir.unlink()
        return
    
    print(f"Processing {len(new_years)} new date ranges")
    
    # Load output schema
    output_schema = load_output_schema(working_dir)
    
    # If we have existing data and new data, we need to append mode
    # If output file exists but we have new data, remove it and recreate with all data
    if output_dir.exists():
        print("Removing existing combined_years.csv to recreate with new data")
        output_dir.unlink()
    
    mode, header = "w", True
    for year in new_years:
        print(f"Opening {year['source_file']}")

        field_reference = json.loads(
            (working_dir / "conf" / year["field_reference_file"]).read_text()
        )

        frame = (
            pd.read_csv(
                year["source_file"],
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
    
    db_engine = get_db_engine()
    with db_engine.connect() as db:
        if_exists = "replace"
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
