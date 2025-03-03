import json
from pathlib import Path
import datetime
import click
import pandas as pd
from sqlalchemy.orm import sessionmaker
import tomli
from inequalitytools import parse_to_inequality
from metadata_audit.capture import record_metadata

from mischooldata_etls import db_engine


EDITION_DATE = "2024-06-29"
SC_DIR = Path(__file__).resolve().parent.parent

def clean_fields(df):
    """
    Mostly renames following the config file, then makes sure the 
    various codes are the right length.
    """
    return (
        df
        .rename(columns = {"isdcode": "isd_code",
                           "isdname": "isd_name",
                           "districtcode": "district_code", 
                           "buildingcode": "building_code", 
                           "districtname": "district_name",
                           "buildingname": "building_name",
                           "countycode": "county_code",
                           "countyname": "county_name",
                           "entitytype": "entity_type",
                           "schoollevel": "school_level"})
        .assign(
            isd_code=lambda df: df["__isd_code"].astype(str).str.zfill(2),
            district_code=lambda df: df["__district_code"].astype(str).str.zfill(5),
            building_code=lambda df: df["__building_code"].astype(str).str.zfill(5),
            county_code=lambda df: df["__county_code"].astype(str).str.zfill(2),
        )
        .query("(district_code != '00000') & (building_code != '00000')")
    )

def parse_to_inequality(value):
    """Parses suppressed fields into inequality representations or errors."""
    if isinstance(value, str):
        value = value.strip()
        if value.startswith("≤"):
            return f"<= {value[1:].strip()}"
        elif value.startswith("≥"):
            return f">= {value[1:].strip()}"
        elif value.lower() in ["suppressed", "n/a", "null", "-"]:
            return "ERROR: Suppressed Value"
    return value

def transform_student_counts(logger):
    # Open the file
    # Renmae the columns
    # Parse suppressed fields to value / error with 'parse_to_inequality'
    logger.info("Transformation on Student Counts started.")

    with open(SC_DIR / "metadata.toml", "rb") as md:
        metadata = tomli.load(md)

    edition = metadata["editions"][EDITION_DATE]

    df = pd.read_csv(SC_DIR.parent / "tmp" / "student_counts_working.csv")

    # Main clean-up
    cleaned_fields = clean_fields(df)

    dated = cleaned_fields.assign(
        start_date = datetime.date.fromisoformat(edition["start"]),
        end_date = datetime.date.fromisoformat(edition["end"]),
    )

    # Parse suppressed fields to value/error with 'parse_to_inequality'
    suppressed_df = dated.applymap(parse_to_inequality)
    logger.info("Suppressed fields parsed successfully.")

    suppressed_df.to_file(SC_DIR.parent / "tmp" / "student_counts_working.parquet")

