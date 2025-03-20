import json
import datetime
from pathlib import Path
import pandas as pd


WORKING_DIR = Path(__file__).parent


def transform_non_resident(logger):
    logger.warning("Transforming non_resident file!")

    field_reference = json.loads(
        (WORKING_DIR / "conf" / "field_reference.json").read_text()
    )

    filepath = WORKING_DIR / "input" / "non_resident_2022_2023.csv"

    df = (
        pd.read_csv(filepath)
        .rename(columns=field_reference["rename"])
        .assign(
            start_date=datetime.date.fromisoformat("2022-07-01"),
            end_date=datetime.date.fromisoformat("2023-06-30"),
        )
    )

    df["operating_district_code"] = (
        df["operating_district_code"].astype(str).str.zfill(5)
    )
    df["resident_district_code"] = (
        df["resident_district_code"].astype(str).str.zfill(5)
    )

    for col, mapping in field_reference["recodes"].items():
        df[col] = df[col].map(mapping)

    df[field_reference["column_order"]].to_csv(
        WORKING_DIR / "input" / "resident_grade_prepped_20250320.csv",
        index=False,
    )
