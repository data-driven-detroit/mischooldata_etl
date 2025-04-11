from pathlib import Path
import datetime
import json
import pandas as pd
import numpy as np

from inequalitytools import parse_to_inequality
from mischooldata_etls import unwrap_value, unwrap_error

from schema import STUDENT_MOBILITY_COLUMNS


TODAY = datetime.date.today().strftime("%Y%m%d")
WORKING_DIR = Path(__file__).parent


def transform_student_counts(logger):
    logger.info("Beginning transformation of student count files.")
    dataset_years = pd.read_csv(WORKING_DIR / "conf" / "dataset_years.csv")

    if (WORKING_DIR / "output" / f"student_counts_{TODAY}.csv").exists():
        logger.warning(
            "File created today already exists -- remove to rerun transform."
        )
        return

    all_years = []
    for _, year in dataset_years.iterrows():
        logger.info(f"Transforming file for year {year["year"]}")

        field_reference = json.loads(
            (WORKING_DIR / "conf" / year["field_reference_file"]).read_text()
        )

        try:
            df = pd.read_csv(
                year["source_file"], 
                dtype={"district_code": "str", "building_code": "str"}
            )

        except FileNotFoundError:
            logger.error(f"File for year {year["year"]} doesn't exist!")
            continue
        

        result = (
            df.rename(field_reference["renames"])
            .assign(year=year["year"])
        )

        all_years.append(result)

    (
        pd.concat(all_years)[STUDENT_MOBILITY_COLUMNS]
        .to_csv(WORKING_DIR / "output" / f"student_mobility_{TODAY}.csv")
    )

