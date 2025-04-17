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


def transform_student_mobility(logger):
    logger.info("Beginning transformation of student mobility files.")
    dataset_years = pd.read_csv(WORKING_DIR / "conf" / "dataset_years.csv")
    report_category_recodes = pd.read_csv(WORKING_DIR / "conf" / "report_category_recodes.csv")


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
                dtype={
                    "DistrictCode": "str", 
                    "BuildingCode": "str",
                    "StudentCount": "str",
                    "StudentCountStable": "str",
                    "StudentCountMobile": "str",
                    "StudentCountIncoming": "str",
                }
            )

        except FileNotFoundError:
            logger.error(f"File for year {year["year"]} doesn't exist!")
            continue
        

        result = (
            df.rename(columns=field_reference["renames"])
            .query("building_code != '00000'")
            .assign(
                start_date=year["start_date"],
                end_date=year["end_date"],
            )
            .merge(report_category_recodes, on="report_category_orig")
        )

        for col in ["count", "count_stable", "count_mobile", "count_incoming"]:
            result[col] = (
                result[col]
                .astype(str)
                .apply(parse_to_inequality)
                .apply(unwrap_value)
            )

            result[f"{col}_error"] = (
                result[col]
                .astype(str)
                .apply(parse_to_inequality)
                .apply(unwrap_error)
            )

        all_years.append(result)

    logger.info("Saving combined files.")
    (
        pd.concat(all_years)[STUDENT_MOBILITY_COLUMNS]
        .to_csv(WORKING_DIR / "output" / f"student_mobility_{TODAY}.csv", index=False)
    )

