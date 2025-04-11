from pathlib import Path
import datetime
import json
import pandas as pd
import numpy as np

from inequalitytools import parse_to_inequality
from mischooldata_etls import unwrap_value, unwrap_error

from schema import STUDENT_COUNTS_COLUMNS


TODAY = datetime.date.today().strftime("%Y%m%d")
WORKING_DIR = Path(__file__).parent


def transform_student_counts(logger):
    logger.info("Beginning transformation of student count files.")

    dataset_years = pd.read_csv(WORKING_DIR / "conf" / "dataset_years.csv")

    if (WORKING_DIR / "output" / f"student_counts_{TODAY}.csv").exists():
        logger.warning("File created today already exists -- remove to rerun transform.")
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

        wide_table = (
            df.rename(columns=field_reference["renames"])
            .query("building_code!='00000'")
            .dropna(subset=["district_code", "building_code"])
        )[field_reference["renames"].values()]

        wide_table["district_code"] = np.where(
            wide_table["district_code"].notna(),
            wide_table["district_code"].astype("int").astype("str").str.zfill(5),
            pd.NA,
        )

        wide_table["building_code"] = np.where(
            wide_table["building_code"].notna(),
            wide_table["building_code"].astype("int").astype("str").str.zfill(5),
            pd.NA,
        )


        long_table = (
            pd.wide_to_long(
                wide_table, 
                i=["district_code", "building_code"], 
                stubnames=field_reference["stubnames"],
                suffix=".*",
                sep="_",
                j="report_subgroup"
            )
            .reset_index()
            .melt(
                id_vars=[
                    "district_code", 
                    "building_code", 
                    "report_subgroup"
                ],
                var_name="report_category",
                value_name="student_count_suppressed"
            )
            .dropna(subset=["building_code", "student_count_suppressed"])
            .astype({"student_count_suppressed": "str"})
            .assign(
                start_date=year["start_date"],
                end_date=year["end_date"],
                count=lambda df: df["student_count_suppressed"].apply(parse_to_inequality).apply(unwrap_value),
                count_error=lambda df: df["student_count_suppressed"].apply(parse_to_inequality).apply(unwrap_error)
            )
            .astype({
                "count": pd.Int64Dtype(),
                "count_error": pd.Int64Dtype(),
            })
        )
        all_years.append(long_table)

    (
        pd.concat(all_years)[STUDENT_COUNTS_COLUMNS]
        .to_csv(WORKING_DIR / "output" / f"student_counts_{TODAY}.csv", index=False)
    )