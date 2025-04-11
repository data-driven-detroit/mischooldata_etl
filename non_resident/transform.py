import json
import datetime
from pathlib import Path
import pandas as pd

import datetime

from schema import NON_RESIDENT_COLUMNS


TODAY = datetime.date.today().strftime("%Y%m%d")
WORKING_DIR = Path(__file__).parent


def transform_non_resident(logger):
    logger.info("Transforming non_resident files!")

    dataset_years = pd.read_csv(WORKING_DIR / "conf" / "dataset_years.csv")

    if (WORKING_DIR / "input" / f"resident_grade_prepped_{TODAY}.csv").exists():
        logger.warning("File created today already exists -- remove to rerun transform.")
        return

    all_years = []
    for _, year in dataset_years.iterrows():
        logger.info(f"Transforming file for year {year["year"]}")
        field_reference = json.loads(
            (WORKING_DIR / "conf" / year["field_reference_file"]).read_text()
        )


        df = (
            pd.read_csv(year["source_file"])
            .rename(columns=field_reference["rename"])
            .assign(
                start_date=datetime.date.fromisoformat(year["start_date"]),
                end_date=datetime.date.fromisoformat(year["end_date"]),
            )
        )

        df["operating_district_code"] = (
            df["operating_district_code"].astype(str).str.zfill(5)
        )
        df["resident_district_code"] = (
            df["resident_district_code"].astype(str).str.zfill(5)
        )
        df["grade_code"] = (
            df["grade_code"].astype(str).str.zfill(2)
        )

        for col, mapping in field_reference["recodes"].items():
            df[col] = df[col].map(mapping)

        all_years.append(df)


    logger.info("Saving compiled file from all available years.")
    pd.concat(all_years)[NON_RESIDENT_COLUMNS].to_csv(
        WORKING_DIR / "input" / f"resident_grade_prepped_{TODAY}.csv",
        index=False,
    )
