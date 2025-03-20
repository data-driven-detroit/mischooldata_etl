from pathlib import Path
import pandas as pd
from mischooldata_etls import db_engine


WORKING_DIR = Path(__file__).parent


def load_non_resident(logger):
    logger.info("Loading non_resident to database.")
    logger.warning("non_resident validation step not written yet!")

    file = pd.read_csv(
        WORKING_DIR / "input" / "resident_grade_prepped_20250320.csv"
    )

    file.to_sql(
        "non_resident", db_engine, schema="education", if_exists="append"
    )
