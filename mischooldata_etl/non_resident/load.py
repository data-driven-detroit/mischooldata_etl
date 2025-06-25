import datetime
from pathlib import Path
import pandas as pd

from mischooldata_etls import db_engine
from schema import NonResident


WORKING_DIR = Path(__file__).parent
TODAY = datetime.date.today().strftime("%Y%m%d")


def load_non_resident(logger):
    logger.info("Loading non_resident to database.")
    file = pd.read_csv(
        WORKING_DIR / "input" / f"resident_grade_prepped_{TODAY}.csv",
        dtype={"resident_district_code": "str", "operating_district_code": "str", "grade_code": "str"}
    )

    validated = NonResident.validate(file)

    # We're doing full replaces on these tables
    validated.to_sql(
        "non_resident", db_engine, schema="education", if_exists="replace"
    )
