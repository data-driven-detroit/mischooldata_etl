import logging
from pathlib import Path
import pandas as pd

from ..db import get_db_engine
from .schema import NonResident


logger = logging.getLogger(__name__)


WORKING_DIR = Path(__file__).parent


def load_non_resident():
    logger.info("Loading non_resident to database.")
    file = pd.read_csv(
        WORKING_DIR / "input" / "resident_grade_prepped.csv",
        dtype={"resident_district_code": "str", "operating_district_code": "str", "grade_code": "str"}
    )

    validated = NonResident.validate(file)

    # We're doing full replaces on these tables
    validated.to_sql(
        "non_resident", get_db_engine(), schema="education", if_exists="replace"
    )
