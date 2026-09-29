import logging
from pathlib import Path
import datetime
import pandas as pd

from ..db import get_db_engine
from .schema import StudentCounts


logger = logging.getLogger(__name__)


WORKING_DIR = Path(__file__).parent
TODAY = datetime.date.today().strftime("%Y%m%d")


def load_student_counts():
    logger.info("Loading student_counts for all years into DB.")

    file = pd.read_csv(
        WORKING_DIR / "output" / f"student_counts_{TODAY}.csv",
        dtype={"district_code": "str", "building_code": "str"}
    )

    validated = StudentCounts.validate(file)

    # We're doing full replaces on these tables
    validated.to_sql(
        "student_counts", get_db_engine(), schema="education", if_exists="replace"
    )
