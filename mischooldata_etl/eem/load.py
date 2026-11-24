import logging
from pathlib import Path

import geopandas as gpd
from sqlalchemy.types import Date

from ..db import get_db_engine
from ..pipeline import generic_load
from .geocode import OUTPUT_FILE
from .schema import school_geocodes_schema


logger = logging.getLogger(__name__)


WORKING_DIR = Path(__file__).parent


def load_eem():
    generic_load("eem", WORKING_DIR)


def load_school_geocode():
    frame = gpd.read_file(OUTPUT_FILE)
    validated = school_geocodes_schema().validate(frame)
    logger.info("Loading %s rows into school_geocodes.", len(validated))

    # Full replace in one transaction: if it fails, the old table survives.
    with get_db_engine().begin() as db:
        validated.to_postgis(
            "school_geocodes", db, schema="education", if_exists="replace",
            index=False, dtype={"start_date": Date(), "end_date": Date()},
        )


if __name__ == "__main__":
    load_eem()
