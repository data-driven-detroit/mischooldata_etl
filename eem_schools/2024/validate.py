"""
The validate script uses the pandera schema found ing eem_schema.py
"""
import geopandas as gpd
from ..eem_schema import SchoolAttendance


def validate_eem(logger):
    logger.info("Validating EEM")

    df = gpd.read_file("../../tmp/eem_working_geocoded.parquet")

    # Validate
    validated = SchoolAttendance.validate(df)

    logger.info("Validation successful!")
