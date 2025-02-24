"""
The validate script uses the pandera schema found ing eem_schema.py
"""
import geopandas as gpd
from ..student_counts_schema import StudentCounts


def validate_student_counts(logger):
    logger.info("Validating Student Counts")

    df = gpd.read_file("../../tmp/student_counts_working_geocoded.parquet")

    # Validate - This throws a helpful error if it doesn't work.
    validated = StudentCounts.validate(df)

    logger.info("Validation successful!")
