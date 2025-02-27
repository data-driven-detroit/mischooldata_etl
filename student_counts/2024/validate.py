"""
The validate script uses the pandera schema found ing eem_schema.py
"""

from ..student_counts_schema import StudentCounts
from pathlib import Path
from datetime import date
import geopandas as gpd
import pandera as pa
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries


SC_DIR = Path(__file__).resolve().parent.parent

class studentcounts(pa.DataFrameModel):

    isd_code: str = pa.Field()
    isd_name: str = pa.Field()
    district_code: str = pa.Field()
    district_name: str = pa.Field()
    building_code: str = pa.Field()
    building_name: str = pa.Field()
    county_code: str = pa.Field()
    county_name: str = pa.Field()
    entity_type: str = pa.Field()
    school_level: str = pa.Field()
    locale_name: str = pa.Field()
    mistem_name: str = pa.Field()
    mistem_code: str = pa.Field()
    total_enrollment: str = pa.Field()
    male_enrollment: str = pa.Field()
    female_enrollment: str = pa.Field()
    american_indian_enrollment: str = pa.Field()
    asian_enrollment: str = pa.Field()
    african_american_enrollment: str = pa.Field()
    hispanic_enrollment: str = pa.Field()
    hawaiian_enrollment: str = pa.Field()
    white_enrollment: str = pa.Field()
    two_or_more_races_enrollment: str = pa.Field()
    early_middle_college_enrollment: str = pa.Field()
    prekindergarten_enrollment: str = pa.Field()
    kindergarten_enrollment: str = pa.Field()
    grade_1_enrollment: str = pa.Field()
    grade_2_enrollment: str = pa.Field()
    grade_3_enrollment: str = pa.Field()
    grade_4_enrollment: str = pa.Field()
    grade_5_enrollment: str = pa.Field()
    grade_6_enrollment: str = pa.Field()
    grade_7_enrollment: str = pa.Field()
    grade_8_enrollment: str = pa.Field()
    grade_9_enrollment: str = pa.Field()
    grade_10_enrollment: str = pa.Field()
    grade_11_enrollment: str = pa.Field()
    ungraded_enrollment: str = pa.Field()
    economic_disadvantaged_enrollment: str = pa.Field()
    special_education_enrollment: str = pa.Field()
    english_language_learners_enrollment: str = pa.Field()
    # geometry: GeoSeries = pa.Field(nullable=True) #TODO:Do we need this in Student Counts?
    start_date: date = pa.Field(coerce=True)
    end_date: date = pa.Field(coerce=True)

    @pa.check("isd_code")
    def isd_code_correct_len(cls, district_code: Series[str]) -> Series[bool]:
        return district_code.str.len() == 2

    @pa.check("isd_code")
    def isd_code_correct_not_zeros(cls, district_code: Series[str]) -> Series[bool]:
        return district_code.str.len() != '00'

    @pa.check("district_code")
    def district_code_correct_len(cls, district_code: Series[str]) -> Series[bool]:
        return district_code.str.len() == 5

    @pa.check("district_code")
    def district_code_not_zeros(cls, district_code: Series[str]) -> Series[bool]:
        return district_code != '00000'

    @pa.check("building_code")
    def building_code_correct_len(cls, building_code: Series[str]) -> Series[bool]:
        return building_code.str.len() == 5

    @pa.check("building_code")
    def building_code_not_zeros(cls, building_code: Series[str]) -> Series[bool]:
        return building_code != '00000'
    
    @pa.check("mistem_code")
    def building_code_not_zeros(cls, building_code: Series[str]) -> Series[bool]:
        return mistem_code.str.len() == 2


def validate_student_counts(logger):
    logger.info("Validating Student Counts")

    df = gpd.read_file("../../tmp/student_counts_working_geocoded.parquet")

    # Validate - This throws a helpful error if it doesn't work.
    validated = StudentCounts.validate(df)

    logger.info("Validation successful!")
