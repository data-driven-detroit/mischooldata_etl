from pathlib import Path
from datetime import date
import pandas as pd
import pandera as pa
from pandera.typing import Series


ATTENDANCE_DIR = Path(__file__).resolve().parent.parent


class SchoolAttendance(pa.DataFrameModel):
    """
    The table for this needs to be replaced with the breakdowns avaialable from
    the full attendance file.
    """
    district_code: str = pa.Field(coerce=True)
    building_code: str = pa.Field(coerce=True)
    report_category: str = pa.Field(coerce=True)
    report_subgroup: str = pa.Field(coerce=True)
    total_students: int = pa.Field(nullable=True) # these are covered by pd.Int64Dtype so no coerce
    total_students_error: int = pa.Field(nullable=True)
    chronically_absent: int = pa.Field(nullable=True)
    chronically_absent_error: int = pa.Field(nullable=True)
    start_date: date = pa.Field(nullable=False)
    end_date: date = pa.Field(nullable=False)
    
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
    

def validate_attendance(logger):
    logger.info("Validating attendance")

    df = pd.read_parquet(ATTENDANCE_DIR.parent / "tmp" / "attendance_working.parquet.gzip")

    # Validate - This throws a helpful error if it doesn't work.
    validated = SchoolAttendance.validate(df)

    logger.info("Validation successful!")
