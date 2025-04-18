from datetime import date
import pandas as pd
import pandera as pa
from pandera.typing import Series


class StudentMobility(pa.DataFrameModel):
    """
    The table for this needs to be replaced with the breakdowns avaialable from
    the full attendance file.
    """
    district_code: str = pa.Field(coerce=True)
    building_code: str = pa.Field(coerce=True)
    report_category: str = pa.Field(coerce=True)
    report_subgroup: str = pa.Field(coerce=True)
    start_date: date = pa.Field(nullable=False, coerce=True)
    end_date: date = pa.Field(nullable=False, coerce=True)
    count: pd.Int64Dtype = pa.Field(nullable=False, coerce=True)
    count_error: pd.Int64Dtype = pa.Field(nullable=False, coerce=True)
    count_stable: pd.Int64Dtype = pa.Field(nullable=False, coerce=True)
    count_stable_error: pd.Int64Dtype = pa.Field(nullable=False, coerce=True)
    count_mobile: pd.Int64Dtype = pa.Field(nullable=False, coerce=True)
    count_mobile_error: pd.Int64Dtype = pa.Field(nullable=False, coerce=True)
    count_incoming: pd.Int64Dtype = pa.Field(nullable=False, coerce=True)
    count_incoming_error: pd.Int64Dtype = pa.Field(nullable=False, coerce=True)

    class Config:
        strict = True    

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


STUDENT_MOBILITY_COLUMNS = list(StudentMobility.to_schema().columns)
