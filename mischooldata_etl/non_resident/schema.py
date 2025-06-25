from datetime import date
import pandera as pa
import pandas as pd


class NonResident(pa.DataFrameModel):
    operating_district_code: str = pa.Field(nullable=False)
    resident_district_code: str = pa.Field(nullable=False)
    grade_code: str = pa.Field()
    student_residency: str = pa.Field(nullable=True)
    student_count: pd.Int64Dtype = pa.Field(coerce=True)
    student_fte_count: pd.Float64Dtype = pa.Field(coerce=True)
    start_date: date = pa.Field(coerce=True)
    end_date: date = pa.Field(coerce=True)

    class Config:
        strict=True
        unique=[
            "operating_district_code",
            "resident_district_code",
            "grade_code",
            "student_residency",
            "start_date",
            "end_date",
        ]


NON_RESIDENT_COLUMNS = list(NonResident.to_schema().columns)
