from datetime import date
import pandera as pa
from pandera.typing import Series


class EEMDistricts(pa.DataFrameModel):
    """
    Same as EEMSchools... but no building name or code
    """


class DistrictAttendance(pa.DataFrameModel):
    pass


class SchoolAssessments(pa.DataFrameModel):
    pass


class DistrictAssessments(pa.DataFrameModel):
    pass


class SchoolFreeReducedLunch(pa.DataFrameModel):
    pass


class KindergardenCount(pa.DataFrameModel):
    pass


class NonResidentStatus(pa.DataFrameModel):
    pass


