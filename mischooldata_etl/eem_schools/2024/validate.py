"""
The validate script uses the pandera schema found ing eem_schema.py
"""
from pathlib import Path
from datetime import date
import geopandas as gpd
import pandera as pa
from pandera.typing import Series
from pandera.typing.geopandas import GeoSeries


EEM_DIR = Path(__file__).resolve().parent.parent

class EEMSchools(pa.DataFrameModel):
    """
    2025-02-12 -- This table exists already so I'm going to adjust
    the columns to match these (removing school_year, adding start_date
    and end_date)
    """

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
    school_emphasis: str = pa.Field()
    setting: str = pa.Field(nullable=True)
    email_address: str = pa.Field(nullable=True)
    phone_number: str = pa.Field(nullable=True)
    street_address: str = pa.Field()
    city: str = pa.Field()
    state: str = pa.Field()
    zip_code: str = pa.Field()
    school_type: str = pa.Field(nullable=True)
    status: str = pa.Field()
    matchtype: str = pa.Field(nullable=True)
    block_geoid: str = pa.Field(nullable=True)
    geometry: GeoSeries = pa.Field(nullable=True)
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


def validate_eem(logger):
    logger.info("Validating EEM")

    df = gpd.read_file(EEM_DIR.parent / "tmp" / "eem_working_geocoded.geojson")

    # Validate - This throws a helpful error if it doesn't work.
    validated = EEMSchools.validate(df)

    logger.info("Validation successful!")
