"""Validation schema for education.school_geocodes."""
import geocoder
import pandera.pandas as pa


def school_geocodes_schema() -> pa.DataFrameSchema:
    """One row per building-year: its address, d3geocoder's match columns, a point.

    The match columns (bu_*, lon/lat, geo_*) and geometry come from
    d3geocoder's own schema for `geocode_dataframe` output, so they track
    whatever d3geocoder version is installed.
    """
    return geocoder.dataframe_schema(cache=True, geometry=True, strict=True).add_columns({
        "building_code": pa.Column(
            str, pa.Check.str_length(5, 5), nullable=False, coerce=True
        ),
        "address": pa.Column(str, nullable=False, coerce=True),
        "city": pa.Column(str, nullable=True, coerce=True),
        "state": pa.Column(str, nullable=True, coerce=True),
        "zip_code": pa.Column(str, nullable=True, coerce=True),
        "start_date": pa.Column(pa.Date, nullable=False, coerce=True),
        "end_date": pa.Column(pa.Date, nullable=False, coerce=True),
    })
