from pathlib import Path
import pandas as pd
import geopandas as gpd
from ..db import get_db_engine
from ..pipeline import generic_load


WORKING_DIR = Path(__file__).parent


def load_eem():
    generic_load("eem", WORKING_DIR)


def load_school_geocode():
    with get_db_engine().begin() as db:
        frame = gpd.read_file(
            WORKING_DIR / "output" / "geocoded_schools.geojson",
            dtype={
            }
        ).astype(
            {
                "building_code": pd.Int64Dtype(),
                "state": pd.Int64Dtype(),
                "county": pd.Int64Dtype(),
                "tract": pd.Int64Dtype(),
                "block": pd.Int64Dtype(),
            },
        ).astype(
            {
                "building_code": "str",
                "state": "str",
                "county": "str",
                "tract": "str",
                "block": "str",
            },
        )

        frame["building_code"] = frame["building_code"].str.zfill(5)
        frame["state"] = frame["state"].str.zfill(2)
        frame["county"] = frame["county"].str.zfill(3)
        frame["tract"] = frame["tract"].str.zfill(6)
        frame["block"] = frame["block"].str.zfill(4)

        frame["start_date"] = pd.to_datetime(frame["start_date"]).dt.date
        frame["end_date"] = pd.to_datetime(frame["end_date"]).dt.date

        frame.to_postgis( 
            "school_geocodes", db, schema="education", if_exists="replace", 
             index=False
        )


if __name__ == "__main__":
    load_eem()
