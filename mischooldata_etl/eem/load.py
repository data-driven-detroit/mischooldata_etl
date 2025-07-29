from pathlib import Path
import json
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
import tomli


WORKING_DIR = Path(__file__).parent
BASE_DIR = Path(__file__).parent.parent

with open(BASE_DIR / "config.toml", "rb") as f:
    config = tomli.load(f)


db_engine = create_engine(
    f"postgresql+psycopg://{config['db']['user']}:{config['db']['password']}"
    f"@{config['db']['host']}:{config['db']['port']}/{config['db']['name']}",
    connect_args={'options': f'-csearch_path={config["app"]["name"]},public'},
)


def load_eem():
    field_reference = json.loads(
        (WORKING_DIR / "conf" / "field_reference_2010_2025.json").read_text()
    )

    with db_engine.connect() as db:
        if_exists = "replace"
        for i, portion in enumerate(pd.read_csv(
            WORKING_DIR / "output" / "combined_years.csv",
            chunksize=20_000,
            dtype=field_reference["out_types"],
        ), start=1): 
            print(f"Loading chunk {i} into database")

            portion.to_sql( 
                "eem", db, schema="education", if_exists=if_exists, index=False
            )
            if_exists = "append"


def load_school_geocode():
    with db_engine.connect() as db:
        if_exists = "replace"
        for i, portion in enumerate(gpd.read_file(
            WORKING_DIR / "output" / "geocoded_schools.geojson",
            chunksize=10_000, # I don't actually know if this works.
            dtype={
                "building_code": "str",
                "state": "str",
                "county": "str",
                "tract": "str",
                "block": "str",
                "start_date": pd.DatetimeTZDtype(),
                "end_date": pd.DatetimeTZDtype(),
            }
        )):
            print(f"Loading chunk {i} into database")

            portion.to_sql( 
                "school_geocodes", db, schema="education", if_exists=if_exists, index=False
            )
            if_exists = "append"


if __name__ == "__main__":
    load_eem()
