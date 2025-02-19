import json
import pathlib as Path
import datetime
import itertools
import geopandas as gpd
import pandas as pd
import tomli
from sqlalchemy import text

import censusgeocode as cg
from mischooldata_etls import db_engine


EDITION_DATE = "2024-07-01"

field_reference = json.loads(
    (Path.cwd() / "conf" / "field_reference.json").read_text()
)


def clean_fields(df):
    """
    Mostly renames following the config file, then makes sure the 
    various codes are the right length.
    """
    return (
        df
        .rename(columns=field_reference["rename"])
        .assign(
            isd_code=lambda df: df["__isd_code"].str.zfill(2),
            district_code=lambda df: df["__district_code"].str.zfill(5),
            building_code=lambda df: df["__building_code"].str.zfill(5),
        )
        .query("(district_code != '00000') & (building_code != '00000')") # Remove summary rows
        .drop(field_reference["tmp_variables"])
    )


def chunk_iterable(iterable, chunk_size=500):
    """
    Break up a single long iterable into chunks of chunk_size.
    """
    iterable = iter(iterable)

    while chunk := list(itertools.islice(iterable, chunk_size)):
        yield chunk


def pull_previous(ungeocoded, logger):
    """
    This goes to the table and tries to find address & building code 
    matches to avoid needless geocoding.
    """

    # The '00000's have been cleared above
    code_addr_pairs = (
        ungeocoded[["building_code", "street_address"]]
        .itertuples(index=False)
    )

    logger.info(f"Checking for previous geocode matches on {len(code_addr_pairs)} buildings.")

    prev_q = text(
        """
        SELECT DISTINCT ON (building_code, street_address) *
        FROM education.eem_schools
        WHERE (building_code, street_address) IN :batch
        """
    )

    found = []
    with db_engine.connect() as db:
        # This is chunked to help performance -- this may be too much
        for batch in chunk_iterable(code_addr_pairs):
            batch_match = gpd.read_postgis(prev_q, db, params={"batch": tuple(batch)})

            found.append(batch_match)

    logger.info(f"Found matches for {len(found)} buildings.")

    # Return all the rows that you've found as one df
    return pd.concat(found)


def geocode(df, logger):
    """
    Geocodes addresses from the dataframe.
    """
    MAX_BATCH_SIZE = 5_000 # 10_000 is max for API but this runs a little faster overall

    to_geocode = (
        df[[
            "building_code",
            "street_address",
            "city",
            "state",
            "zip_code"
        ]]
        .rename(columns={
            "building_code": "id",
            "street_address": "address",
        })
        .to_dict(orient="records")
    )


    num_calls = (len(to_geocode) // MAX_BATCH_SIZE) + 1
    responses = []
    for i, r in enumerate(chunk_iterable(to_geocode, MAX_BATCH_SIZE)):
        logger.info(f"Running request {i+1}/{num_calls} to Census Geocoder")
        response = cg.addressbatch(r) # Returns a list of dictionaries

        responses.extend(response)

    return gpd.GeoDataFrame(responses, geometry="geom", crs="EPSG:4326")


def transform_eem(logger):
    logger.info("Transformation on EEM started.")

    with open("metadata.toml", "rb") as md:
        metadata = tomli.load(md)

    edition = metadata["editions"][EDITION_DATE]

    df = pd.read_csv("../../tmp/eem_working.csv")

    # Main clean-up
    cleaned_fields = clean_fields(df)

    dated = cleaned_fields.assign(
        start_date = datetime.date.fromisoformat(edition["start"]),
        end_date = datetime.date.fromisoformat(edition["end"]),
    )

    # Geocoding eem (referencing old values)
    previously_geocoded = pull_previous(dated, logger)

    new_buildings = (
        dated.merge(
            previously_geocoded[[
                "building_code",
                "matchtype",
                "geometry"
            ]], 
            on="building_code", 
            how="left", 
            indicator=True
        )
        .query("__merge == 'left_only'")
        .drop(columns=["__merge"])
    )

    new_geocoded = geocode(new_buildings, logger) # This is a gdf

    geocoded = pd.concat(
        [
            previously_geocoded,
            new_geocoded,
        ]
    )

    geocoded.to_file("../../tmp/eem_working_geocoded.parquet")
