"""Geocode every school building in the eem directory with d3geocoder.

Each distinct building address is geocoded once, however many years list it,
and the points are joined back to every building-year. Detroit addresses go
to the City's Base Units; everything else falls through to the
known-addresses cache and then the Census geocoder, whose answers are cached,
so later runs only send Census the addresses it hasn't seen.
"""
import logging
from dataclasses import replace
from pathlib import Path

import geocoder
import geopandas as gpd
import pandas as pd
from psycopg.conninfo import make_conninfo

from ..config import get_config


logger = logging.getLogger(__name__)


WORKING_DIR = Path(__file__).parent
OUTPUT_FILE = WORKING_DIR / "output" / "geocoded_schools.geojson"

# What identifies an address; buildings sharing all four share one lookup.
ADDRESS_COLUMNS = ["address", "city", "state", "zip_code"]

# District offices and other non-building rows carry building code 00000.
NOT_A_BUILDING = "00000"


def geocoder_settings() -> geocoder.Settings:
    """d3geocoder settings, connected to this project's warehouse.

    The connection always comes from [db] in config.toml, so the geocoder
    writes to the same database as the rest of the ETL. Everything else --
    which schemas hold Base Units, job tables and the cache, the cache table
    names, Census options -- is d3geocoder's own configuration: GEOCODER_*
    environment variables, or a geocoder.toml (GEOCODER_CONFIG names one
    explicitly), else d3geocoder's defaults.

    make_conninfo quotes each value, so passwords containing spaces, quotes or
    `@` survive.
    """
    db = get_config()["db"]
    dsn = make_conninfo(
        host=db["host"],
        port=db["port"],
        dbname=db["name"],
        user=db["user"],
        password=db["password"],
    )
    settings = geocoder.load_settings()
    return replace(settings, postgres=replace(settings.postgres, dsn=dsn))


def geocode_schools():
    eem = pd.read_csv(
        WORKING_DIR / "output" / "combined_years.csv",
        usecols=["building_code", *ADDRESS_COLUMNS, "start_date", "end_date"],
        dtype=str,  # keep codes and ZIPs as text, leading zeros included
    )
    buildings = eem[(eem["building_code"] != NOT_A_BUILDING) & eem["address"].notna()]
    addresses = buildings[ADDRESS_COLUMNS].drop_duplicates().reset_index(drop=True)
    logger.info(
        "Geocoding %s distinct addresses for %s building-years.",
        len(addresses), len(buildings),
    )

    matched = geocoder.geocode_dataframe(
        addresses,
        address_col="address",
        city_col="city",
        zip_col="zip_code",
        census=True,
        settings=geocoder_settings(),
    )
    logger.info("Geocoder results: %s", matched.attrs.get("geocoder_fallback"))

    match_columns = list(geocoder.RESULT_COLUMNS)
    collected = buildings.merge(
        matched[ADDRESS_COLUMNS + match_columns], on=ADDRESS_COLUMNS, how="left",
        validate="many_to_one",
    )
    unresolved = collected["lon"].isna().sum()
    if unresolved:
        logger.warning("%s of %s building-years have no point.", unresolved, len(collected))

    points = gpd.points_from_xy(collected["lon"], collected["lat"])
    geometry = gpd.GeoSeries(points, index=collected.index, crs="EPSG:4326")
    # points_from_xy makes POINT (NaN NaN) for missing coordinates; make those null.
    gdf = gpd.GeoDataFrame(
        collected, geometry=geometry.where(collected["lon"].notna()), crs="EPSG:4326"
    )
    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    gdf.to_file(OUTPUT_FILE, driver="GeoJSON")
