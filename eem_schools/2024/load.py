"""
The load script adds the data to the database.

Check that no rows are getting repeated on key columns: 
    start_date, 
    district_code, 
    building_code

If this fails, for now, do a manual delete from the DB and start with 
the file again. In the future we can figure out how to make this an 
'upsert.'
"""

import pandas as pd
import geopandas as gpd
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from mischooldata_etls import db_engine, metadata_engine


def load_eem(logger):

    SCHEMA = "education"

    # Open working parquet file
    df = gpd.read_file("../../tmp/eem_working_geocoded.parquet")

    # Check for rows that have been already inserted
    prev_q = text("""
        SELECT count(*) AS matches_found
        FROM education.eem_schools
        WHERE (district_code, building_code, start_date) IN :check_against
    """)

    logger.info("Checking for repeated row inserts.")

    check_against = list(
        df[["district_code", "building_code", "start_date"]]
        .itertuples(index=False)
    )

    with db_engine.connect() as db:
        try:
            result = db.execute(prev_q, params={"new": check_against})

            if result.fetchone().matches_found != 0:
                logger.error(
                    "Some rows from current file have conflicting keys on "
                    "'district_code', 'building_code', and 'start_date'"
                )
                
                raise AssertionError("Resolve database-new upload conflicts and try again.")
        
        except ProgrammingError:
            logger.info("First time table is being pushed, so no row-level conflicts.")

    with db_engine.connect() as db:
        df.to_postgis("eem_schools", db, schema=SCHEMA, index=False, if_exists="append")