import json
from functools import lru_cache
from pathlib import Path
import geopandas as gpd
import pandas as pd
from pygris.geocode import batch_geocode


WORKING_DIR = Path(__file__).parent


def geocode_schools():
    output_file = WORKING_DIR / "output" / "geocoded_schools.geojson"
    
    if output_file.exists():
        print(f"Geocoded file already completed at {str(output_file)}. Delete this "
              "if you'd like to re-run the geocode.")
        return

    field_reference = json.loads(
        (WORKING_DIR / "conf" / "field_reference_2010_2025.json").read_text()
    )
    
    result = []
    for i, chunk in enumerate(pd.read_csv(
        WORKING_DIR / "output" / "combined_years.csv",
        dtype=field_reference["in_types"],
        chunksize=5000
    ), start=1):
        print(f"Processing chunk {i}.")
        
        ID_COLUMN = "building_code"


        # TODO: Need to cache this, because I'm probablly pulling the same thing a bunch of times.

        schools = chunk[chunk["building_code"] != "00000"]
        geocoded = batch_geocode(schools, id_column=ID_COLUMN,
                                 address="address", city="city", state="state", 
                                 zip="zip_code")
        
        # Columns returned
        # - id
        # - address
        # - status
        # - match_quality
        # - match_address
        # - tiger_line_id
        # - tiger_side
        # - state
        # - county
        # - tract
        # - block
        # - logitude
        # - latitude

        dated = (
            chunk[[ID_COLUMN, "start_date", "end_date"]]
            .merge(geocoded.rename(columns={"id": ID_COLUMN}),  on=ID_COLUMN)
            [ID_COLUMN, geocoded.columns.drop("id"), "start_date", "end_date"]
        )
        
        result.append(dated)
    
    collected = pd.concat(result)
    gdf = gpd.GeoDataFrame(collected, geometry="geometry")
    gdf.to_file(output_file, index=False)
