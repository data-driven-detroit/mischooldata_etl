import json
from pathlib import Path
import pandas as pd
from pygris.geocode import batch_geocode 


WORKING_DIR = Path(__file__).parent


def geocode_schools():
    field_reference = json.loads(
        (WORKING_DIR / "conf" / "field_reference_2010_2025.json").read_text()
    )

    all_entities = pd.read_csv(
        WORKING_DIR / "output" / "combined_years.csv",
        dtype=field_reference["in_types"]
    )

    schools = all_entities[all_entities["building_code"] != '00000']
    
    geocoded = batch_geocode(schools, id_column="building_code", 
                             address="address", city="city", state="state", 
                             zip="zip_code", as_gdf = True)

    geocoded.rename({
        "id": "building_code" # put this back to the original name
    }).to_file(WORKING_DIR / "output" / "geocoded_schools.geojson")

