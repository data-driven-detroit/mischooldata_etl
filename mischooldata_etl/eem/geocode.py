import json
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
    
    all_geocoded_results = []
    uncached_batch = []
    batch_size = 5000
    
    for i, chunk in enumerate(pd.read_csv(
        WORKING_DIR / "output" / "combined_years.csv",
        dtype=field_reference["in_types"],
        chunksize=batch_size
    ), start=1):
        print(f"Processing chunk {i}.")

        schools = chunk[chunk["building_code"] != '00000']

        # Check which schools need geocoding by comparing with already geocoded results
        if all_geocoded_results:
            # Combine all previous geocoding results for lookup
            previous_geocoded = pd.concat(all_geocoded_results, ignore_index=True)
            
            # Merge to find which schools are already geocoded
            merged = schools.merge(
                previous_geocoded[["address", "city", "state", "zip_code", "status"]], 
                on=["address", "city", "state", "zip_code"], 
                how="left"
            )
            
            # Schools that need geocoding (status is null)
            needs_geocoding = schools[merged["status"].isna()]
        else:
            needs_geocoding = schools
        
        # Add to batch for geocoding
        uncached_batch.append(needs_geocoding)
        total_uncached = sum(len(df) for df in uncached_batch)
        
        # Process batch when we have enough rows or this is the last chunk
        if total_uncached >= batch_size:
            if uncached_batch:
                print(f"Geocoding batch of {total_uncached} schools.")
                
                # Combine all uncached schools
                batch_to_geocode = pd.concat(uncached_batch, ignore_index=True)
                
                # Geocode the batch
                geocoded_batch: gpd.GeoDataFrame = batch_geocode(
                    batch_to_geocode, 
                    id_column="building_code", 
                    address="address", 
                    city="city", 
                    state="state", 
                    zip="zip_code", 
                    as_gdf=True
                )
                
                all_geocoded_results.append(geocoded_batch)
                uncached_batch = []
    
    # Process any remaining uncached schools
    if uncached_batch:
        total_remaining = sum(len(df) for df in uncached_batch)
        if total_remaining > 0:
            print(f"Geocoding final batch of {total_remaining} schools.")
            
            batch_to_geocode = pd.concat(uncached_batch, ignore_index=True)
            geocoded_batch: gpd.GeoDataFrame = batch_geocode(
                batch_to_geocode, 
                id_column="building_code", 
                address="address", 
                city="city", 
                state="state", 
                zip="zip_code", 
                as_gdf=True
            )
            all_geocoded_results.append(geocoded_batch)
    
    # Combine all geocoded results and save
    if all_geocoded_results:
        final_result = pd.concat(all_geocoded_results, ignore_index=True)
        final_result.rename({"id": "building_code"}, axis=1).to_file(output_file)
        print(f"Saved {len(final_result)} geocoded schools to {output_file}")
    else:
        print("No schools found to geocode.")

