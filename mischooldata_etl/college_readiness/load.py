from pathlib import Path
import json
import pandas as pd
from ..db import get_db_engine


WORKING_DIR = Path(__file__).parent


def load_college_readiness():
    field_reference = json.loads(
        (WORKING_DIR / "conf" / "field_reference_001.json").read_text()
    )

    with get_db_engine().begin() as db:
        if_exists = "replace"
        for i, portion in enumerate(pd.read_csv(
            WORKING_DIR / "output" / "combined_years.csv",
            chunksize=20_000,
            dtype=field_reference["out_types"],
        ), start=1): 
            print(f"Loading chunk {i} into database.")

            portion.to_sql( 
                "college_readiness", db, schema="education", if_exists=if_exists, index=False
            )
            if_exists = "append"


if __name__ == "__main__":
    load_college_readiness()

