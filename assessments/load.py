from pathlib import Path
import pandas as pd
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


def load_assessments():
    with db_engine.connect() as db:
        if_exists = "replace"
        for i, portion in enumerate(pd.read_csv(
            WORKING_DIR / "output" / "combined_years.csv",
            chunksize=5_000
        ), start=1): 
            print(f"Loading chunk {i} into database.")
            portion.to_sql( 
                "assessments", db, schema="education", if_exists=if_exists
            )
            if_exists = "append"


if __name__ == "__main__":
    load_assessments()

