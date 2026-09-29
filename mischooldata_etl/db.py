"""Database engine construction."""
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from .config import get_config


def get_db_engine():
    config = get_config()
    return create_engine(
        URL.create(
            "postgresql+psycopg",
            username=config["db"]["user"],
            password=config["db"]["password"],
            host=config["db"]["host"],
            port=config["db"]["port"],
            database=config["db"]["name"],
        ),
        connect_args={'options': f'-csearch_path={config["app"]["name"]},public'},
    )
