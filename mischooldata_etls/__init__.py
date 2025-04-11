from pathlib import Path
import json
import logging
import logging.config
from sqlalchemy import create_engine
import tomli

from inequalitytools.inequality import Supressed, Interval, Exact, LessThan


BASE_DIR = Path(__file__).parent.parent

with open(BASE_DIR / "config.toml", "rb") as f:
    config = tomli.load(f)


db_engine = create_engine(
    f"postgresql+psycopg2://{config['db']['user']}:{config['db']['password']}"
    f"@{config['db']['host']}:{config['db']['port']}/{config['db']['name']}",
    connect_args={'options': f'-csearch_path={config["app"]["name"]},public'},
)

metadata_engine = create_engine(
    f"postgresql+psycopg2://{config['db']['user']}:{config['db']['password']}"
    f"@{config['db']['host']}:{config['db']['port']}/{config['db']['name']}",
    connect_args={'options': f'-csearch_path={config["db"]["metadata_schema"]},public'},
)


def setup_logging():
    with open(BASE_DIR / "logging_config.json") as f:
        logging_config = json.load(f)

    logging_config["handlers"]["file"]["filename"] = str(BASE_DIR / "logs" / "mischooldata_etls.log") 
    logging.config.dictConfig(logging_config)

    return logging.getLogger(config["app"]["name"])


def unwrap_value(inequality) -> float:
    return inequality.unwrap()


def unwrap_error(inequality) -> float:
    match inequality:
        case Supressed():
            return 0
        case LessThan(_inner_value=inner):
            return inner / 2
        case Interval(_inner_value=_, delta=delta):
            return delta
        case Exact(_inner_value=_):
            return 0
        case float() | int():
            return 0
        case _:
            raise TypeError(f"Object of type {type(inequality)} cannot be unwrapped.")

