from pathlib import Path
import json
import logging
import logging.config
from sqlalchemy import create_engine
import tomli

from inequalitytools.inequality import Supressed, Interval, Exact, LessThan

with open(Path().cwd() / "config.toml", "rb") as f:
    config = tomli.load(f)


db_engine = create_engine(
    f"postgresql+psycopg2://{config['db']['user']}:{config['db']['password']}"
    f"@{config['db']['host']}:{config['db']['port']}/{config['db']['name']}",
    connect_args={'options': f'-csearch_path={config["app"]["name"]}'},
)

metadata_engine = create_engine(
    f"postgresql+psycopg2://{config['db']['user']}:{config['db']['password']}"
    f"@{config['db']['host']}:{config['db']['port']}/{config['db']['name']}",
    connect_args={'options': f'-csearch_path={config["db"]["metadata_schema"]}'},
)


def setup_logging():
    with open(Path.cwd() / "logging_config.json") as f:
        logging_config = json.load(f)

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

