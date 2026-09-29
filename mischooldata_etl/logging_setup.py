"""Logging configuration for an ETL run."""
import json
import logging.config
from pathlib import Path


PROJECT_DIR = Path(__file__).parent.parent


def setup_logging(force=False):
    """Configure logging from logging_config.json.

    Call this once, from a module's `process.py`. Everywhere else declare a
    module-level `logger = logging.getLogger(__name__)` -- there's no need to
    pass a logger through function arguments.

    Does nothing if the root logger already has handlers, so that running under
    an orchestrator that owns logging (Dagster, Airflow) doesn't get its
    configuration replaced. Pass force=True to configure anyway.
    """
    if not force and logging.getLogger().handlers:
        return

    config = json.loads((PROJECT_DIR / "logging_config.json").read_text())

    # Keep the log file with the project rather than wherever the run was
    # launched from.
    file_handler = config.get("handlers", {}).get("file")
    if file_handler is not None and not Path(file_handler["filename"]).is_absolute():
        file_handler["filename"] = str(PROJECT_DIR / file_handler["filename"])

    logging.config.dictConfig(config)
