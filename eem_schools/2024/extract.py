"""
The extract file opens the metadata, finds the appropriate
raw path, then opens the file.
"""
from pathlib import Path
import tomli
import shutil


def open_eem(logger):
    logger.info("Opening EEM from file defined in metadata")

    # Open the metadata config
    with open("../metadata.toml", "rb") as f:
        metadata = tomli.load(f)

    edition_date = "2024-07-01"

    # Edition is available at edition_date in the metadata.toml file
    path = Path(metadata["editions"][edition_date])

    # Copy the file to a temp file for working
    shutil.copy(path, f"../../tmp/eem_working.csv")
