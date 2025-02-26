"""
The extract file opens the metadata, finds the appropriate
raw path, then opens the file.
"""
from pathlib import Path
import tomli
import shutil


def open_student_counts(logger):
    logger.info("Opening Student Count from file defined in metadata")

    EDITION_DATE = "2024-06-29"
    SC_DIR = Path(__file__).resolve().parent.parent

    # Open the metadata config
    with open(SC_DIR / "metadata.toml", "rb") as f:
        metadata = tomli.load(f)


    # Edition is available at edition_date in the metadata.toml file
    path = Path(metadata["editions"][EDITION_DATE]["raw_path"])

    # Copy the file to a temp file for working
    tmp_path = SC_DIR.parent / "tmp"

    shutil.copy(path, tmp_path / "student_count_working.csv")
