from pathlib import Path
import json
import pandas as pd
import tomli
from inequalitytools import parse_to_inequality
from metadata_audit.capture import record_metadata


ATTENDANCE_DIR = Path(__file__).resolve().parent.parent
EDITION_DATE = "2024-06-30"


with open(ATTENDANCE_DIR / "metadata.toml", "rb") as md:
    metadata = tomli.load(md)


category_maps = json.loads(
    (ATTENDANCE_DIR / "2024" / "conf" / "category_maps.json").read_text()
)
subgroup_maps = json.loads(
    (ATTENDANCE_DIR / "2024" / "conf" / "subgroup_maps.json").read_text()
)


def transform_attendance(logger):
    logger.info("Opening attendance CSV.")
    df = pd.read_csv(ATTENDANCE_DIR.parent / "tmp" / "attendance_working.csv")

    result = (
        df.rename()
    )