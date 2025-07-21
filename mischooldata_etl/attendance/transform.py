from pathlib import Path
from ..common import generic_transform

WORKING_DIR = Path(__file__).parent


def transform_attendance():
    generic_transform("attendance", WORKING_DIR)


if __name__ == "__main__":
    transform_attendance()
