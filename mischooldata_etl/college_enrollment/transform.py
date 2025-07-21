from pathlib import Path
from ..common import generic_transform

WORKING_DIR = Path(__file__).parent


def transform_college_enrollment():
    generic_transform("college_enrollment", WORKING_DIR)


if __name__ == "__main__":
    transform_college_enrollment()
