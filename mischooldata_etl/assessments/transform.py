from pathlib import Path
from ..common import generic_transform

WORKING_DIR = Path(__file__).parent


def transform_assessments():
    generic_transform("assessments", WORKING_DIR)


if __name__ == "__main__":
    transform_assessments()
