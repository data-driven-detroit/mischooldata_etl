from pathlib import Path
from ..pipeline import generic_transform

WORKING_DIR = Path(__file__).parent


def transform_college_readiness():
    generic_transform("college_readiness", WORKING_DIR)


if __name__ == "__main__":
    transform_college_readiness()
