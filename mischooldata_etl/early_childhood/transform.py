from pathlib import Path
from ..pipeline import generic_transform

WORKING_DIR = Path(__file__).parent


def transform_early_childhood():
    generic_transform("early_childhood", WORKING_DIR)


if __name__ == "__main__":
    transform_early_childhood()
