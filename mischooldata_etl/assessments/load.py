from pathlib import Path
from ..pipeline import generic_load


WORKING_DIR = Path(__file__).parent


def load_assessments():
    generic_load("assessments", WORKING_DIR)


if __name__ == "__main__":
    load_assessments()
