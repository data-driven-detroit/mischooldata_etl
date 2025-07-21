from pathlib import Path
from ..common import generic_load


WORKING_DIR = Path(__file__).parent


def assessments_special_processing(portion):
    portion["test_population"] = portion["test_population"].fillna("NA")
    return portion


def load_assessments():
    generic_load("assessments", WORKING_DIR, assessments_special_processing)


if __name__ == "__main__":
    load_assessments()

