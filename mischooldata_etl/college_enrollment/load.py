from pathlib import Path
from ..common import generic_load


WORKING_DIR = Path(__file__).parent


def load_college_enrollment():
    generic_load("college_enrollment", WORKING_DIR)


if __name__ == "__main__":
    load_college_enrollment()
