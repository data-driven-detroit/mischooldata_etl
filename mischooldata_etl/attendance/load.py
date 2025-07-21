from pathlib import Path
from ..common import generic_load


WORKING_DIR = Path(__file__).parent


def load_attendance():
    generic_load("attendance", WORKING_DIR)


if __name__ == "__main__":
    load_attendance()

