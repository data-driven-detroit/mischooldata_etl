from pathlib import Path

from ..logging_setup import setup_logging
from ..pipeline import generic_transform, generic_load


WORKING_DIR = Path(__file__).parent


def main():
    setup_logging()
    generic_transform("student_mobility", WORKING_DIR)
    generic_load("student_mobility", WORKING_DIR)


if __name__ == "__main__":
    main()
