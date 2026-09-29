from pathlib import Path

from ..pipeline import generic_load


WORKING_DIR = Path(__file__).parent


def load_student_mobility():
    generic_load("student_mobility", WORKING_DIR)


if __name__ == "__main__":
    load_student_mobility()
