from pathlib import Path

from ..pipeline import generic_transform


WORKING_DIR = Path(__file__).parent


def transform_student_mobility():
    generic_transform("student_mobility", WORKING_DIR)


if __name__ == "__main__":
    transform_student_mobility()
