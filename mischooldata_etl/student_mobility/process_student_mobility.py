from pathlib import Path
from common import generic_transform, generic_load


if __name__ == "__main__":
    WORKING_DIR = Path(__file__).parent
    generic_transform(WORKING_DIR)
    generic_load("student_mobility", WORKING_DIR)
