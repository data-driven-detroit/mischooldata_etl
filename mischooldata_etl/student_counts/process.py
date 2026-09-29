from ..logging_setup import setup_logging
from .transform import transform_student_counts
from .load import load_student_counts


def main():
    setup_logging()
    transform_student_counts()
    load_student_counts()


if __name__ == "__main__":
    main()
