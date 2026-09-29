from ..logging_setup import setup_logging
from .transform import transform_college_enrollment
from .load import load_college_enrollment


def main():
    setup_logging()
    transform_college_enrollment()
    load_college_enrollment()


if __name__ == "__main__":
    main()
