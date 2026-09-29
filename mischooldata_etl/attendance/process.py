from ..logging_setup import setup_logging
from .transform import transform_attendance
from .load import load_attendance


def main():
    setup_logging()
    transform_attendance()
    load_attendance()


if __name__ == "__main__":
    main()
