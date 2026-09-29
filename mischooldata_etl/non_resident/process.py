from ..logging_setup import setup_logging
from .transform import transform_non_resident
from .load import load_non_resident


def main():
    setup_logging()
    transform_non_resident()
    load_non_resident()


if __name__ == "__main__":
    main()
