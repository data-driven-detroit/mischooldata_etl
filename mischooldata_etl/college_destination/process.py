from ..logging_setup import setup_logging
from .transform import transform_college_destination
from .load import load_college_destination


def main():
    setup_logging()
    transform_college_destination()
    load_college_destination()


if __name__ == "__main__":
    main()
