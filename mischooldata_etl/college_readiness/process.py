from ..logging_setup import setup_logging
from .transform import transform_college_readiness
from .load import load_college_readiness


def main():
    setup_logging()
    transform_college_readiness()
    load_college_readiness()


if __name__ == "__main__":
    main()
