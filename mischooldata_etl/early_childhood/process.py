from ..logging_setup import setup_logging
from .transform import transform_early_childhood
from .load import load_early_childhood


def main():
    setup_logging()
    transform_early_childhood()
    load_early_childhood()


if __name__ == "__main__":
    main()
