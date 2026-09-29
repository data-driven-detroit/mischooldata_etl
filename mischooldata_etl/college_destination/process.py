from ..datasets import materialize_group
from ..logging_setup import setup_logging


def main():
    setup_logging()
    materialize_group("college_destination")


if __name__ == "__main__":
    main()
