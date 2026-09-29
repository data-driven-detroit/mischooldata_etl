from ..datasets import materialize_group
from ..logging_setup import setup_logging


def main():
    setup_logging()
    materialize_group("early_childhood")


if __name__ == "__main__":
    main()
