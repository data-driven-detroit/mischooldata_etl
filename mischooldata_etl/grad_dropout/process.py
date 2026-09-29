from ..datasets import DATASETS
from ..logging_setup import setup_logging


def main():
    setup_logging()
    DATASETS["grad_dropout"].run()


if __name__ == "__main__":
    main()
