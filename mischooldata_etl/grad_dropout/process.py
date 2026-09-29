from ..datasets import materialize_group
from ..logging_setup import setup_logging


def main():
    setup_logging()
    materialize_group("grad_dropout")


if __name__ == "__main__":
    main()
