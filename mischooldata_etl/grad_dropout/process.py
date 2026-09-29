from ..logging_setup import setup_logging
from .transform import transform_grad_dropout
from .load import load_grad_dropout


def main():
    setup_logging()
    transform_grad_dropout()
    load_grad_dropout()


if __name__ == "__main__":
    main()
