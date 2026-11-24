from pathlib import Path
from ..pipeline import generic_transform

WORKING_DIR = Path(__file__).parent


def transform_grad_dropout():
    generic_transform("grad_dropout", WORKING_DIR)


if __name__ == "__main__":
    transform_grad_dropout()
