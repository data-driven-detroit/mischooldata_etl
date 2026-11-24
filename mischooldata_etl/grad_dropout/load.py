from pathlib import Path
from ..pipeline import generic_load


WORKING_DIR = Path(__file__).parent


def load_grad_dropout():
    generic_load("grad_dropout", WORKING_DIR)


if __name__ == "__main__":
    load_grad_dropout()
