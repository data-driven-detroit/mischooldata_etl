from pathlib import Path
from ..pipeline import generic_load


WORKING_DIR = Path(__file__).parent


def load_early_childhood():
    generic_load("early_childhood", WORKING_DIR)


if __name__ == "__main__":
    load_early_childhood()
