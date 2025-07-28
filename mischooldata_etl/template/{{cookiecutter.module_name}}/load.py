from pathlib import Path
from ..common import generic_load


WORKING_DIR = Path(__file__).parent


def load_{{cookiecutter.module_name}}():
    generic_load("{{cookiecutter.module_name}}", WORKING_DIR)


if __name__ == "__main__":
    load_{{cookiecutter.module_name}}()
