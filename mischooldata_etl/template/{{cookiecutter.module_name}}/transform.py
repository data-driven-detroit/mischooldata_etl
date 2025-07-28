from pathlib import Path
from ..common import generic_transform

WORKING_DIR = Path(__file__).parent


def transform_{{cookiecutter.module_name}}():
    generic_transform("{{cookiecutter.module_name}}", WORKING_DIR)


if __name__ == "__main__":
    transform_{{cookiecutter.module_name}}()
