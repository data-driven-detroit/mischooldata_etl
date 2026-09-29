from ..logging_setup import setup_logging
from .transform import transform_{{cookiecutter.module_name}}
from .load import load_{{cookiecutter.module_name}}


def main():
    setup_logging()
    transform_{{cookiecutter.module_name}}()
    load_{{cookiecutter.module_name}}()


if __name__ == "__main__":
    main()
