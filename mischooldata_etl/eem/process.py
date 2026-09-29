from ..logging_setup import setup_logging
from .transform import transform_eem
from .load import load_eem, load_school_geocode
from .geocode import geocode_schools


def main():
    setup_logging()
    transform_eem()
    geocode_schools()
    load_eem()
    load_school_geocode()


if __name__ == "__main__":
    main()
