from mischooldata_etls import setup_logging

from extract import extract_non_resident
from transform import transform_non_resident
from load import load_non_resident

logger = setup_logging()

extract_non_resident(logger)
transform_non_resident(logger)
load_non_resident(logger)

