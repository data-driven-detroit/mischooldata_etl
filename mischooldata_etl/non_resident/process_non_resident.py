from mischooldata_etls import setup_logging

from transform import transform_non_resident
from load import load_non_resident

logger = setup_logging()

transform_non_resident(logger)
load_non_resident(logger)

