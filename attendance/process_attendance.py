from mischooldata_etls import setup_logging

from extract import extract_attendance
from transform import transform_attendance
from load import load_attendance


logger = setup_logging()

