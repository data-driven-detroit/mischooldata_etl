from mischooldata_etls import setup_logging

from extract import extract_student_mobility
from transform import transform_student_mobility
from load import load_student_mobility


logger = setup_logging()

extract_student_mobility(logger)
transform_student_mobility(logger)
load_student_mobility(logger)
