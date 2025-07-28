from mischooldata_etls import setup_logging

from transform import transform_student_mobility
from load import load_student_mobility


logger = setup_logging()

transform_student_mobility(logger)
load_student_mobility(logger)
