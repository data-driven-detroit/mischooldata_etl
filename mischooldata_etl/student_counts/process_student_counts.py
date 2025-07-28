from mischooldata_etls import setup_logging

from transform import transform_student_counts
from load import load_student_counts


logger = setup_logging()

transform_student_counts(logger)
load_student_counts(logger)
