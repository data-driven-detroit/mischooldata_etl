from mischooldata_etls import setup_logging

from extract import open_student_counts
from transform import transform_student_counts
from load import load_student_counts


logger = setup_logging()

open_student_counts(logger)
transform_student_counts(logger)
load_student_counts(logger)
