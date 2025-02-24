from mischooldata_etls import setup_logging

from extract import open_student_counts
from transform import transform_student_counts
from validate import validate_student_counts
from load import load_student_counts
from archive import archive_student_counts


logger = setup_logging()

open_student_counts(logger) # -> ../../tmp/student_counts_working.csv
transform_student_counts(logger) # -> ../../tmp/student_counts_working.parquet (includes geography)
validate_student_counts(logger) # Pandera checking
load_student_counts(logger) # ...student_counts_working.parquet -> database
archive_student_counts(logger) # ... student_counts_working.parquet -> vault/ ... somewhere ... / student_counts_2023_2024_20250218.parquet.gzip
