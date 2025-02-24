from mischooldata_etls import setup_logging

from extract import open_eem
from transform import transform_eem
from validate import validate_eem
from load import load_eem
from archive import archive_eem


logger = setup_logging()

open_eem(logger) # -> ../../tmp/eem_working.csv
transform_eem(logger) # -> ../../tmp/eem_working.parquet (includes geography)
validate_eem(logger) # Pandera checking
load_eem(logger) # ...eem_working.parquet -> database
archive_eem(logger) # ... eem_working.parquet -> vault/ ... somewhere ... / eem_2023_2024_20250218.parquet.gzip
