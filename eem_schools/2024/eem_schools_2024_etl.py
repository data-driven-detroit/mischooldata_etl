from mischooldata_etls import setup_logging

from .extract import open_eem
from .transform import transform_eem
from .validate import validate_eem
from .load import load_eem
from .archive import archive_eem


logger = setup_logging()

# Open file 
open_eem(logger)
transform_eem(logger)
validate_eem(logger)
load_eem()
archive_eem()
