import datetime
import click
import pandas as pd
from sqlalchemy.orm import sessionmaker
import tomli
from inequalitytools import parse_to_inequality
from metadata_audit.capture import record_metadata

def transform(logger):
    # Open the file
    # Renmae the columns
    # Parse suppressed fields to value / error with 'parse_to_inequality'
