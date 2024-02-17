import requests
import os

import polars as pl

from config.constants import RAW_DATA_PATH


url = 'https://aqs.epa.gov/data/api/monitors/byCBSA?email=test@aqs.api&key=test&param=42602&bdate=20170101&edate=20170101&cbsa=16740'