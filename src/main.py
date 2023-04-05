# Imports
import sys
import os
import time
import logging
import logging.config
import numpy as np
# Add path in order to access libs folder
sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from libs.steamETL import SteamETL
from libs.settings import settings
from libs.db import default_engine as engine

# Setup logger
logging.config.fileConfig('config_logs.conf')
logger = logging.getLogger('ETL')

# Constant definitions
# Local paths
DATASETS_FOLDER = settings.DATASETS_FOLDER
PATH_CSV_ALL_APPS_LIST = DATASETS_FOLDER + "/raw/steam_app_list.csv"
PATH_CSV_APP_DATA_RAW = DATASETS_FOLDER + "/raw/steam_app_data.csv"
PATH_CSV_CLEAN_DATA = DATASETS_FOLDER + "/clean"
# S3 credentials
AWS_KEY = settings.AWS_KEY
AWS_SECRET = settings.AWS_SECRET
AWS_REGION = settings.AWS_REGION
AWS_S3_BUCKET = settings.AWS_S3_BUCKET
S3_INFO = {
    "access_key": AWS_KEY,
    "secret": AWS_SECRET,
    "region": AWS_REGION,
    "bucket": AWS_S3_BUCKET
}
# API urls
API_ALL_APPS_LIST_URL = settings.API_ALL_APPS_LIST_URL
API_APP_DETAILS_URL = settings.API_APP_DETAILS_URL
# Scraping columns
ALL_APPS_LIST_COLUMNS = ['appid', 'name']
STEAMSPY_COLUMNS = [
    'appid', 'name', 'developer', 'publisher', 'owners',
    'average_forever', 'average_2weeks', 'median_forever', 'median_2weeks',
    'ccu', 'price', 'initialprice', 'discount', 'tags', 'languages', 'genre'
]
# (Remove 'score_rank' column because it has too many null values)


def main():
    # Time each process with counters
    counters = []
    counters.append(time.perf_counter())
    # Execute entire ETL process
    etl = SteamETL()
    # EXTRACT
    # etl.extract(
    #     api_all_apps_list_url=API_ALL_APPS_LIST_URL,
    #     csv_path_all_apps_list=PATH_CSV_ALL_APPS_LIST,
    #     all_apps_list_columns=ALL_APPS_LIST_COLUMNS,
    #     api_app_details_url=API_APP_DETAILS_URL,
    #     csv_path_app_details=PATH_CSV_APP_DATA_RAW,
    #     app_details_columns=STEAMSPY_COLUMNS,
    #     s3_info=S3_INFO
    # )
    counters.append(time.perf_counter())
    # TRANSFORM
    # etl.transform(
    #     input_csv_path=PATH_CSV_APP_DATA_RAW,
    #     output_csv_path=PATH_CSV_CLEAN_DATA,
    #     s3_info=S3_INFO
    # )
    counters.append(time.perf_counter())
    # LOAD
    etl.load(
        dir_csv_files=PATH_CSV_CLEAN_DATA,
        s3_info=S3_INFO,
        engine=engine
    )
    counters.append(time.perf_counter())
    # Log each process time taken
    logger.info(
        f"Total Extraction time: {np.round((counters[1] - counters[0]) / 60.0, 2)}m"
    )
    logger.info(
        f"Total Trasnformation time: {np.round((counters[2] - counters[1]) / 60.0, 2)}m"
    )
    logger.info(
        f"Total Loading time: {np.round((counters[3] - counters[2]) / 60.0, 2)}m"
    )
    logger.info(
        f"Total ETL process time: {np.round((counters[3] - counters[0]) / 60.0, 2)}m"
    )
    # Total ETL process time taken: 31 mins


if __name__ == '__main__':
    main()
