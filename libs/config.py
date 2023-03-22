# Imports
import os
from dotenv import load_dotenv

# Locate and load .env file
load_dotenv()


class Settings:
    """
    Class that contains all project settings.
    """
    PROJECT_NAME: str = "STEAM_DE_PROJECT"
    PROJECT_VERSION: str = "1.0"

    # Database config
    # POSTGRES_USER: str = os.getenv('POSTGRES_USER')
    # POSTGRES_DB: str = os.getenv('POSTGRES_DB')
    # POSTGRES_PASSWORD: str = os.getenv('POSTGRES_PASSWORD')
    # POSTGRES_SERVER: str = os.getenv('POSTGRES_SERVER')
    # POSTGRES_PORT: str = os.getenv('POSTGRES_PORT')
    # DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}" + \
    #     f"@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}"

    # # Aws s3 config
    # S3_KEY: str = os.getenv('S3_KEY')
    # S3_SECRET: str = os.getenv('S3_SECRET')
    # S3_CREDENTIALS = {"key": S3_KEY, "secret": S3_SECRET}
    # S3_BUCKET: str = os.getenv('S3_BUCKET')
    # S3_FOLDER_SAVE_CSV: str = os.getenv('S3_FOLDER_SAVE_CSV')
    # S3_FOLDER_SAVE_CSV_PATH = f's3://{S3_BUCKET}/{S3_FOLDER_SAVE_CSV}'
    # S3_DATASET_FOLDER: str = os.getenv('S3_DATASET_FOLDER')
    # S3_DATASET_NAME: str = os.getenv('S3_DATASET_NAME')
    # S3_DATASET_PATH = f'{S3_DATASET_FOLDER}/{S3_DATASET_NAME}'

    # API urls
    API_ALL_APPS_LIST_URL: str = os.getenv('API_ALL_APPS_LIST_URL')
    API_APP_DETAILS_URL: str = os.getenv('API_APP_DETAILS_URL')

    # Local paths
    OUTPUT_FOLDER: str = os.getenv('OUTPUT_FOLDER')
    LOGS_FOLDER: str = os.getenv('LOGS_FOLDER')
    LOGS_CONFIG_FILE_PATH: str = os.getenv('LOGS_CONFIG_FILE_PATH')


# Reference to class
settings = Settings()
