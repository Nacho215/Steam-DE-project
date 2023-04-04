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

    # AWS S3 config
    AWS_KEY: str = os.getenv('AWS_ACCESS_KEY')
    AWS_SECRET: str = os.getenv('AWS_SECRET_KEY')
    AWS_CREDENTIALS = {"key": AWS_KEY, "secret": AWS_SECRET}
    AWS_REGION: str = os.getenv('AWS_REGION_NAME')
    AWS_S3_BUCKET: str = os.getenv('AWS_S3_BUCKET')

    # API urls
    API_ALL_APPS_LIST_URL: str = os.getenv('API_ALL_APPS_LIST_URL')
    API_APP_DETAILS_URL: str = os.getenv('API_APP_DETAILS_URL')

    # Local paths
    DATASETS_FOLDER: str = os.getenv('DATASETS_FOLDER')
    LOGS_FOLDER: str = os.getenv('LOGS_FOLDER')
    LOGS_CONFIG_FILE_PATH: str = os.getenv('LOGS_CONFIG_FILE_PATH')

    # AWS RDS Postgres config
    POSTGRES_USER: str = os.getenv('POSTGRES_USER')
    POSTGRES_DB: str = os.getenv('POSTGRES_DB')
    POSTGRES_PASSWORD: str = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_SERVER: str = os.getenv('POSTGRES_SERVER')
    POSTGRES_PORT: str = os.getenv('POSTGRES_PORT')
    DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}" + \
        f"@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Reference to class
settings = Settings()
