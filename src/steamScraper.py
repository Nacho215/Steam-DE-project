# Imports
import pandas as pd
import numpy as np
import requests
import asyncio
import aiohttp
import csv
import boto3
import logging
import logging.config
import sys
import os
import time
# Add path in order to access libs folder
sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from libs.config import settings

# Constants definition
# Parameters
DEBUG_APP_QUANTITY = 1000
DEBUG_START_INDEX = 0
BATCH_SIZE = 10000
WAIT_TIME = 10

# Logging variables
total_apps = 0
total_apps_scraped = 0

# Setup logger
logging.config.fileConfig('config_logs.conf')
logger = logging.getLogger('Scraper')


def get_all_apps_list(api_url: str) -> list:
    """
    Requests Steam API for all app ids.

    Args:
        api_url (list): Steam API all apps url.

    Returns:
        list: List of all App ids and its names.
    """
    # Make request
    with requests.get(api_url) as r:
        # Raise exception if any
        if r.status_code != 200:
            r.raise_for_status()
        # Returns it in JSON format
        json_data = r.json()
        if json_data:
            return json_data["applist"]["apps"]
        else:
            # IF can't scrape return None
            logger.error("Can't scrape apps from Steam")
            return None


def clean_all_apps_list(app_list: list) -> list:
    """
    Drop all apps without name.

    Args:
        app_list (list): List of Steam apps, with appids and names.

    Returns:
        list: Cleaned Steam apps list.
    """
    cleaned_app_list = app_list.copy()
    for app in app_list:
        name = str(app["name"]).strip()
        if name in ['', None, np.NAN]:
            cleaned_app_list.remove(app)
    return cleaned_app_list


def store_all_apps_list(
        app_list: list,
        csv_path: str,
        columns: list,
        s3_info: dict
        ) -> bool:
    """
    Store all apps list in a csv file, and then uploads it to S3.

    Args:
        app_list (str): List of Steam apps.
        csv_path (str): Path where store csv file.
        columns (list): List of columns to filter.
        s3_info (dict): Dictionary with S3 credentials.

    Returns:
        bool: True if stored successfully. False otherwise.
    """
    # Store csv file locally
    try:
        with open(csv_path, "w", newline='', encoding='utf-8') as app_data_file:
            file_writer = csv.DictWriter(
                app_data_file,
                fieldnames=columns,
                extrasaction='ignore'
            )
            file_writer.writeheader()
            file_writer.writerows(app_list)
    except Exception:
        logger.error(f"Can't store results in {csv_path}.", exc_info=True)
        return False
    logger.info(f"App list successfully stored in {csv_path}")
    try:
        # Upload it to s3 bucket
        s3 = boto3.client(
            service_name='s3',
            region_name=s3_info["region"],
            aws_access_key_id=s3_info["access_key"],
            aws_secret_access_key=s3_info["secret"]
        )
        # S3 File Key example: 'raw/steam_app_list.csv'
        s3_file_key = '/'.join(csv_path.split('/')[-2:])
        s3.upload_file(
            Filename=csv_path,
            Bucket=s3_info["bucket"],
            Key=s3_file_key
        )
    except Exception:
        logger.error(
            f"Can't upload results to S3 bucket: {s3_info['bucket']}.",
            exc_info=True
        )
        return False
    logger.info(
        f"All apps list successfully uploaded to S3 bucket: {s3_info['bucket']}"
    )
    return True


def scrape_all_apps_list(
        api_url: str,
        csv_path: str,
        columns: list,
        s3_info: dict
        ) -> bool:
    """
    Scrape all apps id using Steam API,
    store results in a csv file,
    and upload results to S3.

    Args:
        api_url (str): Steam API all apps url.
        csv_path (str): Path where store csv file with scraping results.
        columns (list): List of columns to filter.
        s3_info (dict): Dictionary with S3 credentials.

    Returns:
        bool: True if scraped successfully. False otherwise.
    """
    # Get all apps list from Steam API
    app_list = get_all_apps_list(api_url)
    if not app_list:
        return False
    # Clean app list
    app_list = clean_all_apps_list(app_list)
    # Store it to csv file and uploads to s3
    store_all_apps_list(
        app_list,
        csv_path,
        columns,
        s3_info=s3_info
    )
    return True


async def get_app_details(
        session: aiohttp.ClientSession,
        api_url: str,
        appid: int) -> dict:
    """
    Given an app id, requests SteamSpy API for that app details.

    Args:
        session (aiohttp.ClientSession): Session used for async http requests.
        api_url (str): SteamSpy API app details url.
        appid (int): Steam app ID to get details.

    Returns:
        dict: App details in a JSON format.
    """
    # Build url and make async request
    url = f'{api_url}{appid}'
    async with session.get(url) as r:
        # Raise exception if any
        if r.status != 200:
            r.raise_for_status()
        try:
            # Wait for response and returns it in JSON format
            json_data = await r.json()
            if not json_data:
                raise Exception
            return json_data
        # IF can't scrape this app, return only appid
        except aiohttp.ContentTypeError:
            logger.error(f"JSON decode failed for appid: {appid}.", exc_info=True)
            return {"appid": [str(appid)]}
        except Exception:
            logger.error(f"Can't scrape info for appid: {appid}.", exc_info=True)
            return {"appid": [str(appid)]}


async def collect_all_app_details(
        session: aiohttp.ClientSession,
        api_url: str,
        app_ids_list: list
        ) -> list:
    """
    Given a list of app ids, requests SteamSpy API for those app details.

    Args:
        session (aiohttp.ClientSession): Session used for async http requests.
        api_url (str): SteamSpy API app details url.
        app_ids_list (list): List of Steam app IDs to get details.

    Returns:
        list: A list of dictionaries, containing app details.
    """
    # Create an async task for each app in the list
    tasks = []
    for app_id in app_ids_list:
        task = asyncio.create_task(
            get_app_details(session, api_url, app_id)
        )
        tasks.append(task)
    # Wait for all tasks and collect results
    results = await asyncio.gather(*tasks)
    # Count apps scraped for logging
    global total_apps_scraped
    total_apps_scraped = total_apps_scraped + len(app_ids_list)
    logger.debug(
        f"scraped {len(app_ids_list)} apps. Progress: {total_apps_scraped}/{total_apps}"
        )
    # Return details for all apps in the list
    return results


def store_app_details(
        app_details: list,
        csv_path: str,
        fieldnames: list
        ) -> bool:
    """
    Store given app details into a csv file with given columns.

    Args:
        app_details (list): A list of dictionaries containing app details.
        csv_path (str): Path for store csv file.
        fieldnames (list): A list of fields to filter.

    Returns:
        bool: True if stored successfully. False otherwise.
    """
    try:
        with open(csv_path, "a", encoding='utf8') as app_data_file:
            file_writer = csv.DictWriter(
                app_data_file,
                fieldnames=fieldnames,
                extrasaction='ignore'
            )
            file_writer.writerows(app_details)
    except Exception:
        logger.error(f"Can't store results in {csv_path}.", exc_info=True)
        return False
    return True


def upload_app_details_to_s3(
        csv_path: str,
        s3_info: dict
) -> str:
    """
    Upload csv to s3 bucket.

    Args:
        csv_path (str): Local path of csv file.
        s3_info (dict): Dictionary with S3 credentials.

    Returns:
        bool: True if uploaded successfully. False otherwise.
    """
    # Upload csv to s3 bucket
    try:
        s3 = boto3.client(
            service_name='s3',
            region_name=s3_info["region"],
            aws_access_key_id=s3_info["access_key"],
            aws_secret_access_key=s3_info["secret"]
        )
        # S3 File Key example: 'raw/steam_app_data.csv'
        s3_file_key = '/'.join(csv_path.split('/')[-2:])
        s3.upload_file(
            Filename=csv_path,
            Bucket=s3_info["bucket"],
            Key=s3_file_key
        )
    except Exception:
        logger.error(
            f"Can't upload results to S3 bucket: {s3_info['bucket']}.",
            exc_info=True
        )
        return False
    logger.info(
        f"App details successfully uploaded to S3 bucket: {s3_info['bucket']}"
    )
    return True


async def scrape_app_details(
        path_app_list_input: str,
        path_app_details_output: str,
        api_url: str,
        columns: list,
        s3_info: dict,
        start_index: int = 0,
        batch_size: int = 1000,
        wait_time: int = 5
        ) -> bool:
    """
    Scrape all games details using SteamSpy API and store
    results in a csv file.

    Args:
        path_app_list_input (str): Path of csv containing all apps ids.
        path_app_details_output (str): Path where store results.
        api_url (str): SteamSpy API app details url.
        columns (list): A list of fields to filter.
        s3_info (dict): Dictionary with S3 credentials.
        start_index (int, optional): Index to start scraping.
            Defaults to 0.
        batch_size (int, optional): Size of the batch to scrape.
            Defaults to 1000.
        wait_time (int, optional): Seconds to wait between requests.
            Defaults to 5.

    Returns:
        bool: True if scraped successfully. False otherwise.
    """
    try:
        # Prepare csv file
        with open(path_app_details_output, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
        # Get app ids from csv and make it a list
        app_list = pd.read_csv(path_app_list_input)
        app_list = list(app_list["appid"])

        # Calculate total apps to scrape for logging
        global total_apps
        total_apps = len(app_list) - start_index

        # Initialize empty list
        app_details = []
        # Divide all app list into batches
        batch_limits = np.arange(start_index, len(app_list), batch_size)
        for i in batch_limits:
            # Get and store app details for each batch
            batch = app_list[i: i+batch_size]
            async with aiohttp.ClientSession() as session:
                app_details = await collect_all_app_details(session, api_url, batch)
            store_app_details(app_details, path_app_details_output, columns)
            # Wait between requests to avoid overloading the API
            if i < batch_limits[-1]:
                logger.debug(f"Waiting {wait_time} seconds...")
                time.sleep(wait_time)
    except Exception:
        logger.error("Scraping app details failed.", exc_info=True)
        return False
    logger.info("All app details scraped successfully!")
    # When the csv is already stored, upload it to s3
    upload_results = upload_app_details_to_s3(
        csv_path=path_app_details_output,
        s3_info=s3_info
    )
    return upload_results


def run_scraping_process(
        api_all_apps_list_url: str,
        csv_path_all_apps_list: str,
        all_apps_list_columns: list,
        api_app_details_url: str,
        csv_path_app_details: str,
        app_details_columns: list,
        s3_info: dict
) -> bool:
    """
    Scraping process. Scrape all apps ids from Steam API,
    and then use those ids to scrape app details from SteamSpy API.

    Args:
        api_all_apps_list_url (str): Steam API all apps url.
        csv_path_all_apps_list (str): Path to store csv with all apps ids.
        all_apps_list_columns (list): Columns to retrieve from all apps (appid, name).
        api_app_details_url (str): SteamSpy API app details url.
        csv_path_app_details (str): Path to store csv with app details.
        app_details_columns (list): Columns to retrieve from app details.
        s3_info (dict): S3 credentials to store raw data in a bucket.

    Returns:
        bool: True if scraped sucessfully. False otherwise.
    """
    # Scrape all apps list
    logger.info("Starting scraping process for apps list...")
    result_first_scraping = scrape_all_apps_list(
        api_url=api_all_apps_list_url,
        csv_path=csv_path_all_apps_list,
        columns=all_apps_list_columns,
        s3_info=s3_info
    )
    if not result_first_scraping:
        return False
    logger.info("Starting scraping process for apps details...")
    result_second_scraping = asyncio.run(
        scrape_app_details(
            path_app_list_input=csv_path_all_apps_list,
            path_app_details_output=csv_path_app_details,
            api_url=api_app_details_url,
            columns=app_details_columns,
            start_index=DEBUG_START_INDEX,
            batch_size=BATCH_SIZE,
            wait_time=WAIT_TIME,
            s3_info=s3_info
        )
    )
    if not result_second_scraping:
        return False
    return True
