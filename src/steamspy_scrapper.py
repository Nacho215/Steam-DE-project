import pandas as pd
import numpy as np
import asyncio
import aiohttp
import csv
import time

# Constants
OUTPUT_FOLDER = "output"
PATH_CSV_APP_LIST = OUTPUT_FOLDER + "/app_list.csv"
PATH_CSV_APP_DATA = OUTPUT_FOLDER + "/steam_app_data.csv"
DEBUG_APP_QUANTITY = 1000
DEBUG_START_INDEX = 27500
API_BASE_URL = 'https://steamspy.com/api.php'
BATCH_SIZE = 1000
WAIT_TIME = 5

STEAMSPY_COLUMNS = [
        'appid', 'name', 'developer', 'publisher', 'score_rank', 'owners',
        'average_forever', 'average_2weeks', 'median_forever', 'median_2weeks',
        'ccu', 'price', 'initialprice', 'discount', 'tags', 'languages', 'genre'
    ]

# Logging variables
total_apps = 0
total_apps_scrapped = 0


async def get_app_details(session, appid):
    """
    Given an app id, requests SteamSpy API for that app details.

    Args:
        session (aiohttp.ClientSession): Session used for async http requests.
        appid (int): Steam app ID to get details.

    Returns:
        dict: App details in a JSON format.
    """
    # Build url and
    url = f'{API_BASE_URL}?request=appdetails&appid={appid}'
    async with session.get(url) as r:
        if r.status != 200:
            r.raise_for_status()
        json_data = await r.json()
        if json_data:
            return json_data
        else:
            print(f"Can't scrape info for appid: {appid}")
            return {"appid": [str(appid)]}


async def collect_all_app_details(session, app_ids_list):
    tasks = []
    for app_id in app_ids_list:
        task = asyncio.create_task(get_app_details(session, app_id))
        tasks.append(task)
    res = await asyncio.gather(*tasks)
    # Remember apps scrapped
    global total_apps_scrapped
    total_apps_scrapped = total_apps_scrapped + len(app_ids_list)
    print(f"Scrapped {len(app_ids_list)} apps. Progress: {total_apps_scrapped}/{total_apps}")
    return res


def store_app_details(app_details, csv_path, fieldnames):
    with open(csv_path, "a", encoding='utf8') as app_data_file:
        # get dictionary keys for the CSV header
        file_writer = csv.DictWriter(
            app_data_file, fieldnames=fieldnames, extrasaction='ignore')
        file_writer.writerows(app_details)


async def main():

    # # Prepare csv file
    # with open(PATH_CSV_APP_DATA, 'w', newline='') as f:
    #     writer = csv.DictWriter(f, fieldnames=steam_columns)
    #     writer.writeheader()

    # Get app ids from csv
    app_list = pd.read_csv(PATH_CSV_APP_LIST)

    # Get only appids and make it a list
    app_list = list(app_list["appid"])

    # TEST. Get only some apps for debug
    # app_list = app_list[0:DEBUG_APP_QUANTITY]

    # Remember total apps to scrape
    global total_apps
    total_apps = len(app_list) - DEBUG_START_INDEX

    game_details = []

    batch_limits = np.arange(DEBUG_START_INDEX, len(app_list), BATCH_SIZE)

    for i in batch_limits:
        batch = app_list[i: i+BATCH_SIZE]
        async with aiohttp.ClientSession() as session:
            game_details = await collect_all_app_details(session, batch)
        store_app_details(game_details, PATH_CSV_APP_DATA, STEAMSPY_COLUMNS)
        # Don't wait at the end
        if i < batch_limits[-1]:
            print(f"Waiting {WAIT_TIME} seconds...")
            time.sleep(WAIT_TIME)


if __name__ == '__main__':
    start = time.perf_counter()
    asyncio.run(main())
    stop = time.perf_counter()
    print("time taken:", stop - start)
