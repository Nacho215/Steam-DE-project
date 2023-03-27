# Imports
import pandas as pd
import boto3
import json
import os
import sys
import logging
import logging.config
from steamScraper import run_scraping_process
sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from libs.db import DB

# Setup logger
logging.config.fileConfig('config_logs.conf')
logger = logging.getLogger('ETL')

# Table names
table_names = []


class SteamETL:
    def __init__(self) -> None:
        pass

    def extract():
        result = run_scraping_process()
        return result

    def transform(
        self,
        input_csv_path: str,
        output_csv_path: str,
        s3_info: dict
    ) -> bool:
        # download_result = self.download_from_s3(
        #     output_csv_path=input_csv_path,
        #     s3_info=s3_info
        # )
        # if not download_result:
        #     return False

        # Read data from csv
        df_apps = pd.read_csv(input_csv_path)

        # Filter apps without name, developer or publisher
        df_apps = df_apps[~df_apps['name'].isna()]
        df_apps = df_apps[~df_apps['developer'].isna()]
        df_apps = df_apps[~df_apps['publisher'].isna()]

        # Rename id column
        df_apps.rename(columns={"appid": "id_app"}, inplace=True)
        # Transform prices from cents to dollars
        df_apps["price"] = df_apps["price"] / 100.0
        df_apps["initialprice"] = df_apps["initialprice"] / 100.0
        df_apps.rename(
            columns={
                "price": "price_usd",
                "initialprice": "initial_price_usd"
            },
            inplace=True
        )

        # Transform genre column into a list column
        df_apps['genre'] = df_apps['genre'].apply(lambda x: self.transform_genres_column(str(x)))
        # Normalize "genre" column into 2 new tables: "genres" and "apps_genres" (an intermediate table)
        df_apps, df_genres, df_apps_genres = self.normalize_list_column(
            original_df=df_apps,
            id_column='id_app',
            list_column='genre',
            new_table_id_column='id_genre',
            new_table_list_column='genre'
        )

        # Transform languages column into a list column
        df_apps['languages'] = df_apps['languages'].apply(lambda x: self.transform_languages_column(str(x)))
        # Normalize "languages" column into 2 new tables: "languages" and "apps_languages" (an intermediate table)
        df_apps, df_languages, df_apps_languages = self.normalize_list_column(
            original_df=df_apps,
            id_column='id_app',
            list_column='languages',
            new_table_id_column='id_language',
            new_table_list_column='language'
        )

        # Transform tags column into a list column
        df_apps['tags'] = df_apps['tags'].apply(lambda x: self.transform_tags_column(str(x)))
        # Normalize "tags" column into 2 new tables: "tags" and "apps_tags" (an intermediate table)
        df_apps, df_tags, df_apps_tags = self.normalize_json_column(
            original_df=df_apps,
            id_column='id_app',
            json_column='tags',
            new_table_id_column='id_tag',
            new_table_json_column='tag'
        )

        # Store transformed data to csv files
        dataframes_store_info = [
            (df_apps, 'apps'),
            (df_apps_genres, 'apps_genres'),
            (df_genres, 'genres'),
            (df_apps_languages, 'apps_languages'),
            (df_languages, 'languages'),
            (df_apps_tags, 'apps_tags'),
            (df_tags, 'tags')
        ]
        # self.store_transformed_data_locally(
        #     df_list=dataframes_store_info,
        #     output_csv_path=output_csv_path
        # )
        # Save table names
        global table_names
        table_names = [df[1] for df in dataframes_store_info]

    def load(
        self,
        dir_csv_files: str,
        s3_info: dict,
        engine
    ) -> bool:
        # # Upload transformed csv files to S3 bucket
        # upload_result = self.upload_transformed_data_to_s3(
        #     dir_csv_files=dir_csv_files,
        #     s3_info=s3_info
        # )
        # if not upload_result:
        #     return False
        # Truncate tables
        truncate_result = DB.truncate_tables(
            tables=table_names
        )
        if not truncate_result:
            return False
        # Update tables to database
        logger.info('Starting update tables process...')
        update_result = DB.update_tables(
            dir_csv_files=dir_csv_files,
            engine=engine
        )
        return update_result

    def download_from_s3(
        self,
        s3_info: dict,
        output_csv_path: str
    ) -> bool:
        """
        Download dataset from s3 bucket and stores in a given path.

        Args:
            s3_info (dict): S3 credentials.
            output_csv_path (str): Local Path to store dataset.

        Returns:
            _type_: True if downloaded successfully. False otherwise.
        """
        # Download file from s3 bucket
        s3 = boto3.client(
            service_name='s3',
            region_name=s3_info["region"],
            aws_access_key_id=s3_info["access_key"],
            aws_secret_access_key=s3_info["secret"]
        )
        try:
            s3.download_file(
                Bucket=s3_info['bucket'],
                Key=output_csv_path.split('/')[-1],
                Filename=output_csv_path
            )
        except Exception:
            logger.error(
                f"Can't retrieve csv from S3 bucket: {s3_info['bucket']}.",
                exc_info=True
            )
            return False
        logger.info(
            f"Raw app data retrieved successfully from S3 bucket: {s3_info['bucket']} into {output_csv_path}."
        )
        return True

    def normalize_list_column(
        self,
        original_df: pd.DataFrame,
        id_column: str,
        list_column: str,
        new_table_id_column: str,
        new_table_list_column: str
    ) -> tuple:
        """
        Useful method for handling many to many relationship between a table
        and one column containing data in a list format.
        It creates 2 extra tables:
        - one containing unique values for that column,
        - and the other one being an intermediate that links both tables
        Then, deletes the list column from original table.

        Args:
            original_df (pd.DataFrame): DataFrame to normalize.
            id_column (str): ID column from original DataFrame.
            list_column (str): Column containing list values.
            new_table_id_column (str): ID column for the new table.
            new_table_list_column (str): Singular name for the new table list column.
                # E.g: "genres"(list_column) -> "genre"(new_table_list_column)

        Returns:
            tuple: Returns 3 DataFrames in a tuple:
                - Original DataFrame normalized
                - Intermediate table
                - New table
        """
        # Create a copy of original dataframe
        df = original_df.copy()

        # Create new table with unique values from that column
        df_new_table = pd.DataFrame(df.explode(list_column)[list_column])
        df_new_table.rename(columns={list_column: new_table_list_column}, inplace=True)
        df_new_table[new_table_list_column] = df_new_table[new_table_list_column].apply(lambda x: x.strip())
        df_new_table = df_new_table.drop_duplicates(subset=new_table_list_column)
        df_new_table = df_new_table[df_new_table[new_table_list_column] != 'nan']
        df_new_table = df_new_table.reset_index(drop=True).reset_index(names=new_table_id_column)

        # Create new intermediate table that joins two tables
        df_intermediate_table = df.explode(list_column)[[id_column, list_column]].reset_index(drop=True)
        df_intermediate_table = df_intermediate_table[df_intermediate_table[list_column]!='nan']
        df_intermediate_table[list_column] = df_intermediate_table[list_column].apply(lambda x: x.strip())
        df_intermediate_table = df_intermediate_table.merge(
            df_new_table,
            left_on=list_column,
            right_on=new_table_list_column,
            how='left'
        ).drop([list_column, new_table_list_column], axis=1)

        # Drop list column from original table
        df.drop(columns=[list_column], inplace=True)

        # Return 3 dataframes in a tuple
        return df, df_new_table, df_intermediate_table

    def normalize_json_column(
        self,
        original_df: pd.DataFrame,
        id_column: str,
        json_column: str,
        new_table_id_column: str,
        new_table_json_column: str
    ) -> tuple:
        """
        Useful method for handling many to many relationship between a table
        and one column containing data in JSON format.
        It creates 2 extra tables:
        - one containing unique values for that column,
        - and the other one being an intermediate that links both tables
        Then, deletes the json column from original table.

        Args:
            original_df (pd.DataFrame): DataFrame to normalize.
            id_column (str): ID column from original DataFrame.
            json_column (str): Column containing values in JSON format.
            new_table_id_column (str): ID column for the new table.
            new_table_json_column (str): Singular name for the new table json column.
                # E.g: "tags"(json_column) -> "tag"(new_table_json_column)

        Returns:
            tuple: Returns 3 DataFrames in a tuple:
                - Original DataFrame normalized
                - Intermediate table
                - New table
        """
        # Create a copy of original dataframe
        df = original_df.copy()

        # Create new table with unique tags from that column
        df_new_table = pd.DataFrame(df.explode(json_column)[json_column])
        df_new_table.rename(columns={json_column: new_table_json_column}, inplace=True)
        df_new_table.dropna(subset=new_table_json_column, inplace=True)
        df_new_table[new_table_json_column] = df_new_table[new_table_json_column].apply(lambda x: x[0])
        df_new_table.drop_duplicates(subset=new_table_json_column, inplace=True)
        df_new_table = df_new_table.reset_index(drop=True).reset_index(names=new_table_id_column)

        # Create new intermediate table that joins two tables
        cols = [new_table_json_column, 'count']
        df_intermediate_table = df.explode(json_column)[[id_column, json_column]].reset_index(drop=True)
        df_intermediate_table.dropna(subset=json_column, inplace=True)
        df_intermediate_table[cols[0]] = df_intermediate_table[json_column].apply(lambda x: x[0])
        df_intermediate_table[cols[1]] = df_intermediate_table[json_column].apply(lambda x: x[1])
        df_intermediate_table.drop(columns=json_column, inplace=True)
        df_intermediate_table = df_intermediate_table.merge(
            df_new_table,
            left_on=cols[0],
            right_on=new_table_json_column,
            how='left'
        ).drop(cols[0], axis=1)
        # Reorder columns
        df_intermediate_table = df_intermediate_table[[id_column, new_table_id_column, cols[1]]]

        # Drop list column from original table
        df.drop(columns=[json_column], inplace=True)

        # Return 3 dataframes in a tuple
        return df, df_new_table, df_intermediate_table

    def transform_genres_column(
        self,
        col: str
    ) -> list:
        """
        Strip and split genres column.

        Args:
            col (str): A string column with values in a list format.

        Returns:
            list: A list of clean genres.
        """
        # Make column a list and remove trailing spaces
        genres = col.split(',')
        genres = [genre.strip() for genre in genres]
        return genres

    def transform_languages_column(
        self,
        col: str
    ) -> list:
        """
        Split and clean languages column.

        Args:
            col (str): A string column with values in a list format.

        Returns:
            list: A list of clean languages.
        """
        # Chars to remove
        invalid_chars = ['strong', 'amp', '*', '&', 'lt;', 'gt;', ';', r'\/', '/', 'br']

        # Make column a list
        languages = col.split(',')
        for idx, lang in enumerate(languages):
            # Remove invalid chars
            for char in invalid_chars:
                lang = lang.replace(char, '')
            # Replace \r for ' - ' for languages like
            # 'English\nInterface: English\nFull Audio: Flemish\nSubtitles'
            lang = lang.replace('\r\n', ' - ')
            # Remove whitespaces
            lang = lang.strip()
            # Reflect changes in the list
            languages[idx] = lang

        # Filter random Steam languages like '#lang_#lang_spanish*#lang_full_audio'
        for lang in languages:
            if lang.startswith('#'):
                languages.remove(lang)

        return languages

    def transform_tags_column(
        self,
        col: str
    ) -> list:
        """
        Split and clean tags column.

        Args:
            col (str): A string column with values in a JSON format.

        Returns:
            list: A list of clean genres represented in tuples.
        """
        # Return empty list if no tags
        if not col:
            return []
        # Remove decades quotes (1990's)
        col = col.replace("0'", "0")
        # Remove quotes from abbreviations (Shoot 'Em Up)
        col = col.replace("'Em Up", " Em Up")
        # Swap ' for " if JSON is in a bad format
        col = col.replace("{'", '{"')
        col = col.replace("':", '":')
        col = col.replace(", '", ', "')
        # Transform dict into list of tuples
        list_tags = []
        tags_data = dict(json.loads(col))
        for tag, count in tags_data.items():
            list_tags.append((tag, count))
        return list_tags

    def store_transformed_data_locally(
        self,
        df_list: list,
        output_csv_path: str
    ) -> bool:
        """
        Store transformed data in csv files.

        Args:
            df_list (list): A list of tuples containing dataframes to store an their names.
            output_csv_path (str): Path for store csv files.

        Returns:
            bool: True if stored successfully. False otherwise.
        """
        # Store csv files with names given in df_list
        try:
            for df, name in df_list:
                path = f"{output_csv_path}/{name}.csv"
                df.to_csv(
                    path,
                    index=False,
                    encoding='utf-8'
                )
        except Exception:
            logger.error(f"Can't store results in {path}.", exc_info=True)
            return False
        return True

    def upload_transformed_data_to_s3(
        self,
        dir_csv_files: str,
        s3_info: dict
    ) -> bool:
        """
        Upload csv files to s3 bucket.

        Args:
            dir_csv_files (str): Local directory of transformed csv files.
            s3_info (dict): Dictionary with S3 credentials.

        Returns:
            bool: True if uploaded successfully. False otherwise.
        """
        # Upload all csvs to s3 bucket
        try:
            s3 = boto3.client(
                service_name='s3',
                region_name=s3_info["region"],
                aws_access_key_id=s3_info["access_key"],
                aws_secret_access_key=s3_info["secret"]
            )
            for filename in os.listdir(dir_csv_files):
                # Full file path
                file_path = f'{dir_csv_files}/{filename}'
                # S3 File Key example: 'clean/apps.csv'
                clean_dir = dir_csv_files.split('/')[-1]
                s3_file_key = "/".join([clean_dir, filename])
                s3.upload_file(
                    Filename=file_path,
                    Bucket=s3_info["bucket"],
                    Key=s3_file_key
                )
                # Log
                logger.info(
                    f"{s3_info['file_key']} successfully uploaded to S3 bucket: {s3_info['bucket']}"
                )
        except Exception:
            logger.error(
                f"Can't upload results to S3 bucket: {s3_info['bucket']}.",
                exc_info=True
            )
            return False
        logger.info(
            f"Transformed data successfully uploaded to S3 bucket: {s3_info['bucket']}"
        )
        return True
