import pandas as pd
import logging
import logging.config
from sqlalchemy import Engine, text, create_engine
from sqlalchemy.exc import ProgrammingError
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from libs.config import settings

# Engine is created to be called as modules from other scripts
default_engine = create_engine(settings.DATABASE_URL)

# Logging config
#logging.config.fileConfig('config_logs.conf')
logger = logging.getLogger('DB')


class DB:
    '''
    Class used to connect to a database, truncate and update tables on it.
    '''
    def truncate_tables(
        tables: list,
        engine: Engine = default_engine
    ) -> bool:
        """
        This methods performs a truncate to the tables.

        Args:
            engine (Engine, optional): Database connection engine.
                Defaults to default_engine.

        Returns:
            bool : True if truncated successfully, False otherwise.
        """
        # Iterate table names list and build the truncate query
        query = "TRUNCATE TABLE "
        for idx, table_name in enumerate(tables):
            query += table_name
            if idx < len(tables) - 1:
                query += ", "
        # Catch and return possible exceptions
        try:
            with engine.connect() as connection:
                connection.execute(text(query))
        except ProgrammingError as e:
            # This error means tables does not exists yet,
            # so we can skip truncate process
            if e.args[0].startswith('(psycopg2.errors.UndefinedTable)'):
                logger.info("Tables does not exists. Skipping truncate process...")
                return True
            # IF it's other error, return False
            logger.error('Truncate tables failed.', exc_info=True)
            return False
        except Exception:
            logger.error('Truncate tables failed.', exc_info=True)
            return False
        logger.info(f"Truncated Tables successfully: '{query}'")
        return True

    def update_tables(
        dir_csv_files: str,
        engine: Engine = default_engine
    ) -> bool:
        """
        Look for csv files in a given directory, load them as DataFrames,
        and upload them to a database as tables, given an Engine.

        Args:
            dir_csv_files (str): Directory with csv files inside.
            engine (Engine, optional):
                Engine used to connect with database. Defaults to default_engine.

        Returns:
            bool: True if updated sucessfully. False otherwise
        """
        try:
            # Look for csv files in given directory
            for filename in os.listdir(dir_csv_files):
                # Full file path
                file_path = f'{dir_csv_files}/{filename}'
                df_name = filename.split('.')[0]
                # Read dataframe and uploads it to database
                df = pd.read_csv(file_path)
                df.to_sql(
                    df_name,
                    engine,
                    if_exists='replace',
                    index=False
                )
                logger.info(f'Table "{df_name}" updated successfully on database.')
        except Exception:
            logger.error("Failed to update tables on database", exc_info=True)
            return False
        logger.info('All tables updated successfully on database!')
        return True

    def execute_query(
        query: str,
        engine=default_engine
    ) -> list:
        """
        Execute given query on database and return results in a list.
        Args:
            query (str): query to execute.
            engine (Engine, optional): Database connection engine.
                Defaults to default_engine.
        Returns:
            list | False: a list of returned rows or False if an error ocurred.
        """
        # Try to execute query, catching possible exceptions
        try:
            with engine.connect() as connection:
                result = connection.execute(text(query))
        except Exception:
            logger.error(f'Failed to execute query:{query}.', exc_info=True)
            return False
        else:
            # If executed successfully, return fetched rows
            logger.info(f'Query executed successfully: {query}')
            return result.fetchall()
