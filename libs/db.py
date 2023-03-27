'''

'''
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
logging.config.fileConfig('config_logs.conf')
logger = logging.getLogger('DB')


class DB:
    '''
    This class groups actions or tasks in common
    for the different scripts or classes.
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
        table: str,
        columns=None,
        filters=None,
        engine=default_engine
    ) -> list | Exception:
        """
        Builds a simple query with given parameters,
        executes it and return the results.

        Args:
            table (str): table name
            columns (list): a list with column names. Defaults to None.
            filters (str, optional): filters to apply
                (WHERE clause without the WHERE keyword). Defaults to None.
            engine (Engine, optional): Database connection engine.
                Defaults to default_engine.

        Returns:
            list | Exception: a list of returned rows,
                or captured exception if any.
        """
        # Builds the query with given parameters
        # SELECT
        query = "SELECT "
        if columns:
            for idx, col in enumerate(columns):
                query += col
                if idx < len(columns) - 1:
                    query += ', '
        else:
            query += "* "
        # FROM
        query += f"FROM {table}"
        # WHERE
        if filters:
            query += f" WHERE {filters}"
        query += ";"

        # Try to execute it, catching possible exceptions
        try:
            result = engine.execute(query)
        except Exception as exc:
            logger.error(f'Failed to execute query:{query}. Exception: {exc}')
            return exc
        else:
            # If executed successfully, return fetched rows
            logger.info(f'Query executed successfully: {query}')
            return result.fetchall()