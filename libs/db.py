import logging
import logging.config
import os
import sys
import pandas as pd
from sqlalchemy import Engine, text, create_engine
from sqlalchemy.exc import ProgrammingError
# Add path in order to access libs folder
sys.path.append(os.path.dirname(__file__))
from settings import settings

# Engine is created to be called as modules from other scripts
default_engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)

# Logging config
# logging.config.fileConfig('config_logs.conf')
logger = logging.getLogger('DB')

# Tables creation SQL path
TABLES_CREATION_SQL_PATH = os.path.join(
    os.path.dirname(__file__),
    '../',
    'database',
    'create_tables.sql'
)


class DB:
    '''
    Class used to connect to a database, truncate and update tables on it.
    '''

    def create_tables(
        sql_path: str,
        engine: Engine = default_engine
    ) -> bool:
        """
        Executes a SQL script to create tables structure.

        Args:
            sql_path (str): Path of the SQL file.
            engine (Engine, optional): Database connection engine.
                Defaults to default_engine.

        Returns:
            bool: True if created successfully, False otherwise.
        """
        try:
            with engine.connect() as con:
                with open(sql_path) as file:
                    query = text(file.read())
                    con.execute(query)
                    con.commit()
        except Exception:
            logger.error('Failed to create tables.', exc_info=True)
            return False
        logger.info('Tables sucessfully created!')
        return True

    def prepare_tables(
        tables: list,
        engine: Engine = default_engine
    ) -> bool:
        """
        This method truncates tables if they already exist,
        or create them otherwise.

        Args:
            tables (list): List of tables names to create/truncate.
            engine (Engine, optional): Database connection engine.
                Defaults to default_engine.

        Returns:
            bool : True if created or truncated successfully,
                False otherwise.
        """
        # Iterate table names list and build the truncate query
        query = "TRUNCATE TABLE "
        for idx, table_name in enumerate(tables):
            query += table_name
            if idx < len(tables) - 1:
                query += ", "
        # Add cascade keyword to ignore constraints
        query += " CASCADE"
        # Catch and return possible exceptions
        try:
            with engine.connect() as connection:
                connection.execute(text(query))
                connection.commit()
        except ProgrammingError as e:
            # This error means tables does not exists yet,
            # so we need to create them first
            if e.args[0].startswith('(psycopg2.errors.UndefinedTable)'):
                logger.info("Tables does not exists. Creating tables...")
                return DB.create_tables(sql_path=TABLES_CREATION_SQL_PATH)
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
            # Separate normal and intermediate tables
            normal_tables = []
            intermediate_tables = []
            for filename in os.listdir(dir_csv_files):
                if filename.startswith('apps_'):
                    intermediate_tables.append(filename)
                else:
                    normal_tables.append(filename)
            # Put normal tables first, then intermediate ones
            # This is to prevent foreign key violation errors
            tables = normal_tables + intermediate_tables
            # Then, update tables in that order
            for filename in tables:
                # Full file path
                file_path = f'{dir_csv_files}/{filename}'
                df_name = filename.split('.')[0]
                # Read dataframe and uploads it to database
                # in 'append' mode, to maintain table structure
                # and constraints
                df = pd.read_csv(file_path)
                df.to_sql(
                    df_name,
                    engine,
                    if_exists='append',
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
