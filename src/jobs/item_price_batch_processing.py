import json
import os
import pathlib
from typing import Any, Dict, List, Tuple

import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from queries.batch_queries import (
    check_table_query,
    create_and_insert_query,
    create_item_table_query,
    create_or_truncate_staging_item_table_query,
    get_checkpoint_query,
    insert_df_into_staging_query,
    update_checkpoint_query,
    upsert_item_price_query,
)







logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class BatchItemProcessing:

    DB_CONFIG_PATH = pathlib.Path(__file__).resolve().parents[2] / "db_config.json"
    FOLDER_PATH = pathlib.Path(__file__).resolve().parents[2] / 'data/batch_processing_data'

    def __init__(self, file_name_list: List[str]=None) -> None:
        with open(self.DB_CONFIG_PATH, "r") as db_config_file:
            db_config = json.load(db_config_file)

        self.connection = psycopg2.connect(**db_config)
        self.cursor = self.connection.cursor()
        self.new_checkpoint_date = ''
        self.last_checkpoint_date = ''

        # Files must be located in batch_processing_data, this can be modified to pass absolute path instead of filename
        # This assumes files are in order in the folder [correct naming] or when listed.
        if not file_name_list:
            self.file_list = [os.path.join(self.FOLDER_PATH, file) for file
                              in os.listdir(self.FOLDER_PATH)
                              if os.path.isfile(os.path.join(self.FOLDER_PATH, file))]
        else:
            self.file_list = [os.path.join(self.FOLDER_PATH, file) for file
                              in file_name_list
                              if os.path.isfile(os.path.join(self.FOLDER_PATH, file))]

    def get_checkpoint_date(self) -> None:
        # Tries to get last checkpoint. If table doesn't exist it creates it and add a checkpoint far in the past
        self.cursor.execute(check_table_query)
        table_exists = self.cursor.fetchone()[0]

        if not table_exists:
            self.cursor.execute(create_and_insert_query)
            self.connection.commit()

            self.last_checkpoint_date = '1900-01-01T01:01:00+01:00'

            logger.info("Table created and initial checkpoint set.")

        else:
            self.cursor.execute(get_checkpoint_query)
            self.last_checkpoint_date = self.cursor.fetchone()[0]
            logger.info(f"Last checkpoint date: {self.last_checkpoint_date}")

    def update_checkpoint(self) -> None:
        self.cursor.execute(update_checkpoint_query,(self.last_checkpoint_date,))

    def create_item_price_table(self) -> None:
        self.cursor.execute(create_item_table_query)

    def create_or_truncate_staging_item_table(self) -> None:
        self.cursor.execute(create_or_truncate_staging_item_table_query)

    def write_df_to_staging(self, df: pd.DataFrame) -> None:
        rows = list(df.itertuples(index=False, name=None))

        execute_values(self.cursor, insert_df_into_staging_query, rows)


    def upsert_staging_to_production(self) -> None:
        self.cursor.execute(upsert_item_price_query)


    def process_file(self, file) -> pd.DataFrame:
        df = pd.read_csv(file, parse_dates=["created_at","updated_at","system_timestamp"])
        filtered_df = df[df['updated_at'] > self.last_checkpoint_date]
        self.new_checkpoint_date = df['system_timestamp'].max()

        return filtered_df


    def batch_task(self) -> None:
        logger.info(f"Batch processing started.")
        self.get_checkpoint_date()
        self.create_item_price_table()
        for file in self.file_list:
            logger.info(f"Processing file: {file}")
            try:
                self.create_or_truncate_staging_item_table()
                processed_dataframe = self.process_file(file)

                if processed_dataframe.empty:
                    logger.info(f"No data to update in file {file}.")
                    continue

                self.write_df_to_staging(processed_dataframe)
                self.upsert_staging_to_production()
                self.last_checkpoint_date = (self.new_checkpoint_date if
                                             self.last_checkpoint_date < self.new_checkpoint_date else
                                             self.last_checkpoint_date)
                self.update_checkpoint()

                self.connection.commit()

                logger.info(f'Table was successfully updated with file {file}.')

            except Exception as e:

                logger.error(f'Update for file {file} was unsuccessful. Execution will be stopped. Error: {e}')

                self.connection.rollback()

        logger.info(f'Item price table was successfully updated.')
        logger.info(f'Checkpoint date is {self.last_checkpoint_date}.')
        self.cursor.close()
        self.connection.close()



processor = BatchItemProcessing()
processor.batch_task()