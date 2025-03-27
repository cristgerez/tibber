import csv
import json
import logging
import os
import pathlib
import sys
from typing import Any, Dict, List, Tuple

import psycopg2
from pydantic import ValidationError

from clients.vat_client import CurrencyAPIClient
from models.currencies import Currency, ExchangeRate
from queries.currency_queries import (
    create_currencies_table,
    create_currency_convertion_rates_table,
    create_temp_currencies_table,
    create_temp_currency_convertion_rates_table,
    upsert_currencies,
    upsert_currency_conversion_rates,
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)



class CurrencyUpdate:

    ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    CURRENCIES_FILE = pathlib.Path(__file__).resolve().parents[2] / "data/raw_currency_data/currencies.csv"
    EXCHANGE_RATES_FILE = pathlib.Path(__file__).resolve().parents[2] / "data/raw_currency_data/currency_conversion_rates.csv"

    CURRENCIES_COLUMNS = ["code", "name", "symbol"]
    EXCHANGE_RATES_COLUMNS = ["base_currency", "target_currency", "exchange_rate"]

    DB_CONFIG_PATH = pathlib.Path(__file__).resolve().parents[2] / "db_config.json"

    def __init__(self) -> None:
        with open(self.DB_CONFIG_PATH, "r") as db_config_file:
            db_config = json.load(db_config_file)

        self.connection = psycopg2.connect(**db_config)
        self.cursor = self.connection.cursor()
        self.currency_client=CurrencyAPIClient()
        self.currencies_data = []
        self.exchange_rate_data = []

    @staticmethod
    def validate_currency_data(currency_dict: Dict) -> List[tuple[str, ...]]:
        validated_currencies = []
        for currency, data in currency_dict.items():
            try:
                model_check_dict = {"code":currency,**data}
                Currency(**model_check_dict)
                validated_currencies.append(tuple(model_check_dict.values()))

            except ValidationError as e:
                logger.error(f'Error while validating {currency} value won\'t be updated. \n Error:{e}')

        return validated_currencies

    @staticmethod
    def validate_currency_conversion_data(base_currency: str,
                                          target_currency: str,
                                          exchange_rate: float) -> bool:
        try:
            ExchangeRate(base_currency=base_currency,
                         target_currency=target_currency,
                         exchange_rate=exchange_rate)
            return True

        except ValidationError as e:
            return False


    def get_currencies_data(self) -> None:

        currencies_dict = self.currency_client.fetch_currencies()
        self.currencies_data = self.validate_currency_data(currencies_dict)


    def get_exchange_rates(self) -> None:

        rates_list = []
        base_currencies_list = [currency[0] for currency in self.currencies_data]
        for base_currency in base_currencies_list:
            try:
                rates_data = self.currency_client.fetch_base_rates(base_currency)
                for target_currency, exchange_rate in rates_data.items():

                    if not self.validate_currency_conversion_data(base_currency,target_currency,exchange_rate):
                        logger.warning(f'Exchange rate for {base_currency} to {target_currency} is invalid.')

                        continue

                    rates_list.append((base_currency, target_currency, exchange_rate))

            except Exception as error:
                logger.error(f"Error retrieving base {base_currency} currency rates: {error}")

        self.exchange_rate_data = rates_list

    @staticmethod
    def write_data_to_csv(data: List[Tuple[str, str, Any]],
                          file_name: str,
                          column_names: List[Any]) -> None:

        with open(file_name, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(column_names)
            writer.writerows(data)

    def insert_into_staging_table(self, csv_file: str, table_name: str):
        with open(csv_file, 'r', encoding='utf-8') as f:
            copy_query = f"COPY {table_name} FROM STDIN WITH CSV HEADER"

            self.cursor.copy_expert(copy_query, f)

    def upsert_currencies_task(self) -> None:
        try:
            logger.info(f'Starting currency task.')
            self.cursor.execute(create_currencies_table)
            self.cursor.execute(create_temp_currencies_table)

            self.insert_into_staging_table(self.CURRENCIES_FILE, 'staging_currencies')

            self.cursor.execute(upsert_currencies)

            self.connection.commit()

            logger.info('Currencies table upserted successfully.')

        except Exception as error:
            self.connection.rollback()
            logger.error(f'Error while upserting currencies table: {error}')

    def upsert_exchange_rates_task(self) -> None:
        try:
            logger.info(f'Starting exchange rate task.')
            self.cursor.execute(create_currency_convertion_rates_table)
            self.cursor.execute(create_temp_currency_convertion_rates_table)

            self.insert_into_staging_table(self.EXCHANGE_RATES_FILE, 'staging_currency_conversion_rates')

            self.cursor.execute(upsert_currency_conversion_rates)

            self.connection.commit()

            logger.info('Currency rates table upserted successfully.')

        except Exception as error:
            self.connection.rollback()
            logger.error(f'Error while upserting currency rates table: {error}')

    def currencies_table_job(self):
        self.get_currencies_data()
        self.write_data_to_csv(self.currencies_data, self.CURRENCIES_FILE, self.CURRENCIES_COLUMNS)
        self.upsert_currencies_task()

    def exchange_rates_table_job(self):
        self.get_exchange_rates()
        self.write_data_to_csv(self.exchange_rate_data, self.EXCHANGE_RATES_FILE, self.EXCHANGE_RATES_COLUMNS)
        self.upsert_exchange_rates_task()

    def currency_task(self) -> None:
        try:
            self.currencies_table_job()
            self.exchange_rates_table_job()

        except Exception as e:
            logger.error(f'Task was unsuccessful. Execution will be stopped. Error: {e}')

        finally:
            self.cursor.close()
            self.connection.close()
            logger.info("Connection closed.")
