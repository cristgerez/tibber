import json
import pathlib
import psycopg2

from queries.views import (
    view_query,
    all_columns_view_query
)



class NokView:
    DB_CONFIG_PATH = pathlib.Path(__file__).resolve().parents[2] / "db_config.json"

    def __init__(self):
        with open(self.DB_CONFIG_PATH, "r") as db_config_file:
            db_config = json.load(db_config_file)

        self.connection = psycopg2.connect(**db_config)
        self.cursor = self.connection.cursor()

    def create_view(self):
        self.cursor.execute(view_query)
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

NokView().create_view()