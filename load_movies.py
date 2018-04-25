import csv
import re
from psycopg2 import sql

from db_methods import query_sql, insert_row


class MovieLoader(object):
    def __init__(self, file_path, table, cursor):
        self.file_path = file_path
        self.table = table
        self.cursor = cursor

    def _load_csv(self):
        with open(self.file_path) as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)
            for row in reader:
                yield row

    def load_movies(self):
        for row in self._load_csv():
            query = insert_row(table=self.table)
            params = (row[0], row[1], row[2])
            query_sql(cursor=self.cursor, query=query, params=params)
