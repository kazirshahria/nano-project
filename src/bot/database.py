import os
from mysql.connector import connect
import pandas as pd


class Database(object):
    def __init__(self, host: str = os.environ['DB_HOST'], port: str = os.environ['DB_PORT'], user: str = os.environ['DB_USER'], password: str = os.environ['DB_PASSWORD'], db_name: str = os.environ['DB_NAME']):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = db_name
        self.connection = self.connect_database()

    def connect_database(self):
        connection = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )
        return connection

    def close_connection(self):
        return self.connection.close()

    def table(self, table_name: str):
        cursor = self.connection.cursor()
        cursor.execute(
            f'''
            SELECT *
            FROM {table_name}
            '''
        )
        data = cursor.fetchall()
        cols = [col[0] for col in cursor.description]
        return pd.DataFrame(data=data, columns=cols)
