from dotenv import load_dotenv
from psycopg2 import sql
import pandas as pd
import psycopg2
import os

load_dotenv(override=True)

postgres_server = os.getenv("DATABASE_SERVER")
postgres_port = os.getenv("DATABASE_PORT")
postgres_dbname = os.getenv("DATABASE_NAME")
postgres_user = os.getenv("DATABASE_USER")
postgres_pass = os.getenv("DATABASE_PASSWORD")

create_schema_query = os.getenv("CREATE_SCHEMA_QUERY")


class StockDatabaseManager:
    def __init__(self):
        """
        Initialize the database connection
        """
        self.dbname = postgres_dbname
        self.user = postgres_user
        self.password = postgres_pass
        self.host = postgres_server
        self.port = postgres_port
        self.conn = self.create_connection()

    def create_connection(self):
        """
        Create a connection to the database
        """
        conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        return conn

    def create_schema_and_tables(self, tickers):
        """
        Create the schema and tables for the given tickers list
        """
        cursor = self.conn.cursor()

        # Create schema if it doesn't exist
        cursor.execute(create_schema_query)

        # Loop through each ticker and create the corresponding table
        for ticker in tickers:
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS tickets." + ticker + " ("
                "stock_id VARCHAR(10),"
                "date VARCHAR(10),"
                "open VARCHAR(50),"
                "high VARCHAR(50),"
                "low VARCHAR(50),"
                "close VARCHAR(50),"
                "volume VARCHAR(50),"
                "PRIMARY KEY (stock_id, date))"
            )

            # Create index on date for faster queries
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS " + ticker +
                "_date_idx ON tickets." + ticker + " (date)"
            )

        self.conn.commit()
        cursor.close()

    def insert_data(self, ticker, data):
        """
        Insert data into the database
        """
        cursor = self.conn.cursor()

        # Ensure all data is treated as string
        data = data.astype(str)

        insert_query = (
            "INSERT INTO tickets." + ticker +
            " (stock_id, date, open, high, low, close, volume)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (stock_id, date) DO UPDATE SET"
            " open = EXCLUDED.open,"
            " high = EXCLUDED.high,"
            " low = EXCLUDED.low,"
            " close = EXCLUDED.close,"
            " volume = EXCLUDED.volume"
        )

        for index, row in data.iterrows():
            cursor.execute(insert_query, (row['stock_id'], row['date'],
                           row['open'], row['high'], row['low'], row['close'], row['volume']))
        self.conn.commit()
        cursor.close()

    def get_tables(self, schema='tickets'):
        """
        Get all tables in the given schema
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s", (schema,)
        )
        tables = cursor.fetchall()
        cursor.close()
        return [table[0] for table in tables]

    def fetch_all_data(self, schema='tickets'):
        """
        Fetch all data from the given schema
        """
        tables = self.get_tables(schema)
        all_data = {}
        for table in tables:
            query = "SELECT * FROM " + schema + "." + table
            df = pd.read_sql(query, self.conn)
            all_data[table] = df
        return all_data

    def close_connection(self):
        if self.conn:
            self.conn.close()
