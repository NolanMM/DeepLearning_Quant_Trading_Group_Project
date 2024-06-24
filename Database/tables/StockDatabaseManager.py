from dotenv import load_dotenv
import pandas as pd
import psycopg2
import os

load_dotenv(override=True)

create_schema_query = os.getenv("CREATE_SCHEMA_QUERY")


class StockDatabaseManager:
    def __init__(self, con_: psycopg2.extensions.connection):
        """
        Initialize the database connection
        """
        self.conn = con_

    def create_schema_and_tables(self, tickers):
        """
        Create the schema and tables for the given tickers list
        """
        try:
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
        except Exception as e:
            print(e)

    def insert_data(self, ticker, data):
        """
        Insert data into the database
        """
        try:
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
        except Exception as e:
            print(e)

    def get_data_by_table(self, table_name):
        """
        Get data from the given ticket table
        """
        try:
            query = f"SELECT * FROM tickets.{table_name}"
            data = pd.read_sql(query, self.conn)
            return data
        except Exception as e:
            print(e)
            return None

    def get_tables(self, schema='tickets'):
        """
        Get all tables in the given schema
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s", (schema,)
            )
            tables = cursor.fetchall()
            cursor.close()
            return [table[0] for table in tables]
        except Exception as e:
            print(e)
            return None

    def fetch_all_data(self, schema='tickets'):
        """
        Fetch all data from the given schema
        """
        try:
            tables = self.get_tables(schema)
            all_data = {}
            for table in tables:
                query = "SELECT * FROM " + schema + "." + table
                df = pd.read_sql(query, self.conn)
                all_data[table] = df
            return all_data
        except Exception as e:
            print(e)
            return None

    def close_connection(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                print(e)
