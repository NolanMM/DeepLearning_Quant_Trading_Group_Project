import plotly.graph_objects as go
from dotenv import load_dotenv
from datetime import datetime
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

CREATE_REDDIT_TABLE_QUERY = os.getenv("CREATE_REDDIT_TABLE_QUERY")
CONFIGURE_REDDIT_TABLE = os.getenv("CONFIGURE_REDDIT_TABLE")
INSERT_QUERY_REDDIT_TABLE = os.getenv("INSERT_QUERY_REDDIT_TABLE")
CREATE_REDDIT_SCHEMA_QUERY = os.getenv("CREATE_REDDIT_SCHEMA_QUERY")


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
        try:
            conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            return conn
        except Exception as e:
            print(e)
            return None

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


class TicketDimDatabaseManager:
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
        try:
            conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            return conn
        except Exception as e:
            print(e)
            return None

    def create_table(self):
        """
        Create the ticket_dim table
        """
        try:
            cursor = self.conn.cursor()

            # Create schema if it doesn't exist
            cursor.execute(create_schema_query)

            # Create the ticket_dim table
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS tickets.ticket_dim ("
                "symbol VARCHAR(10) PRIMARY KEY,"
                "company_name VARCHAR(255) NULL,"
                "established DATE NULL,"
                "sector VARCHAR(100) NULL,"
                "industry VARCHAR(100) NULL,"
                "exchange VARCHAR(50) NULL"
                ")"
            )

            self.conn.commit()
            cursor.close()
        except Exception as e:
            print(e)

    def insert_data(self, data):
        """
        Insert data into the ticket_dim table
        """
        try:
            if isinstance(data, list):
                data = pd.DataFrame(data, columns=['symbol'])
                data['company_name'] = None
                data['established'] = None
                data['sector'] = None
                data['industry'] = None
                data['exchange'] = None

            cursor = self.conn.cursor()

            # Ensure all data is treated as string
            # df_data = pd.DataFrame(data_in)
            # data = df_data.astype(str)

            insert_query = (
                "INSERT INTO tickets.ticket_dim (symbol, company_name, established, sector, industry, exchange)"
                " VALUES (%s, %s, %s, %s, %s, %s)"
                " ON CONFLICT (symbol) DO UPDATE SET"
                " company_name = EXCLUDED.company_name,"
                " established = EXCLUDED.established,"
                " sector = EXCLUDED.sector,"
                " industry = EXCLUDED.industry,"
                " exchange = EXCLUDED.exchange"
            )

            for index, row in data.iterrows():
                cursor.execute(insert_query, (row['symbol'], row.get('company_name'),
                                              row.get('established'), row.get('sector'), row.get('industry'), row.get('exchange')))
            self.conn.commit()
            cursor.close()
        except Exception as e:
            print(e)

    def get_data(self):
        """
        Get all data from the ticket_dim table
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT * FROM tickets.ticket_dim"
            )
            data = cursor.fetchall()
            cursor.close()
            # Convert data to list
            data = [list(row)[0] for row in data]
            return data
        except Exception as e:
            print(e)
            return None

    def search_ticker(self, symbol):
        """
        Search for a specific ticker by its symbol
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT * FROM tickets.ticket_dim WHERE symbol = %s", (symbol,)
            )
            data = cursor.fetchone()
            cursor.close()
            return data
        except Exception as e:
            print(e)
            return None

    def close_connection(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                print(e)


class RedditNewsDatabaseManager:
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
        try:
            conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            return conn
        except Exception as e:
            print(e)
            return None

    def create_schema_and_tables(self):
        """
        Create the stock_reddit_news table
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(CREATE_REDDIT_SCHEMA_QUERY)
            cursor.execute(CREATE_REDDIT_TABLE_QUERY)
            cursor.execute(CONFIGURE_REDDIT_TABLE)
            self.conn.commit()
            cursor.close()
        except Exception as e:
            print(e)

    def insert_data(self, data):
        """
        Insert data into the stock_reddit_news table
        """
        try:
            cursor = self.conn.cursor()
            insert_query = INSERT_QUERY_REDDIT_TABLE

            for index, row in data.iterrows():
                cursor.execute(insert_query, (row["id"], row["subreddit"], row["url"], row["title"].replace('\'', ""),
                                              row["score"], row["num_comments"], row["downvotes"], row["ups"], row["date_created_utc"]))
            self.conn.commit()
            cursor.close()
        except Exception as e:
            print(e)

    def get_news_by_ticker(self, ticker):
        """
        Get news from the stock_reddit_news table corresponding to a specific ticker
        """
        try:
            cursor = self.conn.cursor()
            query = """
            SELECT * FROM reddits.stock_reddit_news
            WHERE LOWER(title) LIKE %s
            """
            cursor.execute(query, ('%' + ticker.lower() + '%',))
            data = cursor.fetchall()
            cursor.close()
            return data
        except Exception as e:
            print(e)
            return None

    def close_connection(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                print(e)


class DatabaseManager:
    def __init__(self):
        """
        Initialize the database connection
        """
        self.StockDatabaseManager = StockDatabaseManager()
        self.TicketDimDatabaseManager = TicketDimDatabaseManager()
        self.RedditNewsDatabaseManager = RedditNewsDatabaseManager()

    def delete_schema(self):
        try:
            # Fix later
            cursor = self.StockDatabaseManager.conn.cursor()
            cursor.execute("DROP SCHEMA IF EXISTS tickets CASCADE;")
            cursor.execute("DROP SCHEMA IF EXISTS reddits CASCADE;")
            self.StockDatabaseManager.conn.commit()
            cursor.close()
        except Exception as e:
            print(e)

    def close_connection(self):
        self.StockDatabaseManager.close_connection()
        self.TicketDimDatabaseManager.close_connection()
        self.RedditNewsDatabaseManager.close_connection()
