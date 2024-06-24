from Database.tables.RedditNewsDatabaseManager import RedditNewsDatabaseManager
from Database.tables.TicketDimDatabaseManager import TicketDimDatabaseManager
from Database.tables.StockDatabaseManager import StockDatabaseManager
from dotenv import load_dotenv
import psycopg2
import os

load_dotenv(override=True)

postgres_server = os.getenv("DATABASE_SERVER")
postgres_port = os.getenv("DATABASE_PORT")
postgres_dbname = os.getenv("DATABASE_NAME")
postgres_user = os.getenv("DATABASE_USER")
postgres_pass = os.getenv("DATABASE_PASSWORD")


class DatabaseManager:
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
        self.StockDatabaseManager = StockDatabaseManager(self.conn)
        self.TicketDimDatabaseManager = TicketDimDatabaseManager(self.conn)
        self.RedditNewsDatabaseManager = RedditNewsDatabaseManager(self.conn)

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
