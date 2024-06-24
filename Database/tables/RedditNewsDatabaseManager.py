from dotenv import load_dotenv
import psycopg2
import os

load_dotenv(override=True)
postgres_server = os.getenv("DATABASE_SERVER")
postgres_port = os.getenv("DATABASE_PORT")
postgres_dbname = os.getenv("DATABASE_NAME")
postgres_user = os.getenv("DATABASE_USER")
postgres_pass = os.getenv("DATABASE_PASSWORD")
CREATE_REDDIT_TABLE_QUERY = os.getenv("CREATE_REDDIT_TABLE_QUERY")
CONFIGURE_REDDIT_TABLE = os.getenv("CONFIGURE_REDDIT_TABLE")
INSERT_QUERY_REDDIT_TABLE = os.getenv("INSERT_QUERY_REDDIT_TABLE")
CREATE_REDDIT_SCHEMA_QUERY = os.getenv("CREATE_REDDIT_SCHEMA_QUERY")


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
