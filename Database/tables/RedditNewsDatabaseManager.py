from dotenv import load_dotenv
import psycopg2
import os

load_dotenv(override=True)

CREATE_REDDIT_TABLE_QUERY = os.getenv("CREATE_REDDIT_TABLE_QUERY")
CONFIGURE_REDDIT_TABLE = os.getenv("CONFIGURE_REDDIT_TABLE")
INSERT_QUERY_REDDIT_TABLE = os.getenv("INSERT_QUERY_REDDIT_TABLE")
CREATE_REDDIT_SCHEMA_QUERY = os.getenv("CREATE_REDDIT_SCHEMA_QUERY")


class RedditNewsDatabaseManager:
    def __init__(self, con_: psycopg2.extensions.connection):
        """
        Initialize the database connection
        """
        self.conn = con_

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
