import psycopg2
import praw
from datetime import datetime
import pandas as pd
from time import sleep
import os
from dotenv import load_dotenv


class RedditToPostgres:
    def __init__(self):
        load_dotenv(override=True)

        self.subreddits = ["trading", "stockmarket",
                           "investing", "stocks", "wallstreetbets"]
        self.fields = ["title", "url", "subreddit",
                       "score", "num_comments", "ups", "id"]

        self.client_id = os.getenv("REDDIT_CLIENT_ID")
        self.client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        self.username = os.getenv("REDDIT_USER")
        self.password = os.getenv("REDDIT_PASSWORD")
        self.user_agent = os.getenv("REDDIT_USER_AGENT")

        self.postgres_url = os.getenv("POSTGRES_URL")
        self.postgres_user = os.getenv("POSTGRES_USER")
        self.postgres_pass = os.getenv("POSTGRES_PASSWORD")
        self.postgres_table = os.getenv("POSTGRES_TABLE")
        self.format_file = os.getenv("FORMAT_FILE")
        self.mode = os.getenv("MODE")
        self.insert_query = os.getenv("INSERT_QUERY")
        self.configure_table = os.getenv("CONFIGURE_TABLE")
        self.create_table_query = os.getenv("CREATE_TABLE_QUERY")
        self.connection_os_postgres = os.getenv("POSTGRE_CONNECTION")

    @staticmethod
    def convert_unix_to_datetime(unix_timestamp):
        try:
            if isinstance(unix_timestamp, str):
                if unix_timestamp.isdigit():
                    unix_timestamp = int(unix_timestamp)
                else:
                    try:
                        datetime.strptime(unix_timestamp, '%Y-%m-%d %H:%M:%S')
                        return unix_timestamp
                    except ValueError:
                        raise ValueError("Invalid Unix timestamp format")
            elif isinstance(unix_timestamp, int):
                pass
            else:
                raise ValueError("Invalid Unix timestamp format")

            return datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print(f"Error: {e}")
            return None

    def init_postgresql(self):
        connection = psycopg2.connect(self.connection_os_postgres)
        connection.set_client_encoding('UTF8')
        cursor = connection.cursor()
        cursor.execute(self.create_table_query)
        cursor.execute(self.configure_table)
        connection.commit()
        return connection, cursor

    @staticmethod
    def write_to_postgresql(df, cursor, connection, insert_query):
        """Write DataFrame to PostgreSQL database."""
        for _, row in df.iterrows():
            date_created_utc = RedditToPostgres.convert_unix_to_datetime(
                row["date_created_utc"])

            if date_created_utc is None:
                print(
                    f"Skipping row due to invalid timestamp: {date_created_utc}")
                continue
            row["date_created_utc"] = str(row["date_created_utc"])
            data = (row["id"], row["subreddit"], row["url"], row["title"].replace(
                '\'', ""), row["score"], row["num_comments"], row["downvotes"], row["ups"], row["date_created_utc"])
            try:
                cursor.execute(insert_query, data)
                connection.commit()
            except Exception as e:
                print(f"Error: {e}")
                continue

    def init_reddit(self):
        """Initialize the reddit instance"""
        return praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent,
            username=self.username,
            password=self.password
        )

    def reddit_stream(self, reddit, cursor, connection):
        """Stream the data from Reddit"""
        rows = []
        while True:
            recent_posts = reddit.subreddit(
                "+".join(self.subreddits)).hot(limit=10)
            for p in recent_posts:
                rows_dict = {field: str(getattr(p, field))
                             for field in self.fields}
                created_utc = str(int(getattr(p, "created_utc")))
                rows_dict["date_created_utc"] = str(
                    datetime.fromtimestamp(int(created_utc)))
                rows_dict["downvotes"] = str(
                    getattr(p, "ups") - getattr(p, "score"))
                rows.append(rows_dict)
            df = pd.DataFrame(rows)
            with open('reddit.csv', 'a', newline='', encoding='utf-8') as f:
                df.to_csv(f, header=f.tell() == 0, index=False)
            if not df.empty:
                RedditToPostgres.write_to_postgresql(
                    df, cursor, connection, self.insert_query)
            rows.clear()
            print(f"Finish write at {datetime.now()}")
            sleep(65)

    def main(self):
        reddit = self.init_reddit()
        connection, cursor = self.init_postgresql()
        self.reddit_stream(reddit, cursor, connection)


if __name__ == "__main__":
    RedditToPostgres().main()
