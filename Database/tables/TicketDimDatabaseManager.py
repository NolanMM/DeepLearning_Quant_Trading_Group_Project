from dotenv import load_dotenv
import pandas as pd
import psycopg2
import os

load_dotenv(override=True)

create_schema_query = os.getenv("CREATE_SCHEMA_QUERY")
postgres_server = os.getenv("DATABASE_SERVER")
postgres_port = os.getenv("DATABASE_PORT")
postgres_dbname = os.getenv("DATABASE_NAME")
postgres_user = os.getenv("DATABASE_USER")
postgres_pass = os.getenv("DATABASE_PASSWORD")


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
