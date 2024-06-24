import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
import os


class StockDatabaseManager:
    def __init__(self):
        load_dotenv(override=True)
        self.dbname = os.getenv("DATABASE_NAME")
        self.user = os.getenv("DATABASE_USER")
        self.password = os.getenv("DATABASE_PASSWORD")
        self.host = os.getenv("DATABASE_SERVER")
        self.port = os.getenv("DATABASE_PORT")
        self.engine = self.create_engine()
        self.metadata = MetaData(schema='tickets')
        self.create_schema_query = os.getenv("CREATE_SCHEMA_QUERY")

    def create_engine(self):
        db_url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        engine = create_engine(db_url)
        return engine

    def create_schema_and_tables(self, tickers):
        try:
            with self.engine.connect() as connection:
                # Create schema if it doesn't exist
                connection.execute(self.create_schema_query)

                for ticker in tickers:
                    table = Table(ticker, self.metadata,
                                  Column('stock_id', String(
                                      10), primary_key=True),
                                  Column('date', String(10), primary_key=True),
                                  Column('open', String(50)),
                                  Column('high', String(50)),
                                  Column('low', String(50)),
                                  Column('close', String(50)),
                                  Column('volume', String(50))
                                  )
                    table.create(self.engine, checkfirst=True)
            return True
        except Exception as e:
            print(f"Error creating schema and tables: {e}")
            return False

    def insert_data(self, ticker, data):
        table = Table(ticker, self.metadata, autoload_with=self.engine)

        data = data.astype(str)
        try:
            with self.engine.connect() as connection:
                for index, row in data.iterrows():
                    stmt = insert(table).values(
                        stock_id=row['stock_id'],
                        date=row['date'],
                        open=row['open'],
                        high=row['high'],
                        low=row['low'],
                        close=row['close'],
                        volume=row['volume']
                    ).on_conflict_do_update(
                        index_elements=['stock_id', 'date'],
                        set_=dict(
                            open=row['open'],
                            high=row['high'],
                            low=row['low'],
                            close=row['close'],
                            volume=row['volume']
                        )
                    )
                    connection.execute(stmt)
            return True
        except Exception as e:
            print(f"Error inserting data: {e}")
            return False

    def get_tables(self):
        try:
            inspector = self.engine.inspect(self.engine)
            tables = inspector.get_table_names(schema='tickets')
            return tables
        except Exception as e:
            print(f"Error getting tables: {e}")
            return []

    def fetch_all_data(self):
        try:
            tables = self.get_tables()
            all_data = {}
            for table_name in tables:
                table = Table(table_name, self.metadata,
                              autoload_with=self.engine)
                query = table.select()
                df = pd.read_sql(query, self.engine)
                all_data[table_name] = df
            return all_data
        except Exception as e:
            print(f"Error fetching data: {e}")
            return {}
