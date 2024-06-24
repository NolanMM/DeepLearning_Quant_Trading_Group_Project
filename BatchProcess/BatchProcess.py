from Database.PostGreSQLInteraction import DatabaseManager, StockDatabaseManager, TicketDimDatabaseManager, RedditNewsDatabaseManager
from BatchProcess.DataSource.YahooFinance.YahooFinances_Services import YahooFinance
from BatchProcess.DataSource.ListSnP500.ListSnP500Collect import ListSAndP500
from datetime import datetime
import pandas as pd


defaut_start_date = "2014-01-01"

date_to = datetime.now().strftime('%Y-%m-%d')


class BatchProcessManager:
    def __init__(self):
        self.list_of_symbols = None
        self.dict_ticket = dict()

    def run_process(self, list_of_symbols_):
        self.list_of_symbols = list_of_symbols_

        # Get data from Yahoo Finance
        transformed_data = YahooFinance(
            self.list_of_symbols, defaut_start_date, date_to)
        df = transformed_data.process_data()

        # Create Database Manager
        db_manager = DatabaseManager()

        # Drop all tables exist in the database
        db_manager.delete_schema()

        # Create Stock table
        db_manager.StockDatabaseManager.create_schema_and_tables(
            self.list_of_symbols)

        # Create TicketDim table
        db_manager.TicketDimDatabaseManager.create_table()

        # Create RedditNews table
        db_manager.RedditNewsDatabaseManager.create_schema_and_tables()

        # Apply multiprocessing to insert data into the database (Testing later)
        for i in range(len(self.list_of_symbols)):
            filtered_data = df[df['stock_id'] == self.list_of_symbols[i]]
            filtered_data = filtered_data.reset_index()
            self.dict_ticket[self.list_of_symbols[i]] = filtered_data

        # Insert data into the database Stock table
        for key, value in self.dict_ticket.items():
            if isinstance(value, pd.DataFrame):
                db_manager.StockDatabaseManager.insert_data(key, value)

        # Insert data into the database TicketDim table
        db_manager.TicketDimDatabaseManager.insert_data(self.list_of_symbols)

        db_manager.close_connection()
        return self.dict_ticket

    def get_stock_data_by_ticker(self, ticker):
        try:
            # Create StockDatabaseManager
            db_manager = StockDatabaseManager()
            # Get data by table
            data = db_manager.get_data_by_table(ticker)
            db_manager.close_connection()
            return data
        except Exception as e:
            print(e)
            return None

    def get_stock_list_in_database(self):
        try:
            # Create TicketDimDatabaseManager
            db_manager = TicketDimDatabaseManager()
            # Get data
            data = db_manager.get_data()
            db_manager.close_connection()
            return data
        except Exception as e:
            print(e)
            return None

    def get_all_stock_data_in_database(self):
        try:
            db_manager = StockDatabaseManager()
            data = db_manager.fetch_all_data()
            db_manager.close_connection()
            dataframes_list = [value for key, value in data.items()]
            combined_dataframe = pd.concat(dataframes_list, ignore_index=True)
            combined_dataframe['date'] = pd.to_datetime(
                combined_dataframe['date'])

            # Sort the combined dataframe by the 'date' column
            return combined_dataframe.sort_values(by='date')
        except Exception as e:
            print(e)
            return None
