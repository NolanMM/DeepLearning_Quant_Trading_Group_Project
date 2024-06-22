from pyspark.sql.types import StructType, StructField, StringType
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd
import os

load_dotenv(override=True)

column_1_name = os.getenv("COLUMN_1")
column_2_name = os.getenv("COLUMN_2")
column_3_name = os.getenv("COLUMN_3")
column_4_name = os.getenv("COLUMN_4")
column_5_name = os.getenv("COLUMN_5")
column_6_name = os.getenv("COLUMN_6")
column_7_name = os.getenv("COLUMN_7")
column_8_name = os.getenv("COLUMN_8")


class YahooFinance:
    def __init__(self, list_of_symbols, start, end):
        """
        Initialize the YahooFinance class

        Args:
        list_of_symbols (list): A list of stock symbols
        start (str): The start date for the historical data (datetime string format: 'YYYY-MM-DD')
        end (str): The end date for the historical data (datetime string format: 'YYYY-MM-DD')

        Attributes:
        symbols (list): A list of stock symbols
        interval (str): The interval for the historical data (default is '1d' for daily)
        start (str): The start date for the historical data
        end (str): The end date for the historical data
        results (DataFrame): A DataFrame containing the transformed historical stock data for a call
        query (None): A placeholder for the query object 

        """
        self.schema = StructType([
            StructField(column_1_name, StringType(), True),
            StructField(column_2_name, StringType(), True),
            StructField(column_3_name, StringType(), True),
            StructField(column_4_name, StringType(), True),
            StructField(column_5_name, StringType(), True),
            StructField(column_6_name, StringType(), True),
            StructField(column_7_name, StringType(), True)
            # StructField(column_8_name, StringType(), True)
        ])

        self.symbols = list_of_symbols
        self.interval = '1d'
        self.start = start
        self.end = end
        self.results = None

    def process_data(self):
        """
        Process the historical stock data for the stock symbols
        """
        data = self.get_data()
        self.results = self.transform_data(data)
        return self.results

    def get_data(self):
        """
        Get historical stock data from Yahoo Finance API using yfinance library

        Returns:
        DataFrame: A DataFrame containing historical stock data
        """
        try:
            data = yf.download(
                self.symbols,
                start=self.start,
                end=self.end,
                interval=self.interval,
                ignore_tz=True,
                threads=5,
                timeout=60,
                progress=True
            )
            return data
        except Exception as e:
            print(f"Error downloading data: {e}")
            return None

    def transform_data(self, df):
        """
        Transform the historical stock data into a format that can be stored in a database FactPrices table

        Args:
        df (DataFrame): A DataFrame containing historical stock data

        Returns:
        DataFrame: A DataFrame containing transformed historical stock data with the following columns:
        - stock_id (str): The stock symbol
        - date (str): The date of the stock data
        - open (float): The opening price of the stock
        - high (float): The highest price of the stock
        - low (float): The lowest price of the stock
        - close (float): The closing price of the stock
        - volume (int): The volume of the stock
        - adjusted_close (float): The adjusted closing price of the stock

        """
        # Convert df into dataframe
        # df = pd.DataFrame(df)

        # Reset the index to turn the MultiIndex into columns
        df = df.reset_index()

        # Create a list to store transformed records
        records = []

        # Iterate over each row and stock symbol
        for index, row in df.iterrows():
            date = row[('Date', '')]
            for stock in self.symbols:
                try:
                    record = {
                        column_1_name: stock,
                        column_2_name: date,
                        column_3_name: row[('Open', stock)],
                        column_4_name: row[('High', stock)],
                        column_5_name: row[('Low', stock)],
                        column_6_name: row[('Close', stock)],
                        column_7_name: row[('Volume', stock)]
                        # column_8_name: row[('Adj Close', stock)]
                    }
                    records.append(record)
                except KeyError as e:
                    print(f"KeyError: {e} for stock: {stock} on date: {date}")

        # Convert the list of records into a DataFrame
        return pd.DataFrame(records)


"""
Usage:

# transformed_data = YahooFinance(['AAPL','MSFT','GOOGL'], '2024-06-01', '2024-06-09').results
# print(transformed_data.head())
# list_of_symbols = ListSAndP500().tickers_list
# print(list_of_symbols)

"""
