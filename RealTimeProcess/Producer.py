import json
from kafka import KafkaProducer
import pandas as pd
from dotenv import load_dotenv
import os
from ListSnP500.ListSnP500Collect import ListSAndP500
from YahooFinance.YahooFinances_Services import YahooFinance


class FactPriceProducerStreaming:
    def __init__(self):
        load_dotenv(override=True)

        self.list_of_symbols = ListSAndP500().tickers_list
        self.start = '2015-01-01'
        self.end = '2024-01-01'
        self.results = self.manipulate()


    def manipulate(self):
        yahoo = YahooFinance(self.list_of_symbols, self.start, self.end).results
        return yahoo


def main():
    streamer = FactPriceProducerStreaming()
    streamer_result = streamer.results
    print(len(streamer_result))

if __name__ == "__main__":
    main()