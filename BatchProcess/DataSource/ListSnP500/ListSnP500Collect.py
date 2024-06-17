import pandas as pd

list_remove = ['GEV','SOLV','VLTO','BF.B','BRK.B']

class ListSAndP500:
    def __init__(self):
        """
        Initialize the ListSAndP500 class

        Attributes:
        tickers_string (list): A list of stock symbols in string format
        tickers_list (list): A list of stock symbols in list format

        """
        _tickers = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        _tickers = _tickers.Symbol.to_list()
        self.tickers_string = [i.replace('.','-') for i in _tickers]
        _tickers_list_transform_ = [i if i not in list_remove else False for i in _tickers]
        self.tickers_list = [i for i in _tickers_list_transform_ if i]

"""
Usage: list_of_symbols = ListSAndP500().tickers_list
"""