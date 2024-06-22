from datetime import datetime
import time

import pandas as pd
from BatchProcess.DataSource.YahooFinance.YahooFinances_Services import YahooFinance
from BatchProcess.DataSource.ListSnP500.ListSnP500Collect import ListSAndP500
from multiprocessing.pool import ThreadPool
from dotenv import load_dotenv
from pathlib import Path
import streamlit as st
import os

from Database.PostGreSQLInteraction import StockDatabaseManager

load_dotenv(override=True)
pool = ThreadPool(processes=6)

current_dir = Path(__file__).parent if "__file__" in locals() else Path.cwd()
css_file = current_dir / os.getenv("CSS_DIR")
defaut_start_date = "2014-01-01"

st.set_page_config(page_title="Home Page", page_icon=":house:")
st.sidebar.header("Quantitative Trading Project")
st.title("Welcome to the Home Page")
st.markdown(
    """
        <style>
            .st-emotion-cache-13ln4jf.ea3mdgi5 {
                max-width: 900px;
            }
        </style>
    """, unsafe_allow_html=True)

# --- LOAD CSS ---
with open(css_file) as f:
    st.markdown("<style>{}</style>".format(f.read()), unsafe_allow_html=True)


@st.cache_data(ttl=1800)
def retrieve_list_ticket():
    list_of_symbols__ = ListSAndP500().tickers_list
    return list_of_symbols__


PROCESS_TIME = 90  # seconds
_list_of_symbols = retrieve_list_ticket()


@st.cache_data(ttl=1800)
def retrieve_data_from_yahoo(list_of_symbols, date_from, date_to):
    transformed_data = YahooFinance(list_of_symbols, date_from, date_to)
    return transformed_data.process_data()


@st.cache_data(ttl=1800)
def update_datebase_func(list_of_symbols=retrieve_list_ticket(), date_from=defaut_start_date, date_to=datetime.now().strftime('%Y-%m-%d')):
    st.write("Database Updated")
    # Retrieve Data from yahoo finance
    async_result = pool.apply_async(
        retrieve_data_from_yahoo, args=(list_of_symbols, date_from, date_to,))
    bar = st.progress(0)
    per = PROCESS_TIME / 100
    for i in range(100):
        time.sleep(per)
        bar.progress(i + 1)
    df = async_result.get()
    return df


@st.cache_data(ttl=1800)
def process_data_retrieve_from_database(df_in, list_of_symbols__):
    total_data_dict = dict()
    for i in range(len(list_of_symbols__)):
        filtered_data = df_in[df_in['stock_id'] == list_of_symbols__[i]]
        filtered_data = filtered_data.reset_index()
        total_data_dict[list_of_symbols__[i]] = filtered_data
    return total_data_dict


update_database = st.button("Update Database")
if update_database:
    df_historical_yahoo = update_datebase_func(
        list_of_symbols=_list_of_symbols)
    total_data_dict_ = process_data_retrieve_from_database(
        df_historical_yahoo, _list_of_symbols)
    db_manager = StockDatabaseManager()
    db_manager.create_schema_and_tables(_list_of_symbols)
    for key, value in total_data_dict_.items():
        if isinstance(value, pd.DataFrame):
            db_manager.insert_data(key, value)
    all_data = db_manager.fetch_all_data()
    for table, df in all_data.items():
        st.write(f"Data for table {table}:")
        st.write(df.head(10))
    db_manager.close_connection()
    st.write("Done")
    # st.write(total_data_dict_)


List500, Historical_data, IndayData_RealTime, news, reddit_news = st.tabs(
    ["List 500 S&P", "Historical data", "In Day Data", "Top News", "Reddit News"])

with List500:
    st.write("List of 500 S&P")
    st.write(_list_of_symbols)
