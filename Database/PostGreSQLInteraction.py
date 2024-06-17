from dotenv import load_dotenv

load_dotenv(override=True)

postgres_v = os.getenv("POSTGRES_VERSION")
postgres_url = os.getenv("POSTGRES_URL")
postgres_user = os.getenv("POSTGRES_USER")
postgres_pass = os.getenv("POSTGRES_PASSWORD")
postgres_table = os.getenv("POSTGRES_TABLE")
format_file = os.getenv("FORMAT_FILE")
_mode = os.getenv("MODE")


# Fact Prices tables
config_ = postgres_v

column_1_name = os.getenv("COLUMN_1")
column_2_name = os.getenv("COLUMN_2")
column_3_name = os.getenv("COLUMN_3")
column_4_name = os.getenv("COLUMN_4")
column_5_name = os.getenv("COLUMN_5")
column_6_name = os.getenv("COLUMN_6")
column_7_name = os.getenv("COLUMN_7")
column_8_name = os.getenv("COLUMN_8")

# def _write_streaming(self, df_):
    #     """
    #     Write the transformed historical stock data to a PostgreSQL database

    #     Args:
    #     df_ (DataFrame): A DataFrame containing transformed historical stock data

    #     Returns:
    #     bool: A boolean indicating whether the data was successfully written to the database

    #     """
    #     try:
    #         df_.write \
    #             .mode(_mode) \
    #             .format(self.format_file) \
    #             .option("url", f"{self.postgres_url}") \
    #             .option("driver", "org.postgresql.Driver") \
    #             .option("dbtable", self.postgres_table) \
    #             .option("user", self.postgres_user) \
    #             .option("password", self.postgres_pass) \
    #             .save()
    #         return True
    #     except Exception as e:
    #         print(f"Error: {e}")
    #         return False