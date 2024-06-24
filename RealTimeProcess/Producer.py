import json
from kafka import KafkaProducer
import pandas as pd
from dotenv import load_dotenv
import os


class KafkaProducerService:
    def __init__(self):
        load_dotenv(override=True)

        self.bootstrap_servers = os.getenv('BOOTSTRAP_SERVERS')
        self.socket_url = os.getenv('SOCKET_URL')

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send_reddit_dataframe(self, data: pd.DataFrame):
        try:
            # Convert DataFrame to a dictionary
            data_dict = data.to_dict(orient='records')
            # Send each record in the dictionary
            for record in data_dict:
                self.producer.send("reddit-stream", value=record)
            self.producer.flush()
        except Exception as e:
            print(f"Error: {e}")
            return None
