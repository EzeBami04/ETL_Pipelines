import json
import logging
import os
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigtable
from google.cloud.bigtable.row import DirectRow
import websocket

# Set Google Cloud environment variables
project_id = "data-pipeline-443605"
big_table_id = "binancerealtime"
os.environ["GOOGLE_CLOUD_PROJECT"] = project_id
os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "C:/Users/HP USER/Downloads/data-pipeline-443605-57e57c67be42.json"

# WebSocket URL
websocket_url = "wss://testnet.binance.vision/ws/btcusdt@trade"

# DoFn to read data from WebSocket
class ReadFromWebSocket(beam.DoFn):
    def __init__(self, websocket_url):
        self.websocket_url = websocket_url

    def process(self, unused_element):
        ws = websocket.WebSocket()
        try:
            ws.connect(self.websocket_url)
            while True:
                message = ws.recv()
                if message:
                    try:
                        data = json.loads(message)
                        yield data
                    except json.JSONDecodeError as e:
                        logging.error(f"Error decoding JSON: {e}")
        except Exception as e:
            logging.error(f"Error with WebSocket connection: {e}")
        finally:
            ws.close()

# Pipeline options
options = PipelineOptions(
    project=project_id,
    region="europe-west6",
    runner="DataflowRunner",
    temp_location="gs://binance-bucket-id-temp",
    staging_location="gs://binance-bucket-id-staging",
    save_main_session=True,
    streaming=True,
)



# DoFn to format data for Bigtable
class FormatData(beam.DoFn):
    def process(self, data):
        try:
            # Extract fields
            event_type = data["e"]
            event_time = data["E"]
            symbol = data["s"]
            trade_id = data["t"]
            price = data["p"]
            quantity = data["q"]
            buyer_order_id = data.get("b", "")
            seller_order_id = data.get("a", "")
            trade_time = data["T"]
            is_buyer_maker = data["m"]

            # Format row key
            row_key = f"{symbol}-{trade_id}".encode("utf-8")
            bt_row = DirectRow(row_key)

            # Set cells
            bt_row.set_cell("trade", "event_type", event_type.encode("utf-8"))
            bt_row.set_cell("trade", "event_time", str(event_time).encode("utf-8"))
            bt_row.set_cell("trade", "symbol", symbol.encode("utf-8"))
            bt_row.set_cell("trade", "trade_id", str(trade_id).encode("utf-8"))
            bt_row.set_cell("trade", "price", str(price).encode("utf-8"))
            bt_row.set_cell("trade", "quantity", str(quantity).encode("utf-8"))
            bt_row.set_cell("trade", "buyer_order_id", str(buyer_order_id).encode("utf-8"))
            bt_row.set_cell("trade", "seller_order_id", str(seller_order_id).encode("utf-8"))
            bt_row.set_cell("trade", "trade_time", str(trade_time).encode("utf-8"))
            bt_row.set_cell("trade", "is_buyer_maker", str(is_buyer_maker).encode("utf-8"))

            yield bt_row
        except Exception as e:
            logging.error(f"Error formatting data: {e}")
            raise

# DoFn to write data to Bigtable
class WriteToBigTable(beam.DoFn):
    def __init__(self, project_id, instance_id, table_id):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id

    def start_bundle(self):
        client = bigtable.Client(project=self.project_id, admin=True)
        self.instance = client.instance(self.instance_id)
        self.table = self.instance.table(self.table_id)

    def process(self, row):
        try:
            row.commit()
            logging.info(f"Row committed: {row.row_key.decode('utf-8')}")
        except Exception as e:
            logging.error(f"Error writing row to Bigtable: {e}")

# Build and run the pipeline
def run_pipeline():
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "CreateDummy" >> beam.Create([None])
            | "ReadFromWebSocket" >> beam.ParDo(ReadFromWebSocket(websocket_url))
            | "FormatData" >> beam.ParDo(FormatData())
            | "WriteToBigTable" >> beam.ParDo(
                WriteToBigTable(
                    project_id=project_id,
                    instance_id="binancerealtime",
                    table_id=big_table_id,
                )
            )
        )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_pipeline()



