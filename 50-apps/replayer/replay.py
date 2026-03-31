import time
import json
import pandas as pd
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode()
)

df = pd.read_csv("/data/clickstream.csv")
df = df.sort_values("Timestamp")

for _, row in df.iterrows():
    producer.send("clickstream_events", row.to_dict())
    time.sleep(0.2)