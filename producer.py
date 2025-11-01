# in producer.py
import pandas as pd
from kafka import KafkaProducer
import json
import time

# --- CONFIGURATION ---
KAFKA_TOPIC = 'raw-network-logs'
KAFKA_SERVER = 'localhost:9092'
DATA_PATH = './data/CIC-2017.csv'
SIMULATION_SPEED_SEC = 0.5 # Time to wait between sending logs

# --- CLEAN COLUMN NAMES ---
# CSV headers from CIC datasets often have leading/trailing spaces
def clean_col_names(df):
    df.columns = df.columns.str.strip()
    return df

# --- INITIALIZE PRODUCER ---
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        # Serialize values as JSON and encode to utf-8
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"Kafka Producer connected to {KAFKA_SERVER}")
except Exception as e:
    print(f"Error: Could not connect Kafka Producer. {e}")
    print("Is your Docker container running? (docker-compose up)")
    exit(1)

# --- LOAD AND STREAM DATA ---
try:
    print(f"Loading data from {DATA_PATH}...")
    df = pd.read_csv(DATA_PATH)
    df = clean_col_names(df)
    print(f"Data loaded. Found {len(df)} records.")
    
    # Replace NaN with null for valid JSON
    df = df.fillna(value=pd.NA).astype(str).where(pd.notna(df), None)

    print(f"\n🚀 Starting stream to topic '{KAFKA_TOPIC}'...")
    print("Press Ctrl+C to stop.")

    for index, row in df.iterrows():
        log_message = row.to_dict()
        
        # Send to Kafka
        producer.send(KAFKA_TOPIC, value=log_message)
        
        print(f"Sent log {index+1}/{len(df)}", end='\r')
        
        # Wait to simulate a real-time stream
        time.sleep(SIMULATION_SPEED_SEC)

except FileNotFoundError:
    print(f"Error: Data file not found at {DATA_PATH}")
except KeyboardInterrupt:
    print("\nStream stopped by user.")
except Exception as e:
    print(f"\nAn error occurred during streaming: {e}")
finally:
    producer.flush()
    producer.close()
    print("Kafka Producer closed.")