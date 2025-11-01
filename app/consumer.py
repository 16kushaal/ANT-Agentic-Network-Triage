# in app/consumer.py
import json
from kafka import KafkaConsumer
from app.crew import NetworkTriageCrew

# --- CONFIGURATION ---
KAFKA_TOPIC = 'raw-network-logs'
KAFKA_SERVER = 'localhost:9092'
GROUP_ID = 'triage-crew-consumer-group'

# --- INITIALIZE CREW ---
print("Initializing Network Triage Crew...")
triage_crew_instance = NetworkTriageCrew()
crew = triage_crew_instance.crew()
print("✅ Crew initialized.")

# --- INITIALIZE CONSUMER ---
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset='latest', # Start reading at the newest message
        group_id=GROUP_ID,
        # Deserialize JSON from utf-8
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print(f"Kafka Consumer connected. Listening for logs on topic '{KAFKA_TOPIC}'...")
except Exception as e:
    print(f"Error: Could not connect Kafka Consumer. {e}")
    print("Is your Docker container running? (docker-compose up)")
    exit(1)

# --- MAIN LOOP ---
# Run forever, processing logs as they arrive
try:
    for message in consumer:
        # 1. Get the log from Kafka
        log_data = message.value
        print(f"\n--- [New Log Received - Offset: {message.offset}] ---")
        # print(json.dumps(log_data, indent=2)) # Uncomment to see the full log

        # 2. Run the crew!
        print("🕵️  Kicking off Triage Crew...")
        
        # We must pass the log as a JSON string for the tools
        inputs = {'network_log_message': json.dumps(log_data)}
        
        result = crew.kickoff(inputs=inputs)

        # 3. Print the final report
        print("\n--- [Triage Report Complete] ---")
        try:
            # The result from the synthesis agent should be a JSON string
            report = json.loads(result)
            print(json.dumps(report, indent=2))
        except json.JSONDecodeError:
            print("Error: Final result was not valid JSON.")
            print(result)
        
        print("----------------------------------")
        print(f"Listening for next log...")

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")
except Exception as e:
    print(f"An error occurred during consumption: {e}")
finally:
    consumer.close()
    print("Kafka Consumer closed.")