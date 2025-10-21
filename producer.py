import csv
from kafka import KafkaProducer
import json
import sys
import time

# Kafka Broker IP and Port
BROKER = 'BROKER IP:PORT'

# Configure Kafka producer for efficient high-speed sending
producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=50,       
    batch_size=32768,  
    retries=3,          
    acks='1'            
)

BATCH_SIZE = 100
count = 0
start_time = time.time()

try:
    with open('./assets/dataset.csv', mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                # Send records to their respective topics
                producer.send('topic-cpu', {
                    'ts': row['ts'],
                    'server_id': row['server_id'],
                    'cpu_pct': row['cpu_pct']
                })

                producer.send('topic-mem', {
                    'ts': row['ts'],
                    'server_id': row['server_id'],
                    'mem_pct': row['mem_pct']
                })

                producer.send('topic-net', {
                    'ts': row['ts'],
                    'server_id': row['server_id'],
                    'net_in': row['net_in'],
                    'net_out': row['net_out']
                })

                producer.send('topic-disk', {
                    'ts': row['ts'],
                    'server_id': row['server_id'],
                    'disk_io': row['disk_io']
                })

                count += 4  
                if count % BATCH_SIZE == 0:
                    producer.flush()
                    print(f"[INFO] Sent {count} messages so far...")

            except Exception as e:
                print(f"[ERROR] Failed to send record {count}: {e}", file=sys.stderr)

    # Final batch flush
    producer.flush()
    print(f"[SUCCESS] All messages sent successfully. Total messages: {count}")

except FileNotFoundError:
    print("[ERROR] 'dataset.csv' not found. Ensure your file is in the same directory.")
except Exception as e:
    print(f"[ERROR] Unexpected error occurred: {e}", file=sys.stderr)
finally:
    producer.close()
    total_time = time.time() - start_time
    print(f"[INFO] Total execution time: {total_time:.2f} seconds")
