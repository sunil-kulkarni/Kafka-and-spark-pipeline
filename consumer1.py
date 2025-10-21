from kafka import KafkaConsumer
import csv
import json
import threading

BROKER = 'BROKER_ZERO_TIER_IP'

def consume_topic(topic, csv_file, fieldnames, batch_size=100):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id=f'group-{topic}'
    )
    with open(csv_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        batch = []
        for msg in consumer:
            batch.append(msg.value)
            if len(batch) >= batch_size:
                writer.writerows(batch)
                f.flush()
                batch.clear()
        if batch:
            writer.writerows(batch)
            f.flush()

# Run each topic consumer in its own thread
threading.Thread(target=consume_topic, args=('topic-cpu', 'cpu_data.csv', ['ts', 'server_id', 'cpu_pct']), daemon=True).start()
threading.Thread(target=consume_topic, args=('topic-mem', 'mem_data.csv', ['ts', 'server_id', 'mem_pct']), daemon=True).start()

# Keep main thread alive
threading.Event().wait()
