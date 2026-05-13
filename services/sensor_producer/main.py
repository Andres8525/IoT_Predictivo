import os, time, random, json, logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [sensor_producer] %(message)s")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "sensor_data"
SENSORS = [f"sensor_{c}" for c in "ABCDEFGHIJ"]


def make_producer():
    for attempt in range(20):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode(),
            )
        except NoBrokersAvailable:
            logging.info("Kafka no disponible, reintentando (%d/20)...", attempt + 1)
            time.sleep(5)
    raise RuntimeError("No se pudo conectar a Kafka")


def main():
    producer = make_producer()
    logging.info("Productor iniciado. Publicando en '%s'", TOPIC)
    while True:
        sensor_id = random.choice(SENSORS)
        # La mayoría del tiempo valores normales; ocasionalmente picos de alerta
        rand = random.random()
        if rand < 0.05:
            vibration = round(random.uniform(91, 100), 2)   # crítica
        elif rand < 0.20:
            vibration = round(random.uniform(76, 90), 2)    # advertencia
        else:
            vibration = round(random.uniform(10, 74), 2)    # normal

        msg = {"sensor_id": sensor_id, "vibration": vibration, "ts": time.time()}
        producer.send(TOPIC, key=sensor_id, value=msg)
        logging.info("  -> %s  vibration=%.2f", sensor_id, vibration)
        time.sleep(0.8)


if __name__ == "__main__":
    main()
