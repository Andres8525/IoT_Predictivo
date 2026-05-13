"""
Consumidor stateful de Kafka.
- Ventana móvil de N lecturas por sensor.
- Regla 1: vibración > 90  -> alerta CRÍTICA
- Regla 2: 3 lecturas > 75 en ventana -> alerta ADVERTENCIA
Publica en topics alerts_critical / alerts_warning.
"""
import os, time, json, logging, uuid
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [alert_detector] %(message)s")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
WINDOW_SIZE = 5          # lecturas por ventana móvil
WARN_THRESHOLD = 75.0
WARN_COUNT = 3
CRIT_THRESHOLD = 90.0


def wait_kafka():
    for i in range(20):
        try:
            p = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP,
                              value_serializer=lambda v: json.dumps(v).encode())
            return p
        except NoBrokersAvailable:
            logging.info("Esperando Kafka (%d/20)...", i + 1)
            time.sleep(5)
    raise RuntimeError("Kafka no disponible")


def main():
    producer = wait_kafka()
    consumer = KafkaConsumer(
        "sensor_data",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="alert_detector_group",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode()),
    )
    logging.info("Detector iniciado, escuchando sensor_data...")

    # Estado en memoria: { sensor_id -> deque de últimas N vibraciones }
    windows: dict[str, deque] = {}

    for record in consumer:
        msg = record.value
        sid = msg["sensor_id"]
        vib = msg["vibration"]

        if sid not in windows:
            windows[sid] = deque(maxlen=WINDOW_SIZE)
        windows[sid].append(vib)

        alert = None

        # Regla 1 – crítica
        if vib > CRIT_THRESHOLD:
            alert = {
                "alert_id": str(uuid.uuid4()),
                "sensor_id": sid,
                "type": "CRITICAL",
                "vibration": vib,
                "ts": msg["ts"],
            }
            topic = "alerts_critical"

        # Regla 2 – advertencia (solo si no es ya crítica)
        elif sum(1 for v in windows[sid] if v > WARN_THRESHOLD) >= WARN_COUNT:
            alert = {
                "alert_id": str(uuid.uuid4()),
                "sensor_id": sid,
                "type": "WARNING",
                "vibration": vib,
                "window": list(windows[sid]),
                "ts": msg["ts"],
            }
            topic = "alerts_warning"

        if alert:
            producer.send(topic, value=alert)
            logging.info("ALERTA %s -> %s vib=%.2f", alert["type"], sid, vib)


if __name__ == "__main__":
    main()
