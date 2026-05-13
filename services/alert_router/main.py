"""
Puente Kafka -> RabbitMQ.
Consume alerts_critical y alerts_warning de Kafka.
Genera opciones de acción aleatoria y publica en el Fanout Exchange 'human_alerts' de RabbitMQ.
"""
import os, time, json, random, logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s [alert_router] %(message)s")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RABBITMQ_HOST   = os.getenv("RABBITMQ_HOST", "localhost")

CRITICAL_OPTIONS = ["APAGADO_INMEDIATO", "IGNORAR_10_MINUTOS"]
WARNING_OPTIONS  = ["PROGRAMAR_MANTENIMIENTO_AHORA", "RECONOCER_Y_ESPERAR_24H"]

FANOUT_EXCHANGE = "human_alerts"


def wait_kafka():
    for i in range(20):
        try:
            return KafkaConsumer(
                "alerts_critical", "alerts_warning",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="alert_router_group",
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode()),
            )
        except NoBrokersAvailable:
            logging.info("Esperando Kafka (%d/20)...", i + 1)
            time.sleep(5)
    raise RuntimeError("Kafka no disponible")


def wait_rabbit():
    for i in range(20):
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            ch = conn.channel()
            ch.exchange_declare(exchange=FANOUT_EXCHANGE, exchange_type="fanout", durable=True)
            logging.info("RabbitMQ conectado, exchange '%s' declarado.", FANOUT_EXCHANGE)
            return conn, ch
        except Exception as e:
            logging.info("Esperando RabbitMQ (%d/20): %s", i + 1, e)
            time.sleep(5)
    raise RuntimeError("RabbitMQ no disponible")


def main():
    consumer = wait_kafka()
    conn, ch = wait_rabbit()
    logging.info("Router iniciado.")

    for record in consumer:
        alert = record.value
        alert_type = alert.get("type", "WARNING")

        # Selección aleatoria de opciones según tipo
        if alert_type == "CRITICAL":
            options = random.sample(CRITICAL_OPTIONS, len(CRITICAL_OPTIONS))
        else:
            options = random.sample(WARNING_OPTIONS, len(WARNING_OPTIONS))

        payload = {
            "alert_id": alert["alert_id"],
            "sensor_id": alert["sensor_id"],
            "type": alert_type,
            "vibration": alert.get("vibration"),
            "options": options,
            "ts": alert.get("ts"),
        }

        ch.basic_publish(
            exchange=FANOUT_EXCHANGE,
            routing_key="",
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        logging.info("Publicado en human_alerts: sensor=%s tipo=%s opciones=%s",
                     payload["sensor_id"], alert_type, options)


if __name__ == "__main__":
    main()
