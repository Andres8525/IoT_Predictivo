"""
Worker de acciones críticas.
Consume de critical_actions_queue y simula el apagado de emergencia del sensor.
"""
import os, json, time, logging
import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s [actuator_worker] %(message)s")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")


def connect():
    for i in range(20):
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            ch = conn.channel()
            ch.queue_declare(queue="critical_actions_queue", durable=True)
            ch.basic_qos(prefetch_count=1)
            return ch
        except Exception as e:
            logging.info("Esperando RabbitMQ (%d/20): %s", i + 1, e)
            time.sleep(5)
    raise RuntimeError("RabbitMQ no disponible")


def on_message(ch, method, props, body):
    cmd = json.loads(body)
    sensor = cmd.get("sensor_id", "?")
    logging.info("=" * 60)
    logging.info("  APAGADO DE EMERGENCIA EJECUTADO")
    logging.info("  Sensor   : %s", sensor)
    logging.info("  Alert ID : %s", cmd.get("alert_id"))
    logging.info("  Acción   : %s", cmd.get("chosen_action"))
    logging.info("  [SIM] Desconectando alimentación del sensor %s...", sensor)
    time.sleep(1)
    logging.info("  [SIM] Sensor %s APAGADO. Sistema en modo seguro.", sensor)
    logging.info("=" * 60)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    ch = connect()
    ch.basic_consume(queue="critical_actions_queue", on_message_callback=on_message)
    logging.info("actuator_worker esperando en critical_actions_queue...")
    ch.start_consuming()


if __name__ == "__main__":
    main()
