"""
Worker de mantenimiento.
Consume de maintenance_queue (mensajes inmediatos y mensajes con delay del Delayed Exchange).
Simula la creación de una orden de trabajo de mantenimiento.
"""
import os, json, time, logging
import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s [maintenance_worker] %(message)s")

RABBITMQ_HOST  = os.getenv("RABBITMQ_HOST", "localhost")
DELAY_EXCHANGE = "delayed_actions"


def connect():
    for i in range(20):
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            ch = conn.channel()

            # Declarar la misma cola que action_dispatcher ya declara
            ch.queue_declare(queue="maintenance_queue", durable=True)

            # Asegurarse de que el delayed exchange existe
            ch.exchange_declare(
                exchange=DELAY_EXCHANGE,
                exchange_type="x-delayed-message",
                durable=True,
                arguments={"x-delayed-type": "direct"},
            )
            ch.queue_bind(
                queue="maintenance_queue",
                exchange=DELAY_EXCHANGE,
                routing_key="maintenance",
            )
            ch.basic_qos(prefetch_count=1)
            return ch
        except Exception as e:
            logging.info("Esperando RabbitMQ (%d/20): %s", i + 1, e)
            time.sleep(5)
    raise RuntimeError("RabbitMQ no disponible")


_order_counter = 0


def on_message(ch, method, props, body):
    global _order_counter
    _order_counter += 1
    cmd = json.loads(body)
    sensor  = cmd.get("sensor_id", "?")
    action  = cmd.get("chosen_action", "?")

    logging.info("*" * 60)
    logging.info("  ORDEN DE MANTENIMIENTO #%04d CREADA", _order_counter)
    logging.info("  Sensor   : %s", sensor)
    logging.info("  Alert ID : %s", cmd.get("alert_id"))
    logging.info("  Acción   : %s", action)
    logging.info("  [SIM] Asignando técnico al sensor %s...", sensor)
    time.sleep(0.5)
    logging.info("  [SIM] Orden #%04d registrada en el sistema CMMS.", _order_counter)
    logging.info("*" * 60)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    ch = connect()
    ch.basic_consume(queue="maintenance_queue", on_message_callback=on_message)
    logging.info("maintenance_worker esperando en maintenance_queue...")
    ch.start_consuming()


if __name__ == "__main__":
    main()
