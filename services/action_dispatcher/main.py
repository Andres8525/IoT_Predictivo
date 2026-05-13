"""
Despachador de acciones.
- Expone POST /decide (HTTP en puerto 8083).
- Recibe { alert_id, sensor_id, chosen_action, type } del dashboard.
- Enruta el comando a la cola RabbitMQ correcta:
    APAGADO_INMEDIATO              -> critical_actions_queue (directo)
    PROGRAMAR_MANTENIMIENTO_AHORA  -> maintenance_queue (directo)
    RECONOCER_Y_ESPERAR_24H        -> maintenance_queue vía Delayed Exchange (24 h)
    IGNORAR_10_MINUTOS             -> maintenance_queue vía Delayed Exchange (10 min)
"""
import os, json, time, logging
from aiohttp import web
import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s [action_dispatcher] %(message)s")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
HTTP_PORT = 8083

DELAY_EXCHANGE = "delayed_actions"
DIRECT_EXCHANGE = ""   # default exchange

_rabbit_ch = None
_rabbit_conn = None


def init_rabbit():
    global _rabbit_ch, _rabbit_conn
    for i in range(20):
        try:
            _rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            _rabbit_ch = _rabbit_conn.channel()

            # Cola para apagados críticos
            _rabbit_ch.queue_declare(queue="critical_actions_queue", durable=True)

            # Cola de mantenimiento
            _rabbit_ch.queue_declare(queue="maintenance_queue", durable=True)

            # Delayed Exchange para tareas diferidas
            _rabbit_ch.exchange_declare(
                exchange=DELAY_EXCHANGE,
                exchange_type="x-delayed-message",
                durable=True,
                arguments={"x-delayed-type": "direct"},
            )
            _rabbit_ch.queue_bind(
                queue="maintenance_queue",
                exchange=DELAY_EXCHANGE,
                routing_key="maintenance",
            )
            logging.info("RabbitMQ listo: colas y exchanges declarados.")
            return
        except Exception as e:
            logging.info("Esperando RabbitMQ (%d/20): %s", i + 1, e)
            time.sleep(5)
    raise RuntimeError("RabbitMQ no disponible")


async def handle_decide(request: web.Request):
    data = await request.json()
    chosen = data.get("chosen_action", "")
    alert_id = data.get("alert_id", "?")
    sensor_id = data.get("sensor_id", "?")

    logging.info("Decisión recibida: alert=%s sensor=%s accion=%s", alert_id, sensor_id, chosen)

    body = json.dumps({
        "alert_id": alert_id,
        "sensor_id": sensor_id,
        "chosen_action": chosen,
        "ts": time.time(),
    }).encode()

    try:
        if chosen == "APAGADO_INMEDIATO":
            _rabbit_ch.basic_publish(
                exchange=DIRECT_EXCHANGE,
                routing_key="critical_actions_queue",
                body=body,
                properties=pika.BasicProperties(delivery_mode=2),
            )
            logging.info("-> critical_actions_queue (inmediato)")

        elif chosen == "PROGRAMAR_MANTENIMIENTO_AHORA":
            _rabbit_ch.basic_publish(
                exchange=DIRECT_EXCHANGE,
                routing_key="maintenance_queue",
                body=body,
                properties=pika.BasicProperties(delivery_mode=2),
            )
            logging.info("-> maintenance_queue (inmediato)")

        elif chosen == "RECONOCER_Y_ESPERAR_24H":
            delay_ms = 24 * 60 * 60 * 1000   # 24 horas en ms
            _rabbit_ch.basic_publish(
                exchange=DELAY_EXCHANGE,
                routing_key="maintenance",
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    headers={"x-delay": delay_ms},
                ),
            )
            logging.info("-> maintenance_queue (delayed 24 h)")

        elif chosen == "IGNORAR_10_MINUTOS":
            delay_ms = 10 * 60 * 1000   # 10 minutos en ms
            _rabbit_ch.basic_publish(
                exchange=DELAY_EXCHANGE,
                routing_key="maintenance",
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    headers={"x-delay": delay_ms},
                ),
            )
            logging.info("-> maintenance_queue (delayed 10 min)")

        else:
            return web.json_response({"error": f"Acción desconocida: {chosen}"}, status=400)

        return web.json_response({"status": "ok", "routed_to": chosen})

    except Exception as e:
        logging.error("Error publicando en RabbitMQ: %s", e)
        return web.json_response({"error": str(e)}, status=500)


async def main_async():
    init_rabbit()
    app = web.Application()
    app.router.add_post("/decide", handle_decide)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HTTP_PORT)
    await site.start()
    logging.info("action_dispatcher HTTP escuchando en :%d/decide", HTTP_PORT)

    import asyncio
    await asyncio.Future()   # mantener vivo


if __name__ == "__main__":
    import asyncio
    asyncio.run(main_async())
