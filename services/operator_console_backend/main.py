"""
Consola del operador.
- Consume de RabbitMQ (Fanout 'human_alerts').
- Reenvía las alertas (con opciones) a todos los clientes WebSocket en el puerto 8082.
El dashboard usa este WebSocket para mostrar las acciones requeridas al operador.
"""
import os, json, asyncio, logging, threading, time
import pika
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [operator_console] %(message)s")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
WS_PORT = 8082

connected_clients: set = set()
_alert_queue: asyncio.Queue | None = None


def start_rabbit_consumer(loop: asyncio.AbstractEventLoop):
    conn = None
    for i in range(20):
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            break
        except Exception as e:
            logging.info("Esperando RabbitMQ (%d/20): %s", i + 1, e)
            time.sleep(5)

    if conn is None:
        raise RuntimeError("RabbitMQ no disponible")

    ch = conn.channel()
    ch.exchange_declare(exchange="human_alerts", exchange_type="fanout", durable=True)
    result = ch.queue_declare(queue="", exclusive=True)
    queue_name = result.method.queue
    ch.queue_bind(exchange="human_alerts", queue=queue_name)
    logging.info("Escuchando human_alerts en cola '%s'", queue_name)

    def on_message(_ch, _method, _props, body):
        payload = json.loads(body)
        logging.info("Alerta -> operador: sensor=%s tipo=%s opciones=%s",
                     payload.get("sensor_id"), payload.get("type"), payload.get("options"))
        asyncio.run_coroutine_threadsafe(_alert_queue.put(payload), loop)

    ch.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=True)
    ch.start_consuming()


async def alert_broadcaster():
    global connected_clients
    while True:
        alert = await _alert_queue.get()
        payload = json.dumps({"type": "action_required", **alert})
        dead = set()
        for ws in connected_clients.copy():
            try:
                await ws.send(payload)
            except Exception:
                dead.add(ws)
        connected_clients -= dead


async def ws_handler(websocket):
    connected_clients.add(websocket)
    logging.info("Cliente WS conectado (operator_console). Total: %d", len(connected_clients))
    try:
        await websocket.wait_closed()
    finally:
        connected_clients.discard(websocket)
        logging.info("Cliente WS desconectado. Total: %d", len(connected_clients))


async def main_async():
    global _alert_queue
    _alert_queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    t = threading.Thread(target=start_rabbit_consumer, args=(loop,), daemon=True)
    t.start()

    async with websockets.serve(ws_handler, "0.0.0.0", WS_PORT):
        logging.info("WebSocket operator_console escuchando en :%d", WS_PORT)
        await asyncio.gather(alert_broadcaster(), asyncio.Future())


if __name__ == "__main__":
    asyncio.run(main_async())
