"""
Backend de estado de la planta.
- Consume sensor_data, alerts_critical, alerts_warning de Kafka.
- Mantiene en memoria el estado de cada sensor.
- Sirve actualizaciones en tiempo real vía WebSocket en el puerto 8081.
"""
import os, time, json, asyncio, logging, threading
from collections import defaultdict
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [plant_monitor] %(message)s")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
WS_PORT = 8081

# Estado compartido entre el hilo Kafka y el servidor WS
sensor_state: dict[str, dict] = defaultdict(lambda: {"status": "OK", "vibration": 0, "ts": 0})
connected_clients: set = set()
state_lock = threading.Lock()


def kafka_loop():
    """Hilo bloqueante que consume Kafka y actualiza sensor_state."""
    consumer = None
    for i in range(20):
        try:
            consumer = KafkaConsumer(
                "sensor_data", "alerts_critical", "alerts_warning",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="plant_monitor_group",
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode()),
            )
            break
        except NoBrokersAvailable:
            logging.info("Esperando Kafka (%d/20)...", i + 1)
            time.sleep(5)

    if consumer is None:
        raise RuntimeError("Kafka no disponible")

    logging.info("Consumidor Kafka activo (sensor_data, alerts_critical, alerts_warning)")

    for record in consumer:
        msg = record.value
        sid = msg.get("sensor_id")
        if not sid:
            continue

        with state_lock:
            if record.topic == "sensor_data":
                current_status = sensor_state[sid].get("status", "OK")
                sensor_state[sid]["vibration"] = msg["vibration"]
                sensor_state[sid]["ts"] = msg["ts"]
                # Solo regresa a OK si no había alerta
                if current_status == "OK":
                    sensor_state[sid]["status"] = "OK"
            elif record.topic == "alerts_critical":
                sensor_state[sid]["status"] = "CRITICAL"
                sensor_state[sid]["vibration"] = msg.get("vibration", sensor_state[sid]["vibration"])
                sensor_state[sid]["ts"] = msg.get("ts", time.time())
            elif record.topic == "alerts_warning":
                if sensor_state[sid].get("status") != "CRITICAL":
                    sensor_state[sid]["status"] = "WARNING"


async def broadcast_state():
    """Tarea async que envía el estado completo a todos los clientes cada segundo."""
    while True:
        await asyncio.sleep(1)
        if not connected_clients:
            continue
        with state_lock:
            payload = json.dumps({"type": "plant_state", "sensors": dict(sensor_state)})
        dead = set()
        for ws in connected_clients.copy():
            try:
                await ws.send(payload)
            except Exception:
                dead.add(ws)
        connected_clients -= dead


async def ws_handler(websocket):
    connected_clients.add(websocket)
    logging.info("Cliente WS conectado (plant_monitor). Total: %d", len(connected_clients))
    try:
        await websocket.wait_closed()
    finally:
        connected_clients.discard(websocket)
        logging.info("Cliente WS desconectado (plant_monitor). Total: %d", len(connected_clients))


async def main_async():
    # Hilo de Kafka en background
    t = threading.Thread(target=kafka_loop, daemon=True)
    t.start()

    async with websockets.serve(ws_handler, "0.0.0.0", WS_PORT):
        logging.info("WebSocket plant_monitor escuchando en :%d", WS_PORT)
        await asyncio.gather(broadcast_state(), asyncio.Future())


if __name__ == "__main__":
    asyncio.run(main_async())
