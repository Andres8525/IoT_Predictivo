# IoT-Predictivo: Plataforma de Mantenimiento Predictivo Interactivo

Sistema distribuido de monitoreo industrial en tiempo real que demuestra la interoperabilidad entre Apache Kafka y RabbitMQ en una arquitectura orientada a eventos con interacción humana en el lazo de decisiones.

## Tabla de Contenidos

- [Resumen del Proyecto](#resumen-del-proyecto)
- [Arquitectura](#arquitectura)
- [Decisiones de Diseño](#decisiones-de-diseño)
- [Stack Tecnológico](#stack-tecnológico)
- [Estructura del Repositorio](#estructura-del-repositorio)
- [Cómo Levantar el Proyecto](#cómo-levantar-el-proyecto)
- [Cómo Probar el Sistema](#cómo-probar-el-sistema)
- [Flujo de Datos End-to-End](#flujo-de-datos-end-to-end)
- [Microservicios](#microservicios)

---

## Resumen del Proyecto

IoT-Predictivo simula el sistema de monitoreo de una planta industrial con diez sensores de vibración distribuidos. El sistema ingesta un flujo masivo y continuo de telemetría, aplica reglas de detección de fallos con estado (stateful), presenta las alertas generadas a un operador humano y ejecuta la acción correctiva elegida por ese operador, enrutando el comando al worker apropiado.

El diseño separa deliberadamente dos flujos con características radicalmente distintas: el **flujo de datos** de alta velocidad, donde los sensores emiten lecturas cada fracción de segundo y se requiere procesamiento stateful con ventanas temporales, y el **flujo de decisiones**, donde interviene un ser humano que debe evaluar opciones y tomar una resolución que puede diferirse horas o días.

El sistema está compuesto por ocho microservicios personalizados, todos implementados en Python, orquestados con Docker Compose. La arquitectura demuestra que la elección del broker correcto para cada tipo de flujo no es una preferencia estética sino una decisión de ingeniería que afecta directamente la escalabilidad, el orden de procesamiento y la semántica de entrega de mensajes.

---

## Arquitectura

El sistema implementa el patrón **Detección-Escalada-Decisión-Ejecución**, donde el flujo de telemetría cruda es procesado por Kafka y los eventos de alto nivel (alertas que requieren intervención humana) son gestionados por RabbitMQ.

```
[sensor_producer] --> Kafka (sensor_data) --> [alert_detector]
                                                     |
                          ┌──────────────────────────┴──────────────────────────┐
                          v                                                       v
              Kafka (alerts_critical/warning)                    Kafka (alerts_critical/warning)
                          |                                                       |
                          v                                                       v
             [plant_monitor_backend]                                    [alert_router]
                          |                                                       |
                     WebSocket :8081                              RabbitMQ Fanout (human_alerts)
                          |                                                       |
                          v                                                       v
                   [dashboard.html] <──── WebSocket :8082 ── [operator_console_backend]
                          |
                     Operador hace clic
                          |
                          v
             POST /decide → [action_dispatcher]
                                    |
                    ┌───────────────┼───────────────┐
                    v               v               v
          critical_actions_queue  maintenance_queue  delayed_actions exchange
                    |               |                       |
                    v               v                       v
          [actuator_worker]  [maintenance_worker]  [maintenance_worker] (delayed)
```

### Diagrama de Componentes

```
                        ┌──────────────────────────────────────────────────────┐
                        │              FLUJO DE DATOS (Kafka)                  │
                        │                                                      │
  [sensor_producer]─────┤──► sensor_data (topic, key=sensor_id)               │
  10 sensores A-J       │          │                                           │
                        │          ▼                                           │
                        │   [alert_detector]  ◄── ventana móvil por sensor    │
                        │     Regla 1: >90 → CRITICAL                         │
                        │     Regla 2: 3x >75 → WARNING                       │
                        │          │                                           │
                        │    ┌─────┴─────┐                                    │
                        │    ▼           ▼                                     │
                        │ alerts_critical  alerts_warning (topics Kafka)       │
                        │    │           │                                     │
                        │    ├───────────┤ ◄── [plant_monitor_backend]        │
                        │    │           │         WebSocket :8081             │
                        │    └───────────┘ ◄── [alert_router]                 │
                        └──────────────────────────────────────────────────────┘
                                               │
                        ┌──────────────────────▼───────────────────────────────┐
                        │           FLUJO DE DECISIONES (RabbitMQ)             │
                        │                                                      │
                        │   Fanout Exchange: human_alerts                      │
                        │          │                                           │
                        │          ▼                                           │
                        │  [operator_console_backend]  WebSocket :8082         │
                        │          │                                           │
                        │          ▼                                           │
                        │     [dashboard.html]  → operador elige acción        │
                        │          │                                           │
                        │          ▼  POST /decide                             │
                        │   [action_dispatcher]  HTTP :8083                    │
                        │          │                                           │
                        │    ┌─────┼──────────────────┐                       │
                        │    ▼     ▼                   ▼                       │
                        │ critical maintenance   delayed_actions               │
                        │ _actions_ _queue       exchange (x-delay)            │
                        │ _queue       │               │                       │
                        │    │         └───────────────┘                       │
                        │    ▼                 ▼                               │
                        │ [actuator_    [maintenance_                          │
                        │   worker]       worker]                              │
                        └──────────────────────────────────────────────────────┘
```

---

## Decisiones de Diseño

Esta sección responde las preguntas de arquitectura fundamentales del proyecto y justifica cada decisión técnica.

### 1. Por qué Kafka para telemetría y RabbitMQ para decisiones

La distinción entre **flujo de datos** y **flujo de decisiones** es el eje central de esta arquitectura. El flujo de telemetría tiene características que lo hacen ideal para Kafka: altísimo volumen (diez sensores emitiendo continuamente), necesidad de procesamiento stateful con ventanas temporales, y múltiples consumidores independientes que necesitan leer los mismos datos sin interferirse (el `plant_monitor_backend` y el `alert_router` consumen las mismas alertas de forma paralela e independiente).

El flujo de decisiones tiene características opuestas: cada alerta debe presentarse exactamente una vez al operador (semántica de trabajo, no de log), las acciones pueden diferirse arbitrariamente en el tiempo (horas o días), y el enrutamiento de la decisión debe basarse en el contenido del mensaje. RabbitMQ con su modelo de colas, exchanges y el plugin de delayed messages es la herramienta precisa para este propósito.

Usar Kafka para decidir qué operador recibe una alerta, o RabbitMQ para almacenar el histórico de telemetría de mil sensores, sería un error de adecuación herramienta-problema.

### 2. Por qué sensor_id como key en Kafka

Apache Kafka garantiza el orden de los mensajes únicamente dentro de una misma partición. Al usar `sensor_id` como key, se asegura que todas las lecturas de un sensor concreto caen en la misma partición y se procesan en orden estricto de llegada.

Esto es crítico para el `alert_detector`: la ventana móvil (`deque`) que mantiene en memoria para la Regla 2 solo tiene sentido si las lecturas llegan en orden cronológico. Si las lecturas del `sensor_A` se distribuyeran aleatoriamente entre particiones, podría ocurrir que la ventana acumule lecturas desordenadas, produciendo falsos positivos o falsos negativos en la detección.

Como beneficio adicional, sensores distintos pueden procesarse en paralelo en distintas particiones, habilitando el escalado horizontal del `alert_detector` sin sacrificar la consistencia por sensor.

### 3. Por qué un consumidor stateful con ventana móvil

La Regla 1 (umbral puntual) es trivial: una sola lectura basta para dispararla. La Regla 2 (patrón temporal) requiere **memoria**: el sistema debe recordar las últimas N lecturas de cada sensor para determinar si se ha superado el umbral `k` veces dentro de esa ventana.

Implementar esto con una base de datos externa añadiría latencia, complejidad operacional y un punto de fallo extra. En cambio, el `alert_detector` mantiene un `dict[sensor_id -> deque(maxlen=5)]` en memoria. Dado que el particionamiento por `sensor_id` garantiza que todas las lecturas de un sensor van siempre a la misma instancia del consumidor, el estado en memoria es correcto, eficiente y no requiere sincronización entre réplicas.

Esta decisión materializa el patrón **Stateful Stream Processing**: el estado vive junto al procesamiento, en la misma unidad de ejecución que consume la partición correspondiente.

### 4. Por qué Fanout Exchange para human_alerts

Cuando el `alert_router` publica una alerta en RabbitMQ, potencialmente más de un sistema necesita recibirla: el `operator_console_backend` para mostrarla al operador, y en una evolución futura podría agregarse un sistema de auditoría, un servicio de SMS a supervisores, o una integración con un sistema SCADA. Con un **Fanout Exchange**, cualquier nuevo consumidor se vincula a su propia cola y recibe todas las alertas sin modificar ni el router ni los consumidores existentes.

Un Direct Exchange exigiría conocer de antemano todas las routing keys de todos los consumidores, acoplando al emisor con sus receptores.

### 5. Por qué el Delayed Message Exchange para RECONOCER_Y_ESPERAR_24H

Cuando el operador decide posponer el mantenimiento 24 horas, el sistema debe garantizar que ese comando se ejecute exactamente 24 horas después, incluso si todos los servicios reinician en ese intervalo. Implementar esta lógica en la aplicación (un `asyncio.sleep` o un `time.sleep`) significaría perder el comando si el proceso termina.

El plugin `rabbitmq_delayed_message_exchange` delega esta responsabilidad al broker: el mensaje se almacena persistentemente en RabbitMQ y solo se entrega a la cola destino cuando transcurre el `x-delay` configurado. El comando sobrevive a reinicios de la aplicación y a caídas del servicio.

Esta decisión demuestra que RabbitMQ no es solo un sistema de mensajería simple, sino una plataforma que puede gestionar flujos de trabajo con semántica temporal compleja.

### 6. Por qué separar operator_console_backend y action_dispatcher

Una alternativa más simple habría sido que el `operator_console_backend` manejara tanto la recepción de alertas (desde RabbitMQ) como el procesamiento de las decisiones (publicar en las colas correctas). Sin embargo, esto violaría el **principio de responsabilidad única**.

`operator_console_backend` tiene una única responsabilidad: ser el canal de comunicación con el operador humano, escuchar alertas de RabbitMQ y transmitirlas por WebSocket. No debe conocer la lógica de enrutamiento de acciones.

`action_dispatcher` tiene una única responsabilidad: recibir una decisión ya tomada y enrutarla a la cola correcta según reglas de negocio. No debe conocer cómo se presentó la alerta al operador.

Esta separación permite que la lógica de enrutamiento evolucione independientemente de la interfaz de usuario, y que la consola del operador se modifique (por ejemplo, migrar de WebSocket a Server-Sent Events) sin tocar el despachador.

---

## Stack Tecnológico

### Infraestructura

| Componente | Versión | Propósito |
|---|---|---|
| Apache Kafka | Confluent 7.5.0 | Log distribuido de telemetría y alertas |
| Zookeeper | Confluent 7.5.0 | Coordinación del clúster Kafka |
| RabbitMQ | 3.12 con management | Work queues, Fanout Exchange y Delayed Exchange |
| Docker Compose | v3.8 | Orquestación de los diez contenedores |

### Microservicios (Python 3.11)

| Librería | Uso |
|---|---|
| `kafka-python 2.0.2` | Productor y consumidor de Kafka |
| `pika 1.3.2` | Cliente de RabbitMQ |
| `websockets 12.0` | Servidor WebSocket para los dashboards |
| `aiohttp 3.9.5` | Servidor HTTP asíncrono para el action_dispatcher |

---

## Estructura del Repositorio

```
IoT-Predictivo/
├── docker-compose.yml
├── README.md
├── .gitignore
├── test.sh
├── dashboard.html
└── services/
    ├── sensor_producer/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── main.py
    ├── alert_detector/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── main.py
    ├── alert_router/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── main.py
    ├── plant_monitor_backend/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── main.py
    ├── operator_console_backend/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── main.py
    ├── action_dispatcher/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── main.py
    ├── actuator_worker/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── main.py
    └── maintenance_worker/
        ├── Dockerfile
        ├── requirements.txt
        └── main.py
```

---

## Cómo Levantar el Proyecto

### Requisitos previos

- Docker y Docker Compose instalados en el sistema
- Puertos disponibles: `8081` (plant monitor WS), `8082` (operator console WS), `8083` (action dispatcher HTTP), `15672` (RabbitMQ UI), `9092` (Kafka)

### Comandos de ejecución

```bash
git clone <url-del-repositorio>
cd IoT-Predictivo
docker compose up --build
```

La primera ejecución puede tardar entre dos y tres minutos. Los microservicios aplican un mecanismo de reintento con 20 intentos y espera de 5 segundos entre cada uno, lo que permite que Kafka y RabbitMQ completen su inicialización antes de que los servicios dependientes intenten conectarse.

Para correr en segundo plano y ver logs luego:

```bash
docker compose up --build -d
docker compose logs -f
```

### Interfaces de acceso

| Interfaz | URL | Descripción |
|---|---|---|
| Dashboard del operador | Abrir `dashboard.html` en el navegador | Estado de planta + consola de acciones |
| Panel de administración RabbitMQ | `http://localhost:15672` | Usuario: `guest` / Contraseña: `guest` |
| API action_dispatcher | `http://localhost:8083/decide` | Endpoint POST para enviar decisiones |

### Comandos útiles de gestión

```bash
# Ver estado de todos los contenedores
docker compose ps

# Ver logs de un servicio específico
docker compose logs -f alert_detector
docker compose logs -f actuator_worker
docker compose logs -f maintenance_worker

# Reiniciar un servicio sin bajar el stack
docker compose restart alert_detector

# Detener todo el sistema
docker compose down

# Detener y eliminar volúmenes (reset completo)
docker compose down -v
```

---

## Cómo Probar el Sistema

### Opción 1: Suite de pruebas automatizada completa

```bash
chmod +x test.sh

# Ejecutar todas las secciones
./test.sh

# O ejecutar solo una sección específica
./test.sh docker    # estado de contenedores y recursos
./test.sh kafka     # topics, particiones y consumer groups
./test.sh rabbit    # exchanges, colas, bindings y plugins
./test.sh api       # pruebas de integración del action_dispatcher
```

### Opción 2: Pruebas de Docker

```bash
# Estado de los 10 contenedores
docker compose ps

# Uso de CPU y memoria en tiempo real
docker stats sensor_producer alert_detector alert_router \
  plant_monitor_backend operator_console_backend action_dispatcher \
  actuator_worker maintenance_worker kafka rabbitmq

# Inspeccionar la red interna iot_net
docker network inspect iot-predictivo_iot_net

# Verificar que el plugin delayed_message_exchange está activo
docker compose exec rabbitmq rabbitmq-plugins list --enabled
```

### Opción 3: Pruebas de Kafka

```bash
# Listar todos los topics creados por el sistema
docker compose exec kafka \
  kafka-topics --bootstrap-server localhost:9092 --list

# Ver detalle (particiones y réplicas) de un topic
docker compose exec kafka \
  kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic sensor_data

# Consumir mensajes de telemetría en tiempo real (Ctrl+C para salir)
docker compose exec kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sensor_data

# Consumir alertas críticas en tiempo real
docker compose exec kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic alerts_critical

# Consumir alertas de advertencia en tiempo real
docker compose exec kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic alerts_warning

# Ver los consumer groups registrados
docker compose exec kafka \
  kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Ver el lag del consumidor stateful (alert_detector)
# El lag indica cuántos mensajes están pendientes de procesar
docker compose exec kafka \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group alert_detector_group

# Ver el lag del consumidor del dashboard de estado
docker compose exec kafka \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group plant_monitor_group

# Ver el lag del alert_router (puente Kafka→RabbitMQ)
docker compose exec kafka \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group alert_router_group
```

### Opción 4: Pruebas de RabbitMQ (Management API)

```bash
# Ver todos los exchanges declarados
curl -s -u guest:guest http://localhost:15672/api/exchanges/%2F \
  | python3 -m json.tool

# Verificar que el Fanout Exchange human_alerts existe
curl -s -u guest:guest \
  http://localhost:15672/api/exchanges/%2F/human_alerts

# Verificar que el Delayed Exchange delayed_actions existe
curl -s -u guest:guest \
  http://localhost:15672/api/exchanges/%2F/delayed_actions

# Ver todas las colas con conteo de mensajes
curl -s -u guest:guest http://localhost:15672/api/queues/%2F \
  | python3 -m json.tool

# Ver mensajes en critical_actions_queue
curl -s -u guest:guest \
  http://localhost:15672/api/queues/%2F/critical_actions_queue

# Ver mensajes en maintenance_queue
curl -s -u guest:guest \
  http://localhost:15672/api/queues/%2F/maintenance_queue

# Ver bindings del Fanout Exchange (cuántos consumidores están vinculados)
curl -s -u guest:guest \
  "http://localhost:15672/api/exchanges/%2F/human_alerts/bindings/source" \
  | python3 -m json.tool

# Ver conexiones activas al broker
curl -s -u guest:guest http://localhost:15672/api/connections \
  | python3 -m json.tool

# Ver canales activos
curl -s -u guest:guest http://localhost:15672/api/channels \
  | python3 -m json.tool

# Publicar un mensaje de prueba directamente en critical_actions_queue
# (simula que action_dispatcher ya enrutó una decisión)
curl -s -u guest:guest \
  -X POST http://localhost:15672/api/exchanges/%2F/amq.default/publish \
  -H "Content-Type: application/json" \
  -d '{
    "properties": {"delivery_mode": 2},
    "routing_key": "critical_actions_queue",
    "payload": "{\"alert_id\":\"rabbit-test\",\"sensor_id\":\"sensor_Z\",\"chosen_action\":\"APAGADO_INMEDIATO\"}",
    "payload_encoding": "string"
  }'
```

### Opción 5: Pruebas de integración del action_dispatcher

```bash
# Test 1: APAGADO_INMEDIATO → critical_actions_queue (inmediato)
curl -s -X POST http://localhost:8083/decide \
  -H "Content-Type: application/json" \
  -d '{
    "alert_id": "test-001",
    "sensor_id": "sensor_A",
    "chosen_action": "APAGADO_INMEDIATO",
    "type": "CRITICAL"
  }' | python3 -m json.tool

# Test 2: PROGRAMAR_MANTENIMIENTO_AHORA → maintenance_queue (inmediato)
curl -s -X POST http://localhost:8083/decide \
  -H "Content-Type: application/json" \
  -d '{
    "alert_id": "test-002",
    "sensor_id": "sensor_B",
    "chosen_action": "PROGRAMAR_MANTENIMIENTO_AHORA",
    "type": "WARNING"
  }' | python3 -m json.tool

# Test 3: RECONOCER_Y_ESPERAR_24H → delayed_actions (x-delay: 86400000 ms)
# Verificar en RabbitMQ UI que el mensaje queda "encolado con delay"
curl -s -X POST http://localhost:8083/decide \
  -H "Content-Type: application/json" \
  -d '{
    "alert_id": "test-003",
    "sensor_id": "sensor_C",
    "chosen_action": "RECONOCER_Y_ESPERAR_24H",
    "type": "WARNING"
  }' | python3 -m json.tool

# Test 4: IGNORAR_10_MINUTOS → delayed_actions (x-delay: 600000 ms)
curl -s -X POST http://localhost:8083/decide \
  -H "Content-Type: application/json" \
  -d '{
    "alert_id": "test-004",
    "sensor_id": "sensor_D",
    "chosen_action": "IGNORAR_10_MINUTOS",
    "type": "CRITICAL"
  }' | python3 -m json.tool

# Test 5: Acción inválida (debe retornar HTTP 400)
curl -s -o /dev/null -w "HTTP Status: %{http_code}\n" \
  -X POST http://localhost:8083/decide \
  -H "Content-Type: application/json" \
  -d '{"chosen_action": "ACCION_INEXISTENTE"}'
```

### Opción 6: Flujo completo desde el dashboard

1. Levantar el sistema con `docker compose up --build`
2. Abrir `dashboard.html` en el navegador (doble clic en el archivo)
3. Observar cómo los 10 sensores aparecen en el mapa con sus valores de vibración actualizándose cada segundo
4. Esperar a que un sensor alcance un umbral de alerta. Los picos ocurren aproximadamente cada 15–20 segundos según la distribución estadística del productor
5. Cuando aparezca una tarjeta de "Acción Requerida" en el panel derecho, hacer clic en una de las opciones generadas dinámicamente
6. Verificar el resultado en los logs de Docker

```bash
# Ver el apagado ejecutado por el actuator_worker
docker compose logs actuator_worker

# Ver la orden de mantenimiento creada por el maintenance_worker
docker compose logs maintenance_worker
```

### Comportamiento esperado por escenario

| Escenario | Dashboard | actuator_worker | maintenance_worker |
|---|---|---|---|
| Vibración > 90 (Regla 1) | Sensor en rojo pulsante + tarjeta CRITICAL | — | — |
| Operador elige APAGADO_INMEDIATO | Tarjeta desaparece (4s) | Imprime simulacro de apagado | — |
| Operador elige IGNORAR_10_MINUTOS | Tarjeta desaparece (4s) | — | Imprime orden tras 10 min |
| 3 lecturas > 75 (Regla 2) | Sensor en amarillo + tarjeta WARNING | — | — |
| Operador elige PROGRAMAR_MANTENIMIENTO_AHORA | Tarjeta desaparece (4s) | — | Imprime orden inmediatamente |
| Operador elige RECONOCER_Y_ESPERAR_24H | Tarjeta desaparece (4s) | — | Imprime orden tras 24 h |

---

## Flujo de Datos End-to-End

1. `sensor_producer` genera lecturas aleatorias para cada uno de los diez sensores (A–J) y las publica en el topic `sensor_data` de Kafka usando el `sensor_id` como key
2. `alert_detector` consume `sensor_data`, mantiene una ventana móvil de 5 lecturas por sensor en memoria, y evalúa la Regla 1 (vibración > 90 → CRITICAL) y la Regla 2 (3 lecturas > 75 en la ventana → WARNING)
3. Cuando se dispara una regla, `alert_detector` publica la alerta en `alerts_critical` o `alerts_warning` de Kafka
4. **En paralelo**, dos consumidores independientes reaccionan al mismo topic de alerta:
   - `plant_monitor_backend` actualiza su mapa de estado en memoria y lo transmite por WebSocket al `dashboard.html`, que pinta el sensor en amarillo (WARNING) o rojo con pulso (CRITICAL)
   - `alert_router` consume la alerta, genera aleatoriamente las opciones de acción según el tipo, y publica el mensaje enriquecido en el Fanout Exchange `human_alerts` de RabbitMQ
5. `operator_console_backend` consume su cola vinculada al Fanout y reenvía el mensaje por WebSocket al `dashboard.html`
6. El `dashboard.html` renderiza una tarjeta de "Acción Requerida" con botones dinámicos generados a partir del array `options` del mensaje
7. El operador humano hace clic en una opción; el dashboard envía `POST /decide` al `action_dispatcher` con `{ alert_id, sensor_id, chosen_action }`
8. `action_dispatcher` evalúa `chosen_action` y enruta:
   - `APAGADO_INMEDIATO` → publica directamente en `critical_actions_queue`
   - `PROGRAMAR_MANTENIMIENTO_AHORA` → publica directamente en `maintenance_queue`
   - `RECONOCER_Y_ESPERAR_24H` → publica en `delayed_actions` exchange con `x-delay: 86400000 ms`
   - `IGNORAR_10_MINUTOS` → publica en `delayed_actions` exchange con `x-delay: 600000 ms`
9. `actuator_worker` (si recibe de `critical_actions_queue`) o `maintenance_worker` (si recibe de `maintenance_queue`) consumen el comando y ejecutan la acción simulada, imprimiendo el resultado en consola

---

## Microservicios

| Servicio | Puerto | Rol | Input | Output |
|---|---|---|---|---|
| `sensor_producer` | — | Simula telemetría de 10 sensores | — | Kafka `sensor_data` (key=sensor_id) |
| `alert_detector` | — | Procesamiento stateful con ventana móvil | Kafka `sensor_data` | Kafka `alerts_critical` / `alerts_warning` |
| `alert_router` | — | Puente Kafka → RabbitMQ; genera opciones de acción | Kafka `alerts_*` | RabbitMQ Fanout `human_alerts` |
| `plant_monitor_backend` | WS :8081 | Mantiene estado de planta y lo transmite en vivo | Kafka `sensor_data` + `alerts_*` | WebSocket a `dashboard.html` |
| `operator_console_backend` | WS :8082 | Retransmite alertas con opciones al operador | RabbitMQ `human_alerts` | WebSocket a `dashboard.html` |
| `action_dispatcher` | HTTP :8083 | Recibe decisión humana y enruta al worker correcto | POST `/decide` | RabbitMQ (colas directas y delayed) |
| `actuator_worker` | — | Ejecuta apagado de emergencia simulado | RabbitMQ `critical_actions_queue` | Consola (simulación) |
| `maintenance_worker` | — | Crea orden de mantenimiento simulada | RabbitMQ `maintenance_queue` | Consola (simulación) |

---

## Autores

**Equipo 3 — Curso de Sistemas Distribuidos**

- Montero, Edison Andrés
- Arias, Ricardo Armando
- Calzada, Juan Rafael
