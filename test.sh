#!/usr/bin/env bash
# =============================================================================
# test.sh — Suite de pruebas para IoT-Predictivo
# Uso: ./test.sh [seccion]
#   Secciones: docker | kafka | rabbit | api | all (default: all)
# =============================================================================
set -e

RABBIT_API="http://localhost:15672/api"
RABBIT_CREDS="guest:guest"
DISPATCH_API="http://localhost:8083"
SEP="============================================================"

COLOR_OK='\033[0;32m'
COLOR_ERR='\033[0;31m'
COLOR_INFO='\033[0;36m'
COLOR_RESET='\033[0m'

ok()   { echo -e "${COLOR_OK}  ✔ $*${COLOR_RESET}"; }
err()  { echo -e "${COLOR_ERR}  ✘ $*${COLOR_RESET}"; }
info() { echo -e "${COLOR_INFO}  → $*${COLOR_RESET}"; }

SECTION="${1:-all}"

# =============================================================================
# SECCIÓN 1 — DOCKER
# =============================================================================
run_docker_tests() {
  echo ""
  echo "$SEP"
  echo " DOCKER — Estado de contenedores"
  echo "$SEP"

  info "Contenedores en ejecución:"
  docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

  echo ""
  info "Verificando que los 10 contenedores estén Up..."
  RUNNING=$(docker compose ps --status running --format "{{.Name}}" | wc -l | tr -d ' ')
  if [ "$RUNNING" -ge 10 ]; then
    ok "Todos los contenedores están corriendo ($RUNNING/10)"
  else
    err "Solo $RUNNING/10 contenedores están corriendo"
    info "Ver logs: docker compose logs --tail=30 <servicio>"
  fi

  echo ""
  info "Uso de recursos (CPU/RAM):"
  docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" \
    sensor_producer alert_detector alert_router \
    plant_monitor_backend operator_console_backend action_dispatcher \
    actuator_worker maintenance_worker kafka rabbitmq 2>/dev/null || true
}

# =============================================================================
# SECCIÓN 2 — KAFKA
# =============================================================================
run_kafka_tests() {
  echo ""
  echo "$SEP"
  echo " KAFKA — Topics, Particiones y Consumer Groups"
  echo "$SEP"

  info "Listando todos los topics:"
  docker compose exec -T kafka \
    kafka-topics --bootstrap-server localhost:9092 --list
  echo ""

  info "Detalle de topics del sistema:"
  for TOPIC in sensor_data alerts_critical alerts_warning; do
    echo "  ── $TOPIC ──"
    docker compose exec -T kafka \
      kafka-topics --bootstrap-server localhost:9092 --describe --topic "$TOPIC" 2>/dev/null \
      || echo "    (topic aún no creado)"
  done
  echo ""

  info "Consumer groups registrados:"
  docker compose exec -T kafka \
    kafka-consumer-groups --bootstrap-server localhost:9092 --list 2>/dev/null
  echo ""

  info "Lag del grupo alert_detector_group (ventana móvil stateful):"
  docker compose exec -T kafka \
    kafka-consumer-groups --bootstrap-server localhost:9092 \
    --describe --group alert_detector_group 2>/dev/null || echo "  (grupo aún no activo)"

  echo ""
  info "Lag del grupo plant_monitor_group:"
  docker compose exec -T kafka \
    kafka-consumer-groups --bootstrap-server localhost:9092 \
    --describe --group plant_monitor_group 2>/dev/null || echo "  (grupo aún no activo)"

  echo ""
  info "Muestra de 5 mensajes del topic sensor_data (Ctrl+C para salir si cuelga):"
  timeout 6 docker compose exec -T kafka \
    kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic sensor_data \
    --max-messages 5 \
    --from-beginning 2>/dev/null || true

  echo ""
  info "Muestra de 3 alertas críticas (si existen):"
  timeout 6 docker compose exec -T kafka \
    kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic alerts_critical \
    --max-messages 3 \
    --from-beginning 2>/dev/null || echo "  (sin alertas críticas aún)"
}

# =============================================================================
# SECCIÓN 3 — RABBITMQ
# =============================================================================
run_rabbit_tests() {
  echo ""
  echo "$SEP"
  echo " RABBITMQ — Exchanges, Colas y Bindings"
  echo "$SEP"

  info "Verificando que RabbitMQ responde..."
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" -u "$RABBIT_CREDS" "$RABBIT_API/overview")
  if [ "$STATUS" = "200" ]; then
    ok "RabbitMQ Management API respondiendo (HTTP $STATUS)"
  else
    err "RabbitMQ no responde (HTTP $STATUS) — ¿está corriendo en :15672?"
    return
  fi

  echo ""
  info "Exchanges declarados:"
  curl -s -u "$RABBIT_CREDS" "$RABBIT_API/exchanges/%2F" \
    | python3 -c "
import sys, json
exchanges = json.load(sys.stdin)
for e in exchanges:
    if e['name']:
        print(f\"  {e['name']:35s}  type={e['type']:12s}  durable={e['durable']}\")
"

  echo ""
  info "Verificando exchange 'human_alerts' (fanout):"
  RESULT=$(curl -s -u "$RABBIT_CREDS" "$RABBIT_API/exchanges/%2F/human_alerts" 2>/dev/null)
  if echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); exit(0 if d.get('type')=='fanout' else 1)" 2>/dev/null; then
    ok "human_alerts existe, tipo=fanout"
  else
    err "human_alerts no encontrado o tipo incorrecto"
  fi

  echo ""
  info "Verificando exchange 'delayed_actions' (x-delayed-message):"
  RESULT=$(curl -s -u "$RABBIT_CREDS" "$RABBIT_API/exchanges/%2F/delayed_actions" 2>/dev/null)
  if echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); exit(0 if d.get('type')=='x-delayed-message' else 1)" 2>/dev/null; then
    ok "delayed_actions existe, tipo=x-delayed-message"
  else
    err "delayed_actions no encontrado — ¿está activo el plugin rabbitmq_delayed_message_exchange?"
  fi

  echo ""
  info "Colas activas y conteo de mensajes:"
  curl -s -u "$RABBIT_CREDS" "$RABBIT_API/queues/%2F" \
    | python3 -c "
import sys, json
queues = json.load(sys.stdin)
if not queues:
    print('  (sin colas declaradas aún)')
else:
    print(f\"  {'Cola':40s}  {'Mensajes':>8s}  {'Consumers':>9s}\")
    print('  ' + '-'*62)
    for q in queues:
        print(f\"  {q['name']:40s}  {q.get('messages',0):>8d}  {q.get('consumers',0):>9d}\")
"

  echo ""
  info "Bindings del exchange human_alerts:"
  curl -s -u "$RABBIT_CREDS" "$RABBIT_API/exchanges/%2F/human_alerts/bindings/source" \
    | python3 -c "
import sys, json
bindings = json.load(sys.stdin)
if not bindings:
    print('  (sin bindings — operator_console_backend aún no conectó)')
else:
    for b in bindings:
        print(f\"  Queue: {b.get('destination','?')}\")
"

  echo ""
  info "Plugins habilitados (verificando delayed_message_exchange):"
  docker compose exec -T rabbitmq rabbitmq-plugins list --enabled 2>/dev/null \
    | grep -i delayed || echo "  (no se pudo verificar — intentar: docker compose exec rabbitmq rabbitmq-plugins list)"

  echo ""
  info "Conexiones activas a RabbitMQ:"
  curl -s -u "$RABBIT_CREDS" "$RABBIT_API/connections" \
    | python3 -c "
import sys, json
conns = json.load(sys.stdin)
if not conns:
    print('  (sin conexiones activas)')
else:
    for c in conns:
        print(f\"  {c.get('name','?'):50s}  state={c.get('state','?')}\")
"
}

# =============================================================================
# SECCIÓN 4 — API (action_dispatcher)
# =============================================================================
run_api_tests() {
  echo ""
  echo "$SEP"
  echo " API — action_dispatcher POST /decide"
  echo "$SEP"

  info "Test 1: APAGADO_INMEDIATO → critical_actions_queue"
  RESP=$(curl -s -w "\nHTTP_STATUS:%{http_code}" \
    -X POST "$DISPATCH_API/decide" \
    -H "Content-Type: application/json" \
    -d '{"alert_id":"test-001","sensor_id":"sensor_A","chosen_action":"APAGADO_INMEDIATO","type":"CRITICAL"}')
  STATUS=$(echo "$RESP" | grep "HTTP_STATUS" | cut -d: -f2)
  BODY=$(echo "$RESP" | grep -v "HTTP_STATUS")
  if [ "$STATUS" = "200" ]; then
    ok "HTTP $STATUS — $BODY"
  else
    err "HTTP $STATUS — $BODY"
  fi

  echo ""
  info "Test 2: PROGRAMAR_MANTENIMIENTO_AHORA → maintenance_queue (inmediato)"
  RESP=$(curl -s -w "\nHTTP_STATUS:%{http_code}" \
    -X POST "$DISPATCH_API/decide" \
    -H "Content-Type: application/json" \
    -d '{"alert_id":"test-002","sensor_id":"sensor_B","chosen_action":"PROGRAMAR_MANTENIMIENTO_AHORA","type":"WARNING"}')
  STATUS=$(echo "$RESP" | grep "HTTP_STATUS" | cut -d: -f2)
  BODY=$(echo "$RESP" | grep -v "HTTP_STATUS")
  if [ "$STATUS" = "200" ]; then
    ok "HTTP $STATUS — $BODY"
  else
    err "HTTP $STATUS — $BODY"
  fi

  echo ""
  info "Test 3: RECONOCER_Y_ESPERAR_24H → delayed_actions exchange (x-delay 24h)"
  RESP=$(curl -s -w "\nHTTP_STATUS:%{http_code}" \
    -X POST "$DISPATCH_API/decide" \
    -H "Content-Type: application/json" \
    -d '{"alert_id":"test-003","sensor_id":"sensor_C","chosen_action":"RECONOCER_Y_ESPERAR_24H","type":"WARNING"}')
  STATUS=$(echo "$RESP" | grep "HTTP_STATUS" | cut -d: -f2)
  BODY=$(echo "$RESP" | grep -v "HTTP_STATUS")
  if [ "$STATUS" = "200" ]; then
    ok "HTTP $STATUS — $BODY"
    info "Verificar en RabbitMQ UI → delayed_actions exchange → mensaje en espera 24h"
  else
    err "HTTP $STATUS — $BODY"
  fi

  echo ""
  info "Test 4: IGNORAR_10_MINUTOS → delayed_actions exchange (x-delay 10min)"
  RESP=$(curl -s -w "\nHTTP_STATUS:%{http_code}" \
    -X POST "$DISPATCH_API/decide" \
    -H "Content-Type: application/json" \
    -d '{"alert_id":"test-004","sensor_id":"sensor_D","chosen_action":"IGNORAR_10_MINUTOS","type":"CRITICAL"}')
  STATUS=$(echo "$RESP" | grep "HTTP_STATUS" | cut -d: -f2)
  BODY=$(echo "$RESP" | grep -v "HTTP_STATUS")
  if [ "$STATUS" = "200" ]; then
    ok "HTTP $STATUS — $BODY"
  else
    err "HTTP $STATUS — $BODY"
  fi

  echo ""
  info "Test 5: Acción inválida → debe retornar HTTP 400"
  RESP=$(curl -s -w "\nHTTP_STATUS:%{http_code}" \
    -X POST "$DISPATCH_API/decide" \
    -H "Content-Type: application/json" \
    -d '{"alert_id":"test-005","sensor_id":"sensor_E","chosen_action":"ACCION_INEXISTENTE"}')
  STATUS=$(echo "$RESP" | grep "HTTP_STATUS" | cut -d: -f2)
  BODY=$(echo "$RESP" | grep -v "HTTP_STATUS")
  if [ "$STATUS" = "400" ]; then
    ok "HTTP $STATUS (correcto) — $BODY"
  else
    err "Esperado HTTP 400, obtenido $STATUS — $BODY"
  fi

  echo ""
  info "Verificando que critical_actions_queue recibió el mensaje del Test 1:"
  MSG_COUNT=$(curl -s -u "$RABBIT_CREDS" "$RABBIT_API/queues/%2F/critical_actions_queue" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('messages_ready',0))" 2>/dev/null)
  # El worker puede haberlo consumido ya; ambos casos son válidos
  info "Mensajes en critical_actions_queue: ${MSG_COUNT:-?} (0 = ya consumido por actuator_worker)"

  echo ""
  info "Verificando que maintenance_queue recibió el mensaje del Test 2:"
  MSG_COUNT=$(curl -s -u "$RABBIT_CREDS" "$RABBIT_API/queues/%2F/maintenance_queue" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('messages_ready',0))" 2>/dev/null)
  info "Mensajes en maintenance_queue: ${MSG_COUNT:-?} (0 = ya consumido por maintenance_worker)"
}

# =============================================================================
# RESUMEN LOGS
# =============================================================================
show_logs_hint() {
  echo ""
  echo "$SEP"
  echo " COMANDOS ÚTILES DE LOGS"
  echo "$SEP"
  echo ""
  echo "  # Todos los servicios en tiempo real:"
  echo "  docker compose logs -f"
  echo ""
  echo "  # Solo el cerebro (alert_detector):"
  echo "  docker compose logs -f alert_detector"
  echo ""
  echo "  # Ver apagados ejecutados por actuator_worker:"
  echo "  docker compose logs actuator_worker"
  echo ""
  echo "  # Ver órdenes de mantenimiento creadas:"
  echo "  docker compose logs maintenance_worker"
  echo ""
  echo "  # Consumir sensor_data en tiempo real desde terminal:"
  echo "  docker compose exec kafka kafka-console-consumer \\"
  echo "    --bootstrap-server localhost:9092 --topic sensor_data"
  echo ""
  echo "  # Consumir alertas críticas en tiempo real:"
  echo "  docker compose exec kafka kafka-console-consumer \\"
  echo "    --bootstrap-server localhost:9092 --topic alerts_critical"
  echo ""
  echo "  # Ver mensajes en cola (RabbitMQ Management API):"
  echo "  curl -s -u guest:guest http://localhost:15672/api/queues/%2F | python3 -m json.tool"
  echo ""
  echo "  # Reiniciar un servicio sin bajar todo el stack:"
  echo "  docker compose restart alert_detector"
  echo ""
  echo "  # Escalar sensor_producer a 2 instancias:"
  echo "  docker compose up -d --scale sensor_producer=2"
}

# =============================================================================
# MAIN
# =============================================================================
echo ""
echo "  IoT-Predictivo — Suite de Pruebas"
echo "  Sección: $SECTION"

case "$SECTION" in
  docker) run_docker_tests ;;
  kafka)  run_kafka_tests  ;;
  rabbit) run_rabbit_tests ;;
  api)    run_api_tests    ;;
  all)
    run_docker_tests
    run_kafka_tests
    run_rabbit_tests
    run_api_tests
    show_logs_hint
    ;;
  *)
    echo "Sección desconocida: $SECTION"
    echo "Uso: ./test.sh [docker|kafka|rabbit|api|all]"
    exit 1
    ;;
esac

echo ""
echo "$SEP"
echo " Suite de pruebas completada."
echo "$SEP"
echo ""
