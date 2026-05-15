#!/usr/bin/env bash
# =============================================================================
# test.sh — Suite de pruebas para IoT-Predictivo
# Uso: ./test.sh [seccion]
#   Secciones: docker | kafka | rabbit | api | all (default: all)
# =============================================================================
set -e

SECTION="${1:-all}"
SEP="------------------------------------------------------------"

# Colores para la salida
COLOR_INFO='\033[0;36m'
COLOR_RESET='\033[0m'

info() { echo -e "${COLOR_INFO}→ $*${COLOR_RESET}"; }

run_docker() {
    echo "=== SECCIÓN: DOCKER ==="
    info "Estado de los 10 contenedores:"
    docker compose ps
    
    echo "$SEP"
    info "Verificando plugin delayed en RabbitMQ:"
    docker compose exec rabbitmq rabbitmq-plugins list --enabled | grep delayed
}

run_kafka() {
    echo "=== SECCIÓN: KAFKA ==="
    info "Topics en tiempo real (sensor_data):"
    timeout 5 docker compose exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 --topic sensor_data --max-messages 5 || true

    echo "$SEP"
    info "Alertas críticas (alerts_critical):"
    timeout 5 docker compose exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 --topic alerts_critical --max-messages 1 --from-beginning 2>/dev/null || echo "No hay alertas críticas aún."

    echo "$SEP"
    info "Lag del consumidor stateful (alert_detector_group):"
    docker compose exec kafka kafka-consumer-groups \
        --bootstrap-server localhost:9092 --describe --group alert_detector_group
}

run_rabbit() {
    echo "=== SECCIÓN: RABBITMQ ==="
    info "Ver colas y mensajes pendientes:"
    curl -s -u guest:guest http://localhost:15672/api/queues/%2F | python3 -m json.tool | grep -E "name|messages\"|consumers"

    echo "$SEP"
    info "Verificar Fanout Exchange (human_alerts):"
    curl -s -u guest:guest http://localhost:15672/api/exchanges/%2F/human_alerts | python3 -m json.tool | grep -E "name|type"

    echo "$SEP"
    info "Verificar Delayed Exchange (delayed_actions):"
    curl -s -u guest:guest http://localhost:15672/api/exchanges/%2F/delayed_actions | python3 -m json.tool | grep -E "name|type"

    echo "$SEP"
    info "Ver bindings del fanout (consumidores conectados):"
    curl -s -u guest:guest "http://localhost:15672/api/exchanges/%2F/human_alerts/bindings/source" | python3 -m json.tool
}

run_api() {
    echo "=== SECCIÓN: API (action_dispatcher) ==="
    info "Enviando acción de prueba (APAGADO_INMEDIATO):"
    curl -s -X POST http://localhost:8083/decide \
        -H "Content-Type: application/json" \
        -d '{"alert_id":"t1","sensor_id":"sensor_A","chosen_action":"APAGADO_INMEDIATO","type":"CRITICAL"}' \
        | python3 -m json.tool
}

case "$SECTION" in
    docker)
        run_docker
        ;;
    kafka)
        run_kafka
        ;;
    rabbit)
        run_rabbit
        ;;
    api)
        run_api
        ;;
    all)
        run_docker
        echo ""
        run_kafka
        echo ""
        run_rabbit
        echo ""
        run_api
        ;;
    *)
        echo "Uso: $0 {docker|kafka|rabbit|api}"
        exit 1
esac
