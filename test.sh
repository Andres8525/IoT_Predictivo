#!/usr/bin/env bash
# test.sh — Prueba básica de humo del sistema IoT Predictivo
# Uso: ./test.sh
set -e

BASE_DISPATCH="http://localhost:8083"

echo "============================================================"
echo " Test 1: Action Dispatcher — APAGADO_INMEDIATO"
echo "============================================================"
curl -s -X POST "$BASE_DISPATCH/decide" \
  -H "Content-Type: application/json" \
  -d '{"alert_id":"test-001","sensor_id":"sensor_A","chosen_action":"APAGADO_INMEDIATO","type":"CRITICAL"}' \
  | python3 -m json.tool
echo ""

echo "============================================================"
echo " Test 2: Action Dispatcher — PROGRAMAR_MANTENIMIENTO_AHORA"
echo "============================================================"
curl -s -X POST "$BASE_DISPATCH/decide" \
  -H "Content-Type: application/json" \
  -d '{"alert_id":"test-002","sensor_id":"sensor_B","chosen_action":"PROGRAMAR_MANTENIMIENTO_AHORA","type":"WARNING"}' \
  | python3 -m json.tool
echo ""

echo "============================================================"
echo " Test 3: Action Dispatcher — RECONOCER_Y_ESPERAR_24H (delayed)"
echo "============================================================"
curl -s -X POST "$BASE_DISPATCH/decide" \
  -H "Content-Type: application/json" \
  -d '{"alert_id":"test-003","sensor_id":"sensor_C","chosen_action":"RECONOCER_Y_ESPERAR_24H","type":"WARNING"}' \
  | python3 -m json.tool
echo ""

echo "============================================================"
echo " Test 4: Acción desconocida (debe retornar 400)"
echo "============================================================"
curl -s -o /dev/null -w "HTTP Status: %{http_code}\n" \
  -X POST "$BASE_DISPATCH/decide" \
  -H "Content-Type: application/json" \
  -d '{"chosen_action":"ACCION_INEXISTENTE"}'
echo ""

echo "Todos los tests completados."
