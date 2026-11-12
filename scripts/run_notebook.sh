#!/usr/bin/env bash
# Ejecuta un notebook del bundle como run unico en serverless, espera a que termine,
# muestra estado, resultado de dbutils.notebook.exit o el error, y guarda la evidencia.
# Uso: ./scripts/run_notebook.sh 01_cargar_ventas
set -euo pipefail

NB="${1:?Uso: $0 <nombre_notebook_sin_extension>}"
BUNDLE="taller_databricks"
TARGET="dev"

USUARIO=$(databricks current-user me -o json | jq -r '.userName')
RUTA="/Workspace/Users/${USUARIO}/.bundle/${BUNDLE}/${TARGET}/files/src/${NB}"

CUERPO=$(jq -n --arg nb "$RUTA" --arg nombre "manual_${NB}" \
  '{run_name: $nombre, tasks: [{task_key: "nb", notebook_task: {notebook_path: $nb}}]}')

RUN_ID=$(databricks jobs submit --no-wait --json "$CUERPO" -o json | jq -r '.run_id')
echo "run_id=${RUN_ID} ejecutando ${RUTA}"

while true; do
  ESTADO=$(databricks jobs get-run "$RUN_ID" -o json | jq -r '.state.life_cycle_state')
  case "$ESTADO" in
    TERMINATED|SKIPPED|INTERNAL_ERROR) break ;;
  esac
  printf '.'
  sleep 5
done
echo

RUN=$(databricks jobs get-run "$RUN_ID" -o json)
TASK_RUN_ID=$(jq -r '.tasks[0].run_id' <<<"$RUN")

mkdir -p evidencias
SALIDA="evidencias/${NB}_$(date +%Y%m%d_%H%M%S).json"
databricks jobs get-run-output "$TASK_RUN_ID" -o json > "$SALIDA"

jq '{estado: .state.result_state, mensaje: .state.state_message, url: .run_page_url}' <<<"$RUN"
jq '{resultado: ((.notebook_output.result // "null") | try fromjson catch .), error, error_trace}' "$SALIDA"
echo "Evidencia guardada en ${SALIDA}"
