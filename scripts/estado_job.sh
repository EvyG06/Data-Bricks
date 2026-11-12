#!/usr/bin/env bash
# Muestra estado, duracion y dependencias de cada tarea en la ultima ejecucion de un job,
# imprime el error de las tareas fallidas y guarda la evidencia en JSON.
# Uso: ./scripts/estado_job.sh job_ventas_evelyn
set -euo pipefail

NOMBRE="${1:?Uso: $0 <nombre_del_job>}"

JOB_ID=$(databricks jobs list -o json | jq -r --arg n "$NOMBRE" '[.[] | select(.settings.name == $n)][0].job_id')
RUN_ID=$(databricks jobs list-runs --job-id "$JOB_ID" -o json | jq -r '.[0].run_id')
DETALLE=$(databricks jobs get-run "$RUN_ID" -o json)

mkdir -p evidencias
echo "$DETALLE" > "evidencias/${NOMBRE}_run_${RUN_ID}.json"

jq '{job_id, run_id, estado_job: .state.result_state, url: .run_page_url}' <<<"$DETALLE"

jq -r '
  ["tarea", "depende_de", "estado", "duracion_s", "mensaje"],
  (.tasks | sort_by([((.start_time // 0) == 0), .start_time])[] | [
    .task_key,
    ([.depends_on[]?.task_key] | join(",") | if . == "" then "-" else . end),
    (.state.result_state // .state.life_cycle_state),
    (if (.start_time // 0) > 0 then ((.end_time - .start_time) / 1000 | floor) else 0 end),
    (.state.state_message // "" | if . == "" then "-" else . end)
  ])
  | @tsv' <<<"$DETALLE" | column -t -s $'\t'

for TAREA in $(jq -r '.tasks[] | select(.state.result_state == "FAILED") | .run_id' <<<"$DETALLE"); do
  echo
  echo "=== Error de la tarea con run_id ${TAREA}"
  databricks jobs get-run-output "$TAREA" -o json | jq -r '.error // "sin mensaje", (.error_trace // "" | .[0:1500])'
done

echo
echo "Evidencia guardada en evidencias/${NOMBRE}_run_${RUN_ID}.json"
