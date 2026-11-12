#!/usr/bin/env bash
# Ejecuta una sentencia SQL en el SQL warehouse del workspace (Statement Execution API)
# e imprime el resultado como tabla.
# Uso: ./scripts/sql.sh "SELECT COUNT(*) FROM workspace.ingenieria_datos_evelyn.ventas_bronze"
set -euo pipefail

SQL="${1:?Uso: $0 \"<sentencia SQL>\"}"

WH_ID="${WAREHOUSE_ID:-$(databricks warehouses list -o json | jq -r '.[0].id')}"

CUERPO=$(jq -n --arg w "$WH_ID" --arg s "$SQL" \
  '{warehouse_id: $w, statement: $s, wait_timeout: "30s", on_wait_timeout: "CONTINUE"}')

RESP=$(databricks api post /api/2.0/sql/statements --json "$CUERPO")
ID=$(jq -r '.statement_id' <<<"$RESP")

# Si el warehouse estaba apagado la sentencia queda PENDING mientras arranca
while [[ "$(jq -r '.status.state' <<<"$RESP")" =~ ^(PENDING|RUNNING)$ ]]; do
  sleep 3
  RESP=$(databricks api get "/api/2.0/sql/statements/${ID}")
done

if [[ "$(jq -r '.status.state' <<<"$RESP")" != "SUCCEEDED" ]]; then
  jq '.status' <<<"$RESP"
  exit 1
fi

TABLA=$(jq -r '([.manifest.schema.columns[].name] | @tsv),
       ((.result.data_array // [])[] | map(. // "NULL") | @tsv)' <<<"$RESP")

if command -v column >/dev/null 2>&1; then
  column -t -s $'\t' <<<"$TABLA"
else
  echo "$TABLA"
fi
