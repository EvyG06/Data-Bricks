# Taller Databricks Activity 1

Ingenieria de Datos, Universidad EIA 2026-2. Databricks Free Edition con computo serverless.

## Estructura

```
taller_databricks/
├── databricks.yml              # bundle (Declarative Automation Bundles)
├── src/
│   ├── 01_cargar_ventas.py     # ejercicio 1, crea ventas_bronze
│   ├── 01b_consultar_ventas.py # ejercicio 1, lectura desde otro notebook
│   └── 02_transformar_ventas.py# ejercicio 2, ventas_silver y ventas_por_categoria
├── resources/                  # job (ej. 3) y pipeline (ej. 4)
├── scripts/
│   ├── run_notebook.sh         # corre un notebook en serverless y guarda la evidencia
│   └── sql.sh                  # ejecuta SQL en el warehouse y lo imprime como tabla
└── evidencias/                 # salidas JSON de cada ejecucion
```

## Sesion de terminal

```bash
cd taller_databricks
export DATABRICKS_CONFIG_PROFILE=taller
```

## Ejercicio 1

```bash
databricks schemas create ingenieria_datos_evelyn workspace --comment "Taller Databricks Activity 1"
databricks bundle validate
databricks bundle deploy
./scripts/run_notebook.sh 01_cargar_ventas
databricks tables get workspace.ingenieria_datos_evelyn.ventas_bronze -o json | jq '.columns[] | {name, type_text}'
./scripts/run_notebook.sh 01b_consultar_ventas
```

## Ejercicio 2

```bash
./scripts/run_notebook.sh 02_transformar_ventas
./scripts/run_notebook.sh 02_transformar_ventas   # segunda ejecucion, punto 2.7
./scripts/sql.sh "SELECT * FROM workspace.ingenieria_datos_evelyn.ventas_por_categoria ORDER BY valor_total DESC"
```

## Valores esperados

| Etapa | Registros |
| --- | --- |
| ventas_bronze | 17 |
| sin duplicados | 16 |
| ventas_silver | 13 |

| Categoria | Ventas | Valor total |
| --- | --- | --- |
| Tecnologia | 5 | 1.245.000 |
| Hogar | 4 | 876.000 |
| Papeleria | 4 | 186.500 |
