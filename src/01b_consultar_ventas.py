# Databricks notebook source
# MAGIC %md
# MAGIC # 01b_consultar_ventas
# MAGIC Lee `ventas_bronze` desde un notebook distinto al de carga, usando el nombre completo `catalogo.esquema.tabla`.

# COMMAND ----------

import json
from pyspark.sql import functions as F

dbutils.widgets.text("categoria", "Tecnologia")
CATEGORIA = dbutils.widgets.get("categoria")

# Nombre completo escrito literal a proposito, es lo que pide el punto 1.5
df = spark.read.table("workspace.ingenieria_datos_evelyn.ventas_bronze")

total = df.count()
print(f"Total de registros en ventas_bronze: {total}")

# COMMAND ----------

df_categoria = df.filter(F.col("categoria") == CATEGORIA).orderBy("id_venta")
display(df_categoria)

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "total_registros": total,
    "categoria": CATEGORIA,
    "filas_categoria": df_categoria.count(),
    "ventas_categoria": [r.asDict() for r in df_categoria.collect()],
}, default=str))
