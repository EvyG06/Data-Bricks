# Databricks notebook source
# MAGIC %md
# MAGIC # 02_transformar_ventas
# MAGIC Lee `ventas_bronze`, elimina duplicados exactos, aplica reglas de calidad y publica
# MAGIC `ventas_silver` y `ventas_por_categoria`. Re-ejecutarlo no acumula registros porque ambas
# MAGIC escrituras usan overwrite.

# COMMAND ----------

import json
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("esquema", "ingenieria_datos_evelyn")

BASE = f"{dbutils.widgets.get('catalogo')}.{dbutils.widgets.get('esquema')}"
BRONZE = f"{BASE}.ventas_bronze"
SILVER = f"{BASE}.ventas_silver"
POR_CATEGORIA = f"{BASE}.ventas_por_categoria"

# COMMAND ----------

df_bronze = spark.read.table(BRONZE)
df_unicos = df_bronze.dropDuplicates()


def es_verdadero(condicion):
    """NULL cuenta como falso. Sin esto, una cantidad NULL no entra ni a validas ni a rechazadas."""
    return F.coalesce(condicion, F.lit(False))


producto_ok = es_verdadero(F.col("producto").isNotNull() & (F.trim(F.col("producto")) != ""))
cantidad_ok = es_verdadero(F.col("cantidad") > 0)
precio_ok = es_verdadero(F.col("precio_unitario") > 0)
es_valida = producto_ok & cantidad_ok & precio_ok

df_silver = (
    df_unicos
    .filter(es_valida)
    .withColumn("total_venta", (F.col("cantidad") * F.col("precio_unitario")).cast("decimal(18,2)"))
)

df_rechazadas = (
    df_unicos
    .filter(~es_valida)
    .withColumn("motivo", F.concat_ws(", ",
        F.when(~producto_ok, F.lit("sin producto")),
        F.when(~cantidad_ok, F.lit("cantidad <= 0")),
        F.when(~precio_ok, F.lit("precio <= 0")),
    ))
)
display(df_rechazadas.orderBy("id_venta"))

# COMMAND ----------

(df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER))

df_por_categoria = (
    spark.read.table(SILVER)
    .groupBy("categoria")
    .agg(
        F.count("*").alias("num_ventas"),
        F.sum("total_venta").alias("valor_total"),
    )
)

(df_por_categoria.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(POR_CATEGORIA))

display(spark.read.table(POR_CATEGORIA).orderBy(F.desc("valor_total")))

# COMMAND ----------

historial_silver = (
    spark.sql(f"DESCRIBE HISTORY {SILVER}")
    .select(
        "version",
        "timestamp",
        "operation",
        F.col("operationMetrics").getItem("numOutputRows").alias("filas_escritas"),
    )
    .orderBy(F.desc("version"))
    .limit(5)
)
display(historial_silver)

resumen = {
    "registros_bronze": df_bronze.count(),
    "despues_de_quitar_duplicados": df_unicos.count(),
    "rechazadas_por_calidad": [
        {"id_venta": r["id_venta"], "motivo": r["motivo"]}
        for r in df_rechazadas.orderBy("id_venta").collect()
    ],
    "registros_silver": spark.read.table(SILVER).count(),
    "ventas_por_categoria": [
        r.asDict() for r in spark.read.table(POR_CATEGORIA).orderBy(F.desc("valor_total")).collect()
    ],
    "historial_silver": [r.asDict() for r in historial_silver.collect()],
}
dbutils.notebook.exit(json.dumps(resumen, default=str))
