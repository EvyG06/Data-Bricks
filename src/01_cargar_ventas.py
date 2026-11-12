# Databricks notebook source
# MAGIC %md
# MAGIC # 01_cargar_ventas
# MAGIC Crea el esquema del taller, construye las ventas de la tienda y las guarda como tabla Delta `ventas_bronze`.
# MAGIC
# MAGIC El dataset trae errores a proposito para el ejercicio 2: un duplicado exacto, una venta sin producto,
# MAGIC una cantidad negativa y un precio en cero.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("esquema", "ingenieria_datos_evelyn")

CATALOGO = dbutils.widgets.get("catalogo")
ESQUEMA = dbutils.widgets.get("esquema")
TABLA_BRONZE = f"{CATALOGO}.{ESQUEMA}.ventas_bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
print(f"Destino: {TABLA_BRONZE}")

# COMMAND ----------

from datetime import date
from decimal import Decimal
from pyspark.sql.types import (StructType, StructField, IntegerType,
                               StringType, DateType, DecimalType)

esquema_ventas = StructType([
    StructField("id_venta", IntegerType()),
    StructField("fecha", DateType()),
    StructField("producto", StringType()),
    StructField("categoria", StringType()),
    StructField("cantidad", IntegerType()),
    StructField("precio_unitario", DecimalType(12, 2)),
])

ventas = [
    (1,  date(2026, 11, 1), "Mouse inalambrico",         "Tecnologia", 2,  Decimal("45000")),
    (2,  date(2026, 11, 1), "Teclado mecanico",          "Tecnologia", 1,  Decimal("180000")),
    (3,  date(2026, 11, 2), "Cuaderno argollado",        "Papeleria",  5,  Decimal("8500")),
    (4,  date(2026, 11, 2), "Lampara de escritorio",     "Hogar",      1,  Decimal("95000")),
    (5,  date(2026, 11, 3), "Audifonos bluetooth",       "Tecnologia", 1,  Decimal("150000")),
    (6,  date(2026, 11, 3), "Resma de papel",            "Papeleria",  3,  Decimal("22000")),
    (7,  date(2026, 11, 4), "Silla ergonomica",          "Hogar",      1,  Decimal("650000")),
    (8,  date(2026, 11, 4), "Boligrafos x12",            "Papeleria",  2,  Decimal("15000")),
    (9,  date(2026, 11, 5), "Monitor 24 pulgadas",       "Tecnologia", 1,  Decimal("720000")),
    (10, date(2026, 11, 5), "Organizador de escritorio", "Hogar",      2,  Decimal("38000")),
    (11, date(2026, 11, 6), "Memoria USB 64GB",          "Tecnologia", 3,  Decimal("35000")),
    (12, date(2026, 11, 6), "Cojin lumbar",              "Hogar",      1,  Decimal("55000")),
    (13, date(2026, 11, 7), "Marcadores",                "Papeleria",  4,  Decimal("12000")),
    (5,  date(2026, 11, 3), "Audifonos bluetooth",       "Tecnologia", 1,  Decimal("150000")),  # duplicado exacto
    (14, date(2026, 11, 7), None,                        "Hogar",      2,  Decimal("40000")),   # sin producto
    (15, date(2026, 11, 8), "Cable HDMI",                "Tecnologia", -1, Decimal("25000")),   # cantidad negativa
    (16, date(2026, 11, 8), "Agenda 2027",               "Papeleria",  1,  Decimal("0")),       # precio cero
]

df_ventas = spark.createDataFrame(ventas, schema=esquema_ventas)
display(df_ventas)

# COMMAND ----------

(df_ventas.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLA_BRONZE))

# COMMAND ----------

import json

df_guardada = spark.read.table(TABLA_BRONZE)
display(spark.sql(f"DESCRIBE TABLE {TABLA_BRONZE}"))

dbutils.notebook.exit(json.dumps({
    "tabla": TABLA_BRONZE,
    "registros": df_guardada.count(),
    "columnas": [f"{c.name}:{c.dataType.simpleString()}" for c in df_guardada.schema.fields],
}))
