from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Valores que llegan desde la seccion configuration del pipeline
CATALOGO = spark.conf.get("tienda.catalogo")
ESQUEMA = spark.conf.get("tienda.esquema")
BRONZE = f"{CATALOGO}.{ESQUEMA}.ventas_bronze"

# Reglas del ejercicio 2. COALESCE convierte NULL en falso para que un NULL no pase la regla.
REGLAS_CALIDAD = {
    "producto_valido": "COALESCE(producto IS NOT NULL AND trim(producto) <> '', FALSE)",
    "cantidad_valida": "COALESCE(cantidad > 0, FALSE)",
    "precio_valido": "COALESCE(precio_unitario > 0, FALSE)",
}


@dp.materialized_view(
    name="pipeline_ventas_limpias",
    comment="Ventas de bronze sin duplicados exactos que cumplen las reglas de calidad, con total_venta",
)
@dp.expect_all_or_drop(REGLAS_CALIDAD)
def pipeline_ventas_limpias():
    return (
        spark.read.table(BRONZE)
        .dropDuplicates()
        .withColumn("total_venta", (F.col("cantidad") * F.col("precio_unitario")).cast("decimal(18,2)"))
    )


@dp.materialized_view(
    name="pipeline_ventas_por_categoria",
    comment="Numero de ventas y valor total vendido por categoria",
)
def pipeline_ventas_por_categoria():
    return (
        spark.read.table("pipeline_ventas_limpias")
        .groupBy("categoria")
        .agg(
            F.count("*").alias("num_ventas"),
            F.sum("total_venta").alias("valor_total"),
        )
    )
