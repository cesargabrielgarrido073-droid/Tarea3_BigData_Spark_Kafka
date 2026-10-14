from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# 1. Iniciar Apache Spark
spark = SparkSession.builder \
    .appName("Analisis_Historico_TRM") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Cargar el conjunto de datos
ruta = "rows.csv"

df = spark.read.option("header", True) \
    .option("inferSchema", False) \
    .csv(ruta)

print("\n=== DATOS ORIGINALES ===")
df.printSchema()
df.show(5, truncate=False)
print("Registros originales:", df.count())

# 3. Limpieza y transformacion
df_limpio = df.select(
    F.col("VALOR").cast("double").alias("valor"),
    F.col("UNIDAD").alias("unidad"),
    F.to_date("VIGENCIADESDE", "dd/MM/yyyy").alias("fecha")
).dropna(subset=["valor", "fecha"])

df_limpio = df_limpio.filter(F.col("valor") > 0)
df_limpio = df_limpio.dropDuplicates(["fecha"])

df_limpio = df_limpio.withColumn(
    "anio", F.year("fecha")
).withColumn(
    "mes", F.month("fecha")
)

print("\n=== DATOS LIMPIOS ===")
df_limpio.show(10, truncate=False)
print("Registros validos:", df_limpio.count())

# 4. Estadisticas generales
print("\n=== ESTADISTICAS GENERALES ===")
df_limpio.select("valor").summary().show()

# 5. Promedio, minimo y maximo por anio
resumen_anual = df_limpio.groupBy("anio").agg(
    F.round(F.avg("valor"), 2).alias("promedio"),
    F.round(F.min("valor"), 2).alias("minimo"),
    F.round(F.max("valor"), 2).alias("maximo"),
    F.count("*").alias("registros")
).orderBy("anio")

print("\n=== RESUMEN ANUAL DE LA TRM ===")
resumen_anual.show(100, truncate=False)

# 6. Promedio mensual
resumen_mensual = df_limpio.groupBy("anio", "mes").agg(
    F.round(F.avg("valor"), 2).alias("promedio")
).orderBy("anio", "mes")

print("\n=== RESUMEN MENSUAL ===")
resumen_mensual.show(20, truncate=False)

# 7. Valores historicos extremos
print("\n=== TRM MAS ALTA ===")
df_limpio.orderBy(F.desc("valor")).show(5)

print("\n=== TRM MAS BAJA ===")
df_limpio.orderBy(F.asc("valor")).show(5)

# 8. Guardar resultados para graficas
os.makedirs("resultados", exist_ok=True)

resumen_anual.toPandas().to_csv(
    "resultados/resumen_anual.csv", index=False
)

resumen_mensual.toPandas().to_csv(
    "resultados/resumen_mensual.csv", index=False
)

print("\nResultados guardados en la carpeta resultados")

spark.stop()
