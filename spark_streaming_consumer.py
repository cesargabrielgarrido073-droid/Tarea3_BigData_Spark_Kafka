
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, from_unixtime,
    to_timestamp, window, avg, count
)
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, LongType
)

# Crear sesion de Spark
spark = SparkSession.builder \
    .appName("Sensores_Kafka_Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Estructura de los mensajes JSON
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", LongType())
])

# Leer mensajes de Kafka
datos_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# Convertir mensajes JSON en columnas
sensores = datos_kafka.select(
    from_json(
        col("value").cast("string"), schema
    ).alias("datos")
).select("datos.*")

# Convertir fecha UNIX a fecha de Spark
sensores = sensores.withColumn(
    "fecha",
    to_timestamp(from_unixtime(col("timestamp")))
)

# Calcular estadisticas por minuto y sensor
estadisticas = sensores \
    .withWatermark("fecha", "2 minutes") \
    .groupBy(
        window(col("fecha"), "1 minute"),
        col("sensor_id")
    ).agg(
        avg("temperature").alias("temperatura_promedio"),
        avg("humidity").alias("humedad_promedio"),
        count("*").alias("total_registros")
    )

# Mostrar resultados en tiempo real
consulta = estadisticas.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

consulta.awaitTermination()
