# Tarea 3 – Procesamiento de Datos con Apache Spark y Kafka

## 1. Descripción del proyecto

Este repositorio contiene el desarrollo de la Tarea 3 del curso Big Data, correspondiente al procesamiento de datos mediante Apache Spark y Apache Kafka.

El proyecto comprende dos prácticas:

- **Procesamiento por lotes (Batch):** análisis de la Tasa Representativa del Mercado (TRM) histórica de Colombia utilizando PySpark.
- **Procesamiento en tiempo real (Streaming):** simulación de datos de sensores mediante Apache Kafka y análisis con Spark Structured Streaming.

## 2. Objetivo

Aplicar herramientas de procesamiento de grandes volúmenes de datos para realizar operaciones de limpieza, transformación, análisis estadístico y visualización, tanto en conjuntos de datos históricos como en flujos de información en tiempo real.

## 3. Herramientas utilizadas

- Ubuntu 22.04
- Python 3
- Apache Spark 3.5.8
- PySpark
- Apache Kafka 3.9.2
- ZooKeeper
- Pandas y Matplotlib
- VirtualBox y PuTTY

## 4. Conjunto de datos

Se utiliza un conjunto de datos históricos de la Tasa Representativa del Mercado (TRM) de Colombia, obtenido de la plataforma Datos Abiertos Colombia.

Fuente: https://www.datos.gov.co/api/views/mcec-87by/rows.csv

El procesamiento permitió analizar 8.348 registros válidos correspondientes al periodo 1991–2026.

## 5. Archivos del proyecto

- `analisis_batch.py`: lectura, limpieza y análisis estadístico de la TRM con Apache Spark.
- `graficas_trm.py`: generación de gráficas de los resultados históricos.
- `rows.csv`: conjunto de datos original.
- `kafka_producer.py`: simulación y envío de datos de sensores al tema de Kafka.
- `spark_streaming_consumer.py`: consumo y análisis de eventos mediante Spark Structured Streaming.
- `resultados/`: contiene las gráficas anual y mensual, así como los resúmenes estadísticos en CSV.

## 6. Ejecución del procesamiento por lotes

Con Apache Spark instalado y configurado en Ubuntu, acceder a la carpeta del proyecto y ejecutar:

```bash
spark-submit analisis_batch.py
python3 graficas_trm.py
```

Los resultados generados se almacenan en la carpeta `resultados`.

## 7. Ejecución del procesamiento en tiempo real

Se requiere que ZooKeeper y el servidor Apache Kafka estén iniciados.

Crear el tema de Kafka, si todavía no existe:

```bash
/opt/Kafka/bin/kafka-topics.sh --create --topic sensor_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

En una terminal, ejecutar el productor:

```bash
python3 kafka_producer.py
```

En otra terminal, ejecutar el consumidor con Spark:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_consumer.py
```

La aplicación procesa datos simulados de temperatura y humedad, calculando estadísticas por sensor en ventanas temporales.

Para detener los programas, utilizar `Ctrl + C`.

## 8. Resultados

En el procesamiento histórico de la TRM se realizaron operaciones de limpieza, agrupación y análisis descriptivo, obteniendo resúmenes anuales y mensuales acompañados de visualizaciones.

En la práctica de streaming se comprobó la recepción de eventos enviados por Kafka y el cálculo de temperatura promedio, humedad promedio y número de registros mediante Apache Spark.

## 9. Conclusiones

Apache Spark permite analizar conjuntos de datos históricos mediante operaciones distribuidas y generar información útil para identificar comportamientos y tendencias.

La integración de Kafka con Spark Structured Streaming permite procesar flujos de información a medida que se reciben, ofreciendo una alternativa para aplicaciones que requieren análisis continuo de datos.

Ambas prácticas permiten comprender las diferencias y aplicaciones del procesamiento por lotes y del procesamiento en tiempo real.

## 10. Referencias

- Apache Spark. https://spark.apache.org/
- Apache Kafka. https://kafka.apache.org/
- Datos Abiertos Colombia. https://www.datos.gov.co/
