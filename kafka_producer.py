import json
import random
import time
from kafka import KafkaProducer

# Conexion con Apache Kafka
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda dato: json.dumps(dato).encode("utf-8")
)

print("Iniciando simulacion de sensores...")
print("Presiona Ctrl+C para detener el programa.")

try:
    while True:
        # Generar datos simulados
        sensor_data = {
            "sensor_id": random.randint(1, 10),
            "temperature": round(random.uniform(20, 30), 2),
            "humidity": round(random.uniform(30, 70), 2),
            "timestamp": int(time.time())
        }

        # Enviar informacion al tema sensor_data
        producer.send("sensor_data", value=sensor_data)

        print("Dato enviado:", sensor_data)

        # Esperar un segundo
        time.sleep(1)

except KeyboardInterrupt:
    print("\nSimulacion detenida.")

finally:
    producer.flush()
    producer.close()
