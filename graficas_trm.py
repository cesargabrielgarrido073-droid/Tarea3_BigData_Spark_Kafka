import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

# Carpeta donde guardaremos las graficas
carpeta = Path("resultados")
carpeta.mkdir(exist_ok=True)

# Leer resultados del procesamiento con Spark
anual = pd.read_csv(carpeta / "resumen_anual.csv")
mensual = pd.read_csv(carpeta / "resumen_mensual.csv")

# GRAFICA 1: Promedio anual de la TRM
plt.figure(figsize=(12, 6))
plt.plot(
    anual["anio"],
    anual["promedio"],
    marker="o",
    linewidth=2
)

plt.title("Evolucion historica anual de la TRM en Colombia")
plt.xlabel("Anio")
plt.ylabel("TRM promedio (COP por USD)")
plt.grid(True, alpha=0.3)
plt.tight_layout()

plt.savefig(
    carpeta / "grafica_trm_anual.png",
    dpi=200
)
plt.close()

print("Grafica anual creada correctamente")

# GRAFICA 2: Promedio mensual de la TRM
mensual["fecha"] = pd.to_datetime(
    dict(
        year=mensual["anio"],
        month=mensual["mes"],
        day=1
    )
)

mensual = mensual.sort_values("fecha")

plt.figure(figsize=(13, 6))
plt.plot(
    mensual["fecha"],
    mensual["promedio"],
    linewidth=1.5
)

plt.title("Evolucion historica mensual de la TRM en Colombia")
plt.xlabel("Fecha")
plt.ylabel("TRM promedio (COP por USD)")
plt.grid(True, alpha=0.3)
plt.tight_layout()

plt.savefig(
    carpeta / "grafica_trm_mensual.png",
    dpi=200
)
plt.close()

print("Grafica mensual creada correctamente")
print("Imagenes guardadas en la carpeta resultados")
