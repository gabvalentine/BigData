import cfrom pyspark.sql import SparkSession
from functions import read_files_json, viajes, ingresos, top_driver_km, top_driver_ingresos, percentil

# Creamos una sesión de Spark
spark = SparkSession.builder.appName("ProcesamientoDiber").getOrCreate()

# Creamos un DataFrame a partir de los archivos JSON en el mismo directorio de ejecución
dataframe = read_files_json()
dataframe.show()

# Operaciones para calcular métricas
total_viajes = viajes(dataframe)
total_ingresos = ingresos(dataframe)
top_km_driver = top_driver_km(dataframe)
top_ingresos_driver = top_driver_ingresos(dataframe)
percentiles = percentil(dataframe)

# Guardar resultados en archivos CSV
total_viajes.coalesce(1).write.csv("total_viajes.csv", header=True)
total_ingresos.coalesce(1).write.csv("total_ingresos.csv", header=True)
top_km_driver.coalesce(1).write.csv("top_km_driver.csv", header=True)
top_ingresos_driver.coalesce(1).write.csv("top_ingresos_driver.csv", header=True)
percentiles.coalesce(1).write.csv("percentiles.csv", header=True)

# Detener la sesión de Spark
spark.stop()


