from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def read_files_json():
    # Crear una sesión de Spark
    spark = SparkSession.builder.appName("ProcesamientoDiber").getOrCreate()

    # Especificar la ruta de los archivos JSON
    json_path = "ruta/archivos/*.json"

    # Leer archivos JSON y retornar un DataFrame
    dataframe = spark.read.json(json_path)

def viajes(dataframe):
    # Calcular el total de viajes
    total_viajes = dataframe.count()
    return spark.createDataFrame([(total_viajes,)], ["total_viajes"])

def ingresos(dataframe):
    # Calcular el total de ingresos
    total_ingresos = dataframe.agg(F.sum("ingresos")).collect()[0][0]
    return spark.createDataFrame([(total_ingresos,)], ["total_ingresos"])

def top_driver_km(dataframe):
    # Encontrar al conductor con más kilómetros recorridos
    top_driver = dataframe.orderBy(F.desc("kilometros")).limit(1)
    return top_driver.select("conductor", "kilometros")

def top_driver_ingresos(dataframe):
    # Encontrar al conductor con más ingresos
    top_driver = dataframe.orderBy(F.desc("ingresos")).limit(1)
    return top_driver.select("conductor", "ingresos")

def percentil(dataframe):
    # Calcular percentiles
    percentiles_values = dataframe.approxQuantile(["columna1", "columna2"], [0.25, 0.50, 0.75], 0.01)

    # Definir el esquema para el DataFrame de percentiles
    schema = StructType([
        StructField("percentil_25", DoubleType(), True),
        StructField("percentil_50", DoubleType(), True),
        StructField("percentil_75", DoubleType(), True)
    ])

    # Crear DataFrame de percentiles
    percentiles_df = spark.createDataFrame([percentiles_values], schema=schema)
    return percentiles_df


