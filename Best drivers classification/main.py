# Importar bibliotecas necesarias

from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, StringType, StructField, StructType, FloatType, DateType)
import sys
from functions import join_dfs, aggregate, top_n

# Inputs para crear los dataframes

input_1= sys.argv[1]
input_2= sys.argv[2]
input_3= sys.argv[3]

# Creación de la sesión de Spark

spark = SparkSession.builder.appName("Tarea1").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Creación de los schemas para los dataframes

ciclistas_schema = StructType([StructField('cedula', IntegerType()),
                         StructField('nombre_ciclista', StringType()),
                         StructField('provincia', StringType())])

rutas_schema = StructType([StructField('codigo_de_ruta', IntegerType()),
                         StructField('nombre_ruta', StringType()),
                         StructField('kms', FloatType())])

actividad_schema = StructType([StructField('codigo_de_ruta', IntegerType()),
                         StructField('cedula', IntegerType()),
                         StructField('fecha', DateType())])

# Creación de los dataframes a partir de los csvs

ciclistas_df = spark.read.csv(input_1, schema=ciclistas_schema)

rutas_df = spark.read.csv(input_2, schema=rutas_schema)

actividad_df = spark.read.csv(input_3, schema=actividad_schema)       


# Visualización de cada dataframe

print("\n")
print("Ciclistas")
ciclistas_df.show()

print("\n")
print("Rutas")
rutas_df.show()

print("\n")
print("Actividad")
actividad_df.show()


# Join de los 3 dataframes

print("Tabla con información de ciclistas, rutas y actividad")
df_joined = join_dfs(ciclistas_df, actividad_df, rutas_df, 'cedula', 'codigo_de_ruta', 'inner', 'inner')
df_joined.show()


# Agregaciones parciales del total de kms

# Provincia

print("Total de kilómetros recorridos por provincia")
group_provincia = aggregate(df_joined, 'provincia', 'kms', 'total')
group_provincia.show()

# Código de ruta

print("Total de kilómetros recorridos por código de ruta")
group_ruta = aggregate(df_joined, 'codigo_de_ruta', 'kms', 'total')
group_ruta.show()

# Persona (cédula y nombre)

print("Total de kilómetros recorridos por persona")
group_persona = aggregate(df_joined, ['cedula', 'nombre_ciclista'], 'kms', 'total')
group_persona.show()

# Fecha

print("Total de kilómetros recorridos por día")
group_dia = aggregate(df_joined, 'fecha', 'kms', 'total')
group_dia.show()


# Tabla final con total de kms por provincia, número de cédula y nombre

print("Total por provincia, número de cédula y nombre")
final_total = aggregate(df_joined, ['provincia', 'cedula', 'nombre_ciclista'], 'kms', 'total')
final_total.show()

# Tabla final con promedio de kms por provincia, número de cédula y nombre 

print("Promedio por provincia, número de cédula y nombre")
final_promedio= aggregate(df_joined, ['provincia', 'cedula', 'nombre_ciclista'], 'kms', 'mean')
final_promedio.show()


# Top 5 de ciclistas on mayor cantidad de kilómetros recorridos en total por provincia

print("Top 5 de ciclistas con mayor cantidad de kilómetros recorridos en total por provincia")
ranked_total_df = top_n(final_total, 'provincia', 'suma_kms', n = 5)
ranked_total_df.show(ranked_total_df.count(), False)

#Top 5 de ciclistas on mayor cantidad de kilómetros recorridos en promedio por día por provincia

print("Top 5 de ciclistas con mayor cantidad de kilómetros recorridos en promedio por día por provincia")
ranked_promedio_df = top_n(final_promedio, 'provincia', 'media_kms', n = 5)
ranked_promedio_df.show(ranked_promedio_df.count(), False)
