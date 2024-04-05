#!/usr/bin/env python
# coding: utf-8


###############################################################################
# Código base: DataCamp
###############################################################################


###############################################################################
import pyspark
from pyspark.sql import functions as f

tLinea = "*"*80

###############################################################################
def hagaAlto(pMensaje):    
    print(tLinea)
    print(pMensaje)
    print(tLinea)

###############################################################################
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName('PySpark_Tutorial').getOrCreate()

###############################################################################
# cargar archivo
b_data = spark.read.csv(
    'data/stocks_price_final.csv',
    sep = ',',
    header = True,
    )

b_data.printSchema()

hagaAlto("05-esquema 1")

###############################################################################
# cambiar estructura
from pyspark.sql.types import *

data_schema = [
               StructField('_c0', IntegerType(), True),
               StructField('symbol', StringType(), True),
               StructField('data', DateType(), True),
               StructField('open', DoubleType(), True),
               StructField('high', DoubleType(), True),
               StructField('low', DoubleType(), True),
               StructField('close', DoubleType(), True),
               StructField('volume', IntegerType(), True),
               StructField('adjusted', DoubleType(), True),
               StructField('market.cap', StringType(), True),
               StructField('sector', StringType(), True),
               StructField('industry', StringType(), True),
               StructField('exchange', StringType(), True),
            ]

final_struc = StructType(fields=data_schema)

###############################################################################
# se lee con la estructura
data = spark.read.csv(
    'data/stocks_price_final.csv',
    sep = ',',
    header = True,
    schema = final_struc
    )

data.printSchema()

hagaAlto("10-esquema 2")

###############################################################################
# mostrar
data.show(5)

hagaAlto("15-datos")


###############################################################################
# manejo de columnas
data = data.withColumnRenamed('market.cap', 'market_cap')

data = data.withColumn('date', data.data)

data.show(5)

hagaAlto("20-show 2")


###############################################################################
# manejo de columnas
data = data.withColumnRenamed('date', 'data_changed')

data.show(5)

hagaAlto("25-show 3")

###############################################################################
# borrar una columna
data = data.drop('data_changed')

data.show(5)

hagaAlto("30-luego de drop")


###############################################################################
# datos faltantes
data.na.drop()

data.na.fill(data.select(f.mean(data['open'])).collect()[0][0])


###############################################################################
# ## 5. Selección de datos con PySpark SQL
# * Select
# * Filter
# * Between
# * When
# * Like
# * GroupBy
# * Aggregations

###############################################################################
data.select(['open', 'high', 'low', 'close', 'volume', 'adjusted']).describe().show()

hagaAlto("35-luego de llenar missing values")

###############################################################################
from pyspark.sql.functions import col, lit

data.filter( (col('data') >= lit('2020-01-01')) 
            & (col('data') <= lit('2020-01-31')) ).show(5)

hagaAlto("40-filtro data")

###############################################################################
data.filter(data.adjusted.between(100.0, 500.0)).show(5)

hagaAlto("45-filtro between")


###############################################################################
data.select('open', 'close', 
            f.when(data.adjusted >= 200.0, 1).otherwise(0)).show(5)

hagaAlto("50-filtro ajusted")

###############################################################################
data.select('sector', 
            data.sector.rlike('^[B,C]').alias('Sector Starting with B or C')
            ).distinct().show()

hagaAlto("55-iniciando con B o C")

###############################################################################
data.select(['industry', 'open', 'close', 'adjusted']).groupBy('industry').mean().show()
hagaAlto("60-media por industria")

###############################################################################
from pyspark.sql.functions import col, min, max, avg, lit

data.groupBy("sector").agg(min("data").alias("From"), 
         max("data").alias("To"),          
         min("open").alias("Minimum Opening"),
         max("open").alias("Maximum Opening"), 
         avg("open").alias("Average Opening"), 
         min("close").alias("Minimum Closing"), 
         max("close").alias("Maximum Closing"), 
         avg("close").alias("Average Closing"), 
         min("adjusted").alias("Minimum Adjusted Closing"), 
         max("adjusted").alias("Maximum Adjusted Closing"), 
         avg("adjusted").alias("Average Adjusted Closing"), 
      ).show(truncate=False)

hagaAlto("65-estadísticas")

###############################################################################
data.filter( (col('data') >= lit('2019-01-02')) & (col('data') <= lit('2020-01-31')) ).groupBy("sector").agg(min("data").alias("From"), 
         max("data").alias("To"), 
         min("open").alias("Minimum Opening"),
         max("open").alias("Maximum Opening"), 
         avg("open").alias("Average Opening"), 
         min("close").alias("Minimum Closing"), 
         max("close").alias("Maximum Closing"), 
         avg("close").alias("Average Closing"), 

         min("adjusted").alias("Minimum Adjusted Closing"), 
         max("adjusted").alias("Maximum Adjusted Closing"), 
         avg("adjusted").alias("Average Adjusted Closing"), 

      ).show(truncate=False)
hagaAlto("70-filtros")

###############################################################################
# Escribir a archivos
# CSV
data.write.csv('dataset.csv')

# JSON
data.write.save('dataset.json', format='json')

## Writing selected data to different file formats

# CSV
data.select(['data', 'open', 'close', 'adjusted']).write.csv('dataset2.csv')

# JSON
data.select(['data', 'open', 'close', 'adjusted']).write.save('dataset2.json', format='json')

hagaAlto("75-FIN DEL PROGRAMA")
###############################################################################
# fin
###############################################################################
