from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructField, StructType, IntegerType, LongType, StringType, DateType, DecimalType
from pyspark.sql import functions as F
import os

def init_spark():
    return SparkSession.builder.appName("Tarea1_GabrielValentine").getOrCreate(), spark.sparkContext

def start_df(spark, data_schema, path):
    return spark.read.csv(path, schema=data_schema) if path else None

def file_name(file_path):
    return os.path.splitext(os.path.basename(file_path))[0]

def create_schema(file_name):
    data_schema = None

    if file_name == "actividad":
        data_schema = StructType([
            StructField("codigo", IntegerType(), True),
            StructField("cedula", LongType(), True),
            StructField("fecha", DateType(), True)
        ])
    elif file_name == "ciclista":
        data_schema = StructType([
            StructField("cedula", LongType(), True),
            StructField("nombre", StringType(), True),
            StructField("provincia", StringType(), True)
        ])
    elif file_name == "ruta":
        data_schema = StructType([
            StructField("codigo", IntegerType(), True),
            StructField("nombre_ruta", StringType(), True),
            StructField("kilometros", DecimalType(10, 2), True)
        ])

    return data_schema

def csv_file(file_path):
    return os.path.splitext(file_path)[1].lower() == '.csv'

def inner_join(df_1, df_2, on_column):
    return df_1.join(df_2, on=on_column, how="inner")

def group_data(df, group_column, name_column, distance_column, date_column):
    return df.groupBy(group_column, df[name_column]).agg(
        F.sum(distance_column).alias("total_km"),
        F.round((F.sum(distance_column) / F.count(date_column)), 2).alias("%_kilometros_por_fecha")
    )

def best_drivers(database, write_result=False, n=5):
    df_route, df_driver, df_activity = database['ruta'], database['ciclista'], database['actividad']

    join_df = query_inner_join(df_driver, df_activity, on_column='cedula')
    join_df = query_inner_join(join_df, df_route, on_column='codigo')

    result_df = query_group_data_and_aggregate(join_df, "provincia", "nombre", "kilometros", "fecha")

    window_spec = Window.partitionBy("provincia").orderBy(F.asc('provincia'), F.desc("total_km"))
    result_df_with_rank = result_df.withColumn("rank", F.row_number().over(window_spec))

    records = result_df_with_rank.filter("rank <= " + str(n)).drop("rank")

    if write_result:
        records.write.option("header", "true").mode("overwrite").csv("file:///C:/Users/gabva/Desktop/Big Data/Tarea 1/Tarea 1/results")

    return records


