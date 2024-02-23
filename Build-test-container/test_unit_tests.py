from functions import csv_file, file_name, inner_join, group_data, best_drivers
from pyspark.sql.types import StructField, StringType, IntegerType, DoubleType, StructType
from pyspark.sql.functions import col


def test_is_csv_file():
    filenames = ['file1.csv', 'data.txt', 'document.csv', 'report.pdf']
    expected_results = [True, False, True, False]
    results = [is_file_csv(filename) for filename in filenames]
    assert expected_results == results

def test_file_name():
    filenames = ['file1.csv', 'data.txt', 'document.csv', 'report.pdf']
    expected_results = ['file1', 'data', 'document', 'report']
    results = [file_name(filename) for filename in filenames]
    assert expected_results == results

def test_inner_join(spark_session):
    data1 = [("A", 1), ("B", 2), ("C", 3)]
    columns1 = ["common_column", "value1"]
    df1 = spark_session.createDataFrame(data1, columns1)

    data2 = [("A", 10), ("B", 20), ("D", 30)]
    columns2 = ["common_column", "value2"]
    df2 = spark_session.createDataFrame(data2, columns2)

    result_df = inner_join(df1, df2, "common_column")

    schema = StructType([
        StructField("common_column", StringType(), True),
        StructField("value1", IntegerType(), True),
        StructField("value2", IntegerType(), True),
    ])

    expected_data = [("A", 1, 10), ("B", 2, 20)]
    expected_result = spark_session.createDataFrame(expected_data, schema)

    assert result_df.collect() == expected_result.collect()

def test_group_data(spark_session):
    mock_data = [("San Jose", "Ana González", 1, "2021-02-21"),
                 ("Alajuela", "Andrés Fernández", 2, "2020-11-08"),
                 ("Alajuela", "Carlos Rodríguez", 3, "2023-06-02")]

    df = spark_session.createDataFrame(
        mock_data, schema=['provincia', 'nombre', 'distancia', 'fecha'])

    result_df = group_data(
        df, "province", "name", "distance", "date")

    expected_result = spark_session.createDataFrame(
        [('San Jose', 'Ana González', 10.5, 178), ('Heredia', 'Carlos Rodríguez', 12.1, 178)],
        schema=['province', 'name', 'total_distance', 'percentage_distance_by_date']
    )

    assert result_df.collect() == expected_result.collect()


def test_best_drivers(spark_session):
    mock_routes = [(1, 'Route1', 10), (2, 'Route2', 20), (3, 'Route3', 30)]
    mock_drivers = [(100, 'Hoyt', 'A'),
                   (200, 'Patrick', 'C'),
                   (300, 'Donald', 'C'),
                   (400, 'Bailey', 'B'),
                   (500, 'Arlie', 'C')
                   ]

    mock_activities = [(3, 300, "2023-01-24"),
                     (1, 200, "2023-01-24"), (3, 400, "2023-02-24")]

    df_routes = spark_session.createDataFrame(
        mock_routes, schema=['route_id', 'route_name', 'distance'])
    df_drivers = spark_session.createDataFrame(
        mock_drivers, schema=['driver_id', 'driver_name', 'province'])
    df_activities = spark_session.createDataFrame(
        mock_activities, schema=['route_id', 'driver_id', 'date'])

    database = {
        'routes': df_routes,
        'drivers': df_drivers,
        'activities': df_activities
    }

    result_df = best_drivers(database)

    expected_result = spark_session.createDataFrame(
        [('B', 'Bailey', 30, 30), ('C', 'Arlie', 30, 30), ('C', 'Patrick', 10, 10)],
        schema=['province', 'driver_name', 'total_distance', 'percentage_distance_by_date']
    )

    expected_result = expected_result.withColumn(
        "percentage_distance_by_date", col("percentage_distance_by_date").cast(DoubleType())
    )

    assert result_df.collect() == expected_result.collect()


