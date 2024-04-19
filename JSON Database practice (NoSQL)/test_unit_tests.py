from functions import create_total_trips_df, create_total_revenue_df, create_metrics_df
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def test_create_total_trips_df(spark_session):
    mock_data = [(1, 101, 102, 10, 5.5),
                 (1, 102, 101, 10, 5.5),
                 (1, 103, 104, 10, 5.5)]

    column_names = ["identificador",
                    "codigo_postal_origen", "codigo_postal_destino", "kilometros", "precio_kilometro"]

    mock_df = spark_session.createDataFrame(mock_data, column_names)

    result_df = create_total_trips_df(mock_df)

    expected_data = [
        (101, "destino", 1),
        (102, "destino", 1),
        (104, "destino", 1),
        (101, "origen", 1),
        (102, "origen", 1),
        (103, "origen", 1)
    ]

    column_names = ["identificador",
                    "origen_destino", "cantidad_de_viajes"]

    expected_df = spark_session.createDataFrame(expected_data, column_names)

    assert result_df.collect() == expected_df.collect()


def test_create_total_revenue_df(spark_session):
    mock_data = [(1, 101, 102, 10, 5.5),
                 (1, 102, 101, 10, 5.5),
                 (1, 103, 104, 10, 5.5)]

    column_names = ["identificador",
                    "codigo_postal_origen", "codigo_postal_destino", "kilometros", "precio_kilometro"]

    mock_df = spark_session.createDataFrame(mock_data, column_names)

    result_df = create_total_revenue_df(mock_df)

    expected_data = [
        (101, "destino", 55.0),
        (102, "destino", 55.0),
        (104, "destino", 55.0),
        (101, "origen", 55.0),
        (102, "origen", 55.0),
        (103, "origen", 55.0)
    ]

    column_names = ["identificador",
                    "origen_destino", "ingresos"]

    expected_df = spark_session.createDataFrame(expected_data, column_names)

    assert result_df.collect() == expected_df.collect()


def test_create_metrics_df(spark_session):
    mock_data = [(1, 101, 102, 10, 5),
                 (1, 102, 101, 100, 0),
                 (1, 103, 104, 10, 5),
                 (2, 251, 252, 15, 4),
                 (2, 351, 352, 18, 35)]

    column_names = ["identificador",
                    "codigo_postal_origen", "codigo_postal_destino", "kilometros", "precio_kilometro"]

    mock_df = spark_session.createDataFrame(mock_data, column_names)
    result_df = create_metrics_df(mock_df, spark_session)

    expected_data = [
        ("persona_con_mas_kilometros", 1.0),
        ("persona_con_mas_ingresos", 2.0),
        ("percentil_25", 100.0),
        ("percentil_50", 100.0),
        ("percentil_75", 690.0),
        ("codigo_postal_origen_con_mas_ingresos", 351.0),
        ("codigo_postal_destino_con_mas_ingresos", 352.0)
    ]

    schema = StructType([
        StructField("metrica", StringType(), True),
        StructField("valor", DoubleType(), True)
    ])

    expected_df = spark_session.createDataFrame(expected_data, schema=schema)

    assert result_df.collect() == expected_df.collect()



