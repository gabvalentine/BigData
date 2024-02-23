import functions


def run(file_paths):
    # Inicializar Spark
    spark, sc = functions.init_spark()

    # Crear DataFrames utilizando las funciones de carga y esquema
    ciclista_df = load_data_frame(spark, file_paths, "ciclista")
    actividad_df = load_data_frame(spark, file_paths, "actividad")
    ruta_df = load_data_frame(spark, file_paths, "ruta")

    # Ejecutar la consulta principal
    result_df = functions.best_drivers(spark, ciclista_df, actividad_df, ruta_df, write_result=True)

    # Mostrar el resultado
    print(result_df.show())
