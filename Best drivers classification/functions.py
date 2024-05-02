from pyspark.sql.functions import col, dense_rank, sum, mean, col
from pyspark.sql.window import Window


def join_dfs(df_1, df_2, df_3, on_column1, on_column2, type1 = 'inner', type2 = 'inner'):
    """
    Une tres dataframe según las columnas y el tipo de join indicados por el usuario. 
    Retorna un dataframe con los datos unidos de los tres dataframes.

    df_1: dataframe que se une con df_2.
    df_2: dataframe que se une con df_1 primero y después con df_3.
    df_3: dataframe que se une con df_2.
    on_column1: columna para realizar el join entre df_1 y df_2.
    on_column2: columna para realizar el join entre df_2 y df_3.
    type1: tipo de join.
    type2: tipo de join.  
    
    """
    
    
    df_temp = df_1.alias('a').join(df_2.alias('b'), df_1[on_column1] ==  df_2[on_column1], type1).select('a.*', 'b.codigo_de_ruta', 'b.fecha')
    df = df_temp.alias('c').join(df_3.alias('d'), df_temp[on_column2] == df_3[on_column2], type2).select('c.*', 'd.nombre_ruta', 'd.kms')
    
    
    if type1 == "inner" or type2 == "inner": # Si el tipo de join es inner se eliminan las filas que no están en ambos dataframes

        df = df.na.drop(subset=["kms"])


    return df


def aggregate(df, column_group, column_values, type_agg):

    """
    Realiza agregaciones parciales de los valores según las columnas de agrupamiento y el tipo de agregación (total o media) 
    y retorna una tabla con los valores agregados y la columna(s) de agrupamiento.

    df: dataframe con al menos una columna de valores y una columna de agrupamiento.
    column_group: columna por la cual se desean agrupar los datos.
    column_values: columna con valores a agrupar.
    type_agg: tipo de agregación (total o media).
    
    """

    df = df.replace("",None) # Se eliminan las filas con valores nulos.
    df = df.na.drop(subset=["kms"])

    if type_agg == "total":

        df_grouped = df.groupBy(column_group).agg(sum(column_values).alias('suma_kms'))
        return df_grouped

    elif type_agg == "mean":        

        df_grouped = df.groupBy(column_group).agg(mean(column_values).alias('media_kms'))
        return df_grouped

    else:
        return "Type_agg debe ser 'total' o 'mean'"


def top_n(df, column_group, column_value, n = 5):

    """
    Retorna una tabla con el top n de observaciones (descendente) según la columna de valores y agrupada por la columna de agrupación. 
    Brinda el ranking de cada observación en orden ascendente.
    
    df: dataframe con al menos una columna de valores y una columna de agrupamiento.
    column_group: columna por la cual se desean agrupar los datos.
    column_values: columna con valores a agrupar.
    n: Número de observaciones top deseadas. Debe ser un número entero mayor a 0. Por default es 5.   
    """

    topN_df = df.withColumn("Rank", dense_rank().over(Window.partitionBy(column_group).orderBy(col(column_value).desc()))).where(col("Rank") <= n)
        
    return topN_df

    


