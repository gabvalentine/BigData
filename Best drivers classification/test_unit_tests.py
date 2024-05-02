from functions import join_dfs, aggregate, top_n


# Unit tests para el join

# Test 1

def test_join_pass(spark_session):
        print("\n")
        print("Unit Test: join pass")
        print("\n")


        ciclistas = [(336996814, 'Javier Rodriguez', 'Cartago'), 
                        (618541295, 'Olga Martinez', 'Puntarenas'), 
                        (449300413, 'Jose Fernandez', 'Heredia'), 
                        (442900777, 'Francisco Vargas', 'Heredia')]
        rutas = [(602, 'Puerto Viejo', 6.63), 
                (901, 'Puntarenas - Esparza', 6.09),
                (401,'Heredia - San Joaquín', 4.45),
                (402,'Alajuela - San Rafael', 9.43)]
        actividad = [(901, 336996814, '2021-11-01'),
                (402, 618541295, '2021-11-03'),
                (402, 449300413, '2021-11-06'),
                (602, 442900777, '2021-11-02')]


        ciclistas_df  = spark_session.createDataFrame(ciclistas,['cedula', 'nombre_ciclista','provincia'])
        rutas_df = spark_session.createDataFrame(rutas,['codigo_de_ruta', 'nombre_ruta','kms'])
        actividad_df = spark_session.createDataFrame(actividad, ['codigo_de_ruta', 'cedula', 'fecha'])


        joined_df = join_dfs(ciclistas_df, actividad_df, rutas_df, 'cedula', 'codigo_de_ruta', 'inner', 'inner')
        correct_df = spark_session.createDataFrame([(442900777, 'Francisco Vargas', 'Heredia', 602, '2021-11-02', 'Puerto Viejo', 6.63),
                                                (618541295, 'Olga Martinez', 'Puntarenas', 402, '2021-11-03', 'Alajuela - San Rafael', 9.43),
                                                (449300413, 'Jose Fernandez', 'Heredia', 402, '2021-11-06', 'Alajuela - San Rafael', 9.43),       
                                                (336996814, 'Javier Rodriguez', 'Cartago', 901, '2021-11-01', 'Puntarenas - Esparza', 6.09)],
        ['cedula', 'nombre_ciclista', 'provincia', 'codigo_de_ruta', 'fecha', 'nombre_ruta', 'kms'])


        print("-----------------------------------------------")
        print("Output esperado")
        print("\n")
        correct_df.show()
        print("-----------------------------------------------")
        print("Output del código")
        joined_df.show()
        print("\n")
        print("-----------------------------------------------")

        assert joined_df.collect() == correct_df.collect()



# Test 2

def test_join_with_missing_kms(spark_session):
        print("\n")
        print("Unit Test: join with missing values in kms column")
        print("\n")

        ciclistas = [(336996814, 'Javier Rodriguez', 'Cartago'), 
                        (618541295, 'Olga Martinez', 'Puntarenas'), 
                        (449300413, 'Jose Fernandez', 'Heredia'), 
                        (442900777, 'Francisco Vargas', 'Heredia')]
        rutas = [(602, 'Puerto Viejo', None), 
                (901, 'Puntarenas - Esparza', None),
                (401,'Heredia - San Joaquín', 4.45),
                (402,'Alajuela - San Rafael', 9.43)]
        actividad = [(901, 336996814, '2021-11-01'),
                        (402, 618541295, '2021-11-03'),
                        (402, 449300413, '2021-11-06'),
                        (602, 442900777, '2021-11-02')]


        ciclistas_df  = spark_session.createDataFrame(ciclistas,['cedula', 'nombre_ciclista','provincia'])
        rutas_df = spark_session.createDataFrame(rutas,['codigo_de_ruta', 'nombre_ruta','kms'])
        actividad_df = spark_session.createDataFrame(actividad, ['codigo_de_ruta', 'cedula', 'fecha'])


        joined_df = join_dfs(ciclistas_df, actividad_df, rutas_df, 'cedula', 'codigo_de_ruta', 'inner', 'inner')
        correct_df = spark_session.createDataFrame([(618541295, 'Olga Martinez', 'Puntarenas', 402, '2021-11-03', 'Alajuela - San Rafael', 9.43),
                                                        (449300413, 'Jose Fernandez', 'Heredia', 402, '2021-11-06', 'Alajuela - San Rafael', 9.43)],
        ['cedula', 'nombre_ciclista', 'provincia', 'codigo_de_ruta', 'fecha', 'nombre_ruta', 'kms'])


        print("-----------------------------------------------")
        print("Output esperado")
        print("\n")
        correct_df.show()
        print("-----------------------------------------------")
        print("Output del código")
        joined_df.show()
        print("\n")
        print("-----------------------------------------------")


        assert joined_df.collect() == correct_df.collect()




# Test 3

def test_join_with_missing_values(spark_session):
        print("\n")
        print("Unit Test: join with missing values in columns that are not kms")
        print("\n")

        ciclistas = [(336996814, 'Javier Rodriguez', ''), 
            (618541295, '', 'Puntarenas'), 
            (449300413, 'Jose Fernandez', 'Heredia'), 
            (None, 'Francisco Vargas', 'Heredia')]
        rutas = [(602, '', 6.63), 
                (None, 'Puntarenas - Esparza', 6.09),
                (401,'Heredia - San Joaquín', 4.45),
                (402,'Alajuela - San Rafael', 9.43)]
        actividad = [(901, 336996814, ''),
                        (402, 618541295, ''),
                        (None, 449300413, '2021-11-06'),
                        (602, 442900777, '2021-11-02')]

        
        ciclistas_df  = spark_session.createDataFrame(ciclistas,['cedula', 'nombre_ciclista','provincia'])
        rutas_df = spark_session.createDataFrame(rutas,['codigo_de_ruta', 'nombre_ruta','kms'])
        actividad_df = spark_session.createDataFrame(actividad, ['codigo_de_ruta', 'cedula', 'fecha'])


        joined_df = join_dfs(ciclistas_df, actividad_df, rutas_df, 'cedula', 'codigo_de_ruta', 'inner', 'inner')
        correct_df = spark_session.createDataFrame([
                (618541295, '', 'Puntarenas', 402, '', 'Alajuela - San Rafael', 9.43)
                ],
        ['cedula', 'nombre_ciclista', 'provincia', 'codigo_de_ruta', 'fecha', 'nombre_ruta', 'kms'])

        print("-----------------------------------------------")
        print("Output esperado")
        print("\n")
        correct_df.show()
        print("-----------------------------------------------")
        print("Output del código")
        joined_df.show()
        print("\n")
        print("-----------------------------------------------")


        assert joined_df.collect() == correct_df.collect()


# Test 4

def test_left_join(spark_session):
        print("\n")
        print("Unit Test: left join")
        print("\n")

        ciclistas = [(336996814, 'Javier Rodriguez', 'Cartago'), 
                (618541295, 'Olga Martinez', 'Puntarenas'), 
                 (449300413, 'Jose Fernandez', 'Heredia'), 
                 (442900777, 'Francisco Vargas', 'Heredia'),
                 (579936231, 'Adriana Abarca', 'Guanacaste')]
        rutas = [(602, 'Puerto Viejo', 6.63), 
                (901, 'Puntarenas - Esparza', 6.09),
                (401,'Heredia - San Joaquín', 4.45),
                (402,'Alajuela - San Rafael', 9.43)]
        actividad = [(901, 336996814, '2021-11-01'),
                        (402, 618541295, '2021-11-03'),
                        (402, 449300413, '2021-11-06'),
                        (602, 442900777, '2021-11-02')]


        ciclistas_df  = spark_session.createDataFrame(ciclistas,['cedula', 'nombre_ciclista','provincia'])
        rutas_df = spark_session.createDataFrame(rutas,['codigo_de_ruta', 'nombre_ruta','kms'])
        actividad_df = spark_session.createDataFrame(actividad, ['codigo_de_ruta', 'cedula', 'fecha'])


        joined_df = join_dfs(ciclistas_df, actividad_df, rutas_df, 'cedula', 'codigo_de_ruta', 'left', 'left')
        correct_df = spark_session.createDataFrame([(579936231, 'Adriana Abarca', 'Guanacaste', None, None, None, None),
                                                (449300413, 'Jose Fernandez', 'Heredia', 402, '2021-11-06', 'Alajuela - San Rafael', 9.43),
                                                (618541295, 'Olga Martinez', 'Puntarenas', 402, '2021-11-03', 'Alajuela - San Rafael', 9.43),
                                                (442900777, 'Francisco Vargas', 'Heredia', 602, '2021-11-02', 'Puerto Viejo', 6.63),                                                    
                                                (336996814, 'Javier Rodriguez', 'Cartago', 901, '2021-11-01', 'Puntarenas - Esparza', 6.09)],
        ['cedula', 'nombre_ciclista', 'provincia', 'codigo_de_ruta', 'fecha', 'nombre_ruta', 'kms'])


        print("-----------------------------------------------")
        print("Output esperado")
        print("\n")
        correct_df.show()
        print("-----------------------------------------------")
        print("Output del código")
        joined_df.show()
        print("\n")
        print("-----------------------------------------------")

        assert joined_df.collect() == correct_df.collect()


# Unit tests para las agregaciones parciales

# Test 5

def test_aggregate_total(spark_session):
        print("\n")
        print("Unit test: Aggregate total with different columns")
        print("\n")

        
        joined_df = spark_session.createDataFrame([(779935691,'Roberto Barquero','Limón', 802, '2021-11-04', 'Acosta - Parrita', 8.17),
                                                (198496876,'Maria Chavarria','San José', 301, '2021-11-06', 'Cartago - Oreamuno', 4.55),
                                                (297911806,'Manuel Rojas', 'Alajuela', 801, '2021-11-06', 'Tibás - Coronado', 3.57),
                                                (350317937,'Ana Guzman','Cartago', 301, '2021-11-03', 'Cartago - Oreamuno', 4.55),
                                                (730228926,'Gerardo Granados','Limón', 901, '2021-11-05', 'Puntarenas - Esparza',6.09),
                                                (730228926,'Gerardo Granados','Limón', 602, '2021-11-06', 'Puerto Viejo', 6.63),
                                                (168323739,'Luz Salas','San José', 601,'2021-11-05', 'Pérez Zeledón', 8.36),
                                                (433134592,'Jose Calderon','Heredia', 702, '2021-11-06', 'Liberia', 4.31),
                                                (500991182,'Alexander Rojas', 'Guanacaste', 802, '2021-11-06', 'Acosta - Parrita', 8.17),
                                                (500991182,'Alexander Rojas', 'Guanacaste', 602, '2021-11-06', 'Puerto Viejo', 6.63)
                                                ], 
        ['cedula', 'nombre_ciclista', 'provincia', 'codigo_de_ruta', 'fecha', 'nombre_ruta', 'kms'])


        # Agregación por provincia

        aggregated_total_provincia_df = aggregate(joined_df, 'provincia', 'kms', 'total')
        correct_provincia_df = spark_session.createDataFrame([('San José', 12.91),
                                                        ('Alajuela', 3.57),
                                                        ('Limón', 20.89),
                                                        ('Heredia', 4.31),
                                                        ('Cartago', 4.55),                                                        
                                                        ('Guanacaste', 14.8)],
        ['provincia', 'suma_kms'])

        # Agregación por persona

        aggregated_total_persona_df = aggregate(joined_df, ['cedula', 'nombre_ciclista'], 'kms', 'total')
        correct_persona_df = spark_session.createDataFrame([(198496876,'Maria Chavarria', 4.55),
                                                            (730228926,'Gerardo Granados',12.719999999999999),
                                                            (297911806,'Manuel Rojas', 3.57),
                                                            (779935691,'Roberto Barquero', 8.17),
                                                            (168323739,'Luz Salas', 8.36),
                                                            (433134592,'Jose Calderon', 4.31),
                                                            (500991182,'Alexander Rojas', 14.8),
                                                            (350317937,'Ana Guzman', 4.55)],
        ['cedula', 'nombre_ciclista', 'suma_kms'])  

        # Agregación por fecha

        aggregated_total_fecha_df = aggregate(joined_df, 'fecha', 'kms', 'total')
        correct_fecha_df = spark_session.createDataFrame([('2021-11-03', 4.55),
                                                        ('2021-11-05', 14.45),
                                                        ('2021-11-06', 33.86),
                                                        ('2021-11-04', 8.17)],
        ['fecha', 'suma_kms'])  

        
        # Agregación por código de ruta

        aggregated_total_codigo_de_ruta_df = aggregate(joined_df, 'codigo_de_ruta', 'kms', 'total')
        correct_codigo_de_ruta_df = spark_session.createDataFrame([(602, 13.26),
                                                        (702, 4.31),
                                                        (901, 6.09),
                                                        (301, 9.1),                                                                                                     
                                                        (801, 3.57),                                                       
                                                        (601, 8.36),                                                        
                                                        (802, 16.34)],
        ['codigo_de_ruta', 'suma_kms'])

        print("-----------------------------------------------")
        print("Output esperado por provincia")
        print("\n")
        correct_provincia_df.show()
        print("-----------------------------------------------")
        print("Output del código")
        aggregated_total_provincia_df.show()
        print("\n")
        print("-----------------------------------------------")

        assert aggregated_total_provincia_df.collect() == correct_provincia_df.collect()

        print("-----------------------------------------------")
        print("Output esperado por persona")
        print("\n")
        correct_persona_df.show()
        print("-----------------------------------------------")
        print("Output del código")
        aggregated_total_persona_df.show()
        print("\n")
        print("-----------------------------------------------")

        assert aggregated_total_persona_df.collect() == correct_persona_df.collect()


        print("-----------------------------------------------")
        print("Output esperado por fecha")
        print("\n")
        correct_fecha_df.show()
        print("-----------------------------------------------")
        print("Output del código")
        aggregated_total_fecha_df.show()
        print("\n")
        print("-----------------------------------------------")

        assert aggregated_total_fecha_df.collect() == correct_fecha_df.collect()


        print("-----------------------------------------------")
        print("Output esperado por código de ruta")
        print("\n")
        correct_codigo_de_ruta_df.show()
        print("-----------------------------------------------")
        print("Output del código")
        aggregated_total_codigo_de_ruta_df.show()
        print("\n")
        print("-----------------------------------------------")

        assert aggregated_total_codigo_de_ruta_df.collect() == correct_codigo_de_ruta_df.collect()

# Test 6

def test_aggregate_mean_missing_values(spark_session):
        print("\n")
        print("Unit test: Aggregate mean with missing kms")
        print("\n")

        
        # Creación de la tabla unificada

        joined_df = spark_session.createDataFrame([(779935691,'Roberto Barquero','Limón', 802, '2021-11-04', 'Acosta - Parrita', 8.17),
                                                (198496876,'Maria Chavarria','San José', 301, '2021-11-06', 'Cartago - Oreamuno', 4.55),
                                                (297911806,'Manuel Rojas', 'Alajuela', 801, '2021-11-06', 'Tibás - Coronado', 3.57),
                                                (350317937,'Ana Guzman','Cartago', 301, '2021-11-03', 'Cartago - Oreamuno', None),
                                                (730228926,'Gerardo Granados','Limón', 901, '2021-11-05', 'Puntarenas - Esparza',6.09),
                                                (730228926,'Gerardo Granados','Limón', 602, '2021-11-06', 'Puerto Viejo', None),
                                                (168323739,'Luz Salas','San José', 601,'2021-11-05', 'Pérez Zeledón', 8.36),
                                                (433134592,'Jose Calderon','Heredia', 702, '2021-11-06', 'Liberia', None),
                                                (500991182,'Alexander Rojas', 'Guanacaste', 802, '2021-11-06', 'Acosta - Parrita', 8.17),
                                                (500991182,'Alexander Rojas', 'Guanacaste', 602, '2021-11-06', 'Puerto Viejo', 6.63)
                                                ], 
        ['cedula', 'nombre_ciclista', 'provincia', 'codigo_de_ruta', 'fecha', 'nombre_ruta', 'kms'])


        aggregated_mean_df = aggregate(joined_df, ['provincia', 'cedula', 'nombre_ciclista'], 'kms', 'mean')
        correct_mean_df = spark_session.createDataFrame([('San José', 168323739,'Luz Salas', 8.36),
                                                        ('San José', 198496876,'Maria Chavarria', 4.55),                                                        
                                                        ('Alajuela', 297911806,'Manuel Rojas', 3.57),
                                                        ('Limón', 779935691,'Roberto Barquero', 8.17),
                                                        ('Guanacaste', 500991182, 'Alexander Rojas', 7.4),
                                                        ('Limón', 730228926,'Gerardo Granados', 6.09)],
        ['provincia', 'cedula', 'nombre_ciclista', 'media_kms'])


        print("-----------------------------------------------")
        print("Output esperado promedio por provincia, cédula y nombre")
        print("\n")
        correct_mean_df.show()
        print("-----------------------------------------------")
        print("Output del código")
        aggregated_mean_df.show()
        print("\n")
        print("-----------------------------------------------")

        assert aggregated_mean_df.collect() == correct_mean_df.collect()


# Units test para el top n


# Test 7

def test_top_n_total(spark_session):
        print("\n")
        print("Unit test: Get the top n observations by total kms according to grouping columns")
        print("\n")


        aggregated_total_df = spark_session.createDataFrame([('Limón', 779935691, 'Roberto Barquero', 8.17),
                                                ('San José', 198496876,'Maria Chavarria', 4.55), 
                                                ('Limón', 730228926,'Gerardo Granados',12.719999999999999),                                                
                                                ('Limón', 711650670,'Oscar Jimenez', 4.55),
                                                ('San José', 168323739,'Luz Salas', 8.36), 
                                                ('San José', 179104670,'Jorge Espinoza', 9.43)                                             
                                                ], 
        ['provincia', 'cedula', 'nombre_ciclista', 'suma_kms'])


        
        top_2_total_kms_df = top_n(aggregated_total_df, 'provincia', 'suma_kms', n = 2)
        correct_top_df = spark_session.createDataFrame([('Limón', 730228926,'Gerardo Granados', 12.719999999999999, 1),                                                       
                                                        ('Limón', 779935691,'Roberto Barquero', 8.17, 2),
                                                        ('San José', 179104670,'Jorge Espinoza', 9.43, 1),                
                                                        ('San José', 168323739,'Luz Salas', 8.36, 2)                                                       
                                                        ],
        ['provincia', 'cedula', 'nombre_ciclista', 'suma_kms', 'Rank'])


        print("-----------------------------------------------")
        print("Output esperado top n por provincia, cédula y nombre")
        print("\n")
        correct_top_df.show()
        print("-----------------------------------------------")
        print("Output del código")
        top_2_total_kms_df.show()
        print("\n")
        print("-----------------------------------------------")

        assert top_2_total_kms_df.collect() == correct_top_df.collect()


# Test 8

def test_top_n_mean(spark_session):
        print("\n")
        print("Unit test: Get the top n observations by mean kms with missing values in grouping columns")
        print("\n")


        aggregated_media_df = spark_session.createDataFrame([('Limón', 779935691, 'Roberto Barquero', 7.5),
                                                ('San José', 198496876,'Maria Chavarria', 4.55), 
                                                ('Limón', 730228926,'Gerardo Granados', 6.2),                                                
                                                ('Limón', 711650670,'Oscar Jimenez', 4.55),
                                                ('San José', 168323739,'Luz Salas', 8.36), 
                                                ('San José', 179104670,'Jorge Espinoza', 9.43)], 
        ['provincia', 'cedula', 'nombre_ciclista', 'media_kms'])


        
        top_1_media_kms_df = top_n(aggregated_media_df, 'provincia', 'media_kms', n = 1)
        correct_top_mean_df = spark_session.createDataFrame([('Limón', 779935691, 'Roberto Barquero', 7.5, 1),
                                                             ('San José', 179104670,'Jorge Espinoza', 9.43, 1)],
        ['provincia', 'cedula', 'nombre_ciclista', 'media_kms', 'Rank'])


        print("-----------------------------------------------")
        print("Output esperado top n en promedio de kms por provincia, cédula y nombre")
        print("\n")
        correct_top_mean_df.show()
        print("-----------------------------------------------")
        print("Output del código")
        top_1_media_kms_df.show()
        print("\n")
        print("-----------------------------------------------")

        assert top_1_media_kms_df.collect() == correct_top_mean_df.collect()















