"""
Created on 08/02/2024

# %% INTEGRANTES

# Nombre: Aaron Cuellar
# Mail: aaroncuellar2003@gmail.com
# LU: 810/23
    
# Nombre: Sebastian Ceffalotti
# Mail: sebastian.ceffalotti@gmail.com
# LU: 394/23

# Nombre: Rodrigo Copa
# Mail: roodrigo.coppa98@gmail.om
# LU: 471/22
"""

# %% Importación de librerías
import duckdb as dd  
import pandas as pd
# %% Lectura de tablas
ruta_carpeta = "/home/saludos/Desktop/laboratorio de datos/tareas/practica-sql/"
casos = pd.read_csv(ruta_carpeta + "casos.csv")
departamento = pd.read_csv(ruta_carpeta + "departamento.csv")
grupoetario = pd.read_csv(ruta_carpeta + "grupoetario.csv")
provincia = pd.read_csv(ruta_carpeta + "provincia.csv")
tipoevento =  pd.read_csv(ruta_carpeta + "tipoevento.csv")

# %% A. Consultas sobre una tabla

# %% A.a. Listar sólo los nombres de todos los departamentos que hay en la tabla departamento (dejando los registros repetidos).

codigo_sql = """
                SELECT Descripcion
                FROM departamento
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% A.b. Listar sólo los nombres de todos los departamentos que hay en la tabla departamento (eliminando los registros repetidos).

codigo_sql = """
                SELECT DISTINCT Descripcion
                FROM departamento
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% A.c. Listar sólo los códigos de departamento y sus nombres, de todos los departamentos que hay en la tabla departamento.

codigo_sql = """
                SELECT Descripcion, id
                FROM departamento
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% A.d. Listar todas las columnas de la tabla departamento.

codigo_sql = """
                SELECT *
                FROM departamento
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% A.e. Listar los códigos de departamento y nombres de todos los departamentos que hay en la tabla departamento. 
# Utilizar los siguientes alias para las columnas: codigo_depto y nombre_depto, respectivamente.

codigo_sql = """
                SELECT Descripcion AS nombre_depto, id AS codigo_depto
                FROM departamento
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% A.f. Listar los registros de la tabla departamento cuyo código de provincia es igual a 54.

codigo_sql = """
                SELECT *
                FROM departamento
                WHERE id_provincia = 54
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% A.g. Listar los registros de la tabla departamento cuyo código de provincia es igual a 22, 78 u 86.

codigo_sql = """
                SELECT *
                FROM departamento
                WHERE id_provincia = 22 OR id_provincia = 78 OR id_provincia = 86
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% A.h. Listar los registros de la tabla departamento cuyos códigos de provincia se encuentren entre el 50 y el 59 (ambos valores inclusive).

codigo_sql = """
                SELECT *
                FROM departamento
                WHERE id_provincia >= 50 AND id_provincia <= 59
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% B. Consultas multitabla (INNER JOIN)

# %% B.a. Devolver una lista con los códigos y nombres de departamentos, asociados al nombre de la provincia al que pertenecen.

codigo_sql = """
                SELECT d.id, d.descripcion AS nombre_depto, p.descripcion
                FROM departamento AS d
                INNER JOIN provincia AS p
                ON d.id_provincia = p.id
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% B.b. Devolver una lista con los código y nombres de departamentos, asociados al nombre de la provincia al que pertenecen.
# Es lo mismo dX
codigo_sql = """
                SELECT d.id, d.descripcion AS nombre_depto, p.descripcion
                FROM departamento AS d
                INNER JOIN provincia AS p
                ON d.id_provincia = p.id
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% B.c. Devolver los casos registrados en la provincia de “Chaco”.

codigo_sql = """
                SELECT *
                FROM casos AS c
                INNER JOIN departamento AS d
                ON c.id_depto = d.id
                INNER JOIN provincia AS p
                ON p.id = d.id_provincia
                WHERE id_provincia = 22
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% B.d. Devolver aquellos casos de la provincia de “Buenos Aires” cuyo campo cantidad supere los 10 casos.

codigo_sql = """
                SELECT c.id_tipoevento, c.semana_epidemiologica, c.id_depto,
                c.id_grupoetario, c.cantidad, 
                FROM departamento AS d
                INNER JOIN casos AS c
                ON d.id = c.id_depto
                INNER JOIN provincia AS p
                ON p.id = d.id_provincia
                WHERE c.cantidad > 10 AND p.id = 6
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% C. Consultas multitabla (OUTER JOIN)

# %% C.a. Devolver un listado con los nombres de los departamentos que no tienen 
# ningún caso asociado.

codigo_sql = """
                SELECT DISTINCT d.descripcion AS dept_sin_casos 
                FROM departamento AS d
                LEFT OUTER JOIN casos AS c
                ON d.id = c.id_depto
                WHERE c.id_depto IS NULL
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% C.b. Devolver un listado con los tipos de evento que no tienen 
#ningún caso asociado.

codigo_sql = """
                SELECT DISTINCT tp.descripcion AS enfermedad
                FROM tipoevento AS tp
                LEFT OUTER JOIN casos AS c
                ON tp.id = c.id_tipoevento
                WHERE c.id_tipoevento IS NULL
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% D. Consultas resumen

# %% D.a. Calcular la cantidad total de casos que hay en la tabla casos.

codigo_sql = """
                SELECT SUM(c.cantidad) AS casos_totales 
                FROM casos AS c
                
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()
# %% D.b. Calcular la cantidad total de casos que hay en la tabla casos para cada año y cada tipo de caso. 
# Presentar la información de la siguiente manera: descripción del tipo de caso, año y cantidad. 
# Ordenarlo por tipo de caso (ascendente) y año (ascendente).

codigo_sql = """
                SELECT c.id_tipoevento, c.anio, SUM(c.cantidad) AS casos_totales
                FROM casos AS c
                GROUP BY c.anio, c.id_tipoevento
                ORDER BY c.id_tipoevento ASC, c.anio ASC
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()


# %% D.c. Misma consulta que el item anterior, pero sólo para el año 2019.

codigo_sql = """
                SELECT c.id_tipoevento, c.anio, SUM(c.cantidad) AS casos_totales
                FROM casos AS c
                WHERE c.anio = 2019
                GROUP BY c.anio, c.id_tipoevento
                ORDER BY c.id_tipoevento ASC, c.anio ASC
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% D.d. Calcular la cantidad total de departamentos que hay por provincia. 
#Presentar la información ordenada por código de provincia.

codigo_sql = """
                SELECT id_provincia, COUNT(*) AS cant_deptos_por_provincia
                FROM departamento 
                GROUP BY id_provincia
                ORDER BY id_provincia ASC
                
             """

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% D.e. Listar los departamentos con menos cantidad de casos en el año 2019.


# FORMA 1

codigo_sql1 = """
                SELECT c.id_depto, SUM(c.cantidad) AS total_casos
                FROM casos AS c
                WHERE anio = 2019 
                GROUP BY c.id_depto
                HAVING total_casos = (
                    SELECT MIN(suma_casos) AS minimo
                    FROM ( SELECT c.id_depto, SUM(c.cantidad) AS suma_casos
                          FROM casos AS c
                          WHERE anio = 2019
                          GROUP BY c.id_depto)
                    )
                ORDER BY c.id_depto
                """                

# FORMA 2

suma_casos_2019 = dd.sql(
                """   
                SELECT c.id_depto, SUM(c.cantidad) AS casos_totales
                FROM casos As c 
                WHERE anio = 2019
                GROUP BY c.id_depto   
                """).df()
 
codigo_sql2 = """
                SELECT id_depto
                FROM suma_casos_2019
                WHERE casos_totales = (SELECT MIN(casos_totales)
                                       FROM suma_casos_2019)
                ORDER BY id_depto
                """
CONSULTA_SQL = dd.sql(codigo_sql1).df()


# %% D.f. Listar los departamentos con más cantidad de casos en el año 2020.

suma_casos_2020 = dd.sql(
                """   
                SELECT c.id_depto, SUM(c.cantidad) AS casos_totales
                FROM casos As c 
                WHERE anio = 2020
                GROUP BY c.id_depto   
                """).df()

codigo_sql = """
                SELECT c.id_depto, d.descripcion AS nombre_depto, p.descripcion AS provincia
                FROM suma_casos_2020 AS c
                INNER JOIN departamento AS d
                ON c.id_depto = d.id
                INNER JOIN provincia AS p
                ON p.id = d.id_provincia
                WHERE c.casos_totales = (SELECT MAX(casos_totales)
                                       FROM suma_casos_2020)
                ORDER BY c.id_depto
                """
CONSULTA_SQL = dd.sql(codigo_sql).df()
# %% D.g. Listar el promedio de cantidad de casos por provincia y año.

codigo_sql = (
                """
                SELECT p.descripcion AS provincia, anio, AVG(cantidad) AS promedio_casos
                FROM casos
                INNER JOIN departamento AS d
                ON casos.id_depto = d.id
                INNER JOIN provincia AS p
                ON d.id_provincia = p.id
                GROUP BY p.descripcion, anio
                ORDER BY p.descripcion, anio ASC;
                """)


CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% D.h. Listar, para cada provincia y año, cuáles fueron los departamentos que más cantidad de casos tuvieron.

casos_por_depto = dd.sql(
    """
        SELECT p.descripcion AS provincia, c.anio, c.id_depto,
        d.descripcion AS departamento, SUM(cantidad) AS casos_totales
        FROM casos AS c
        INNER JOIN departamento AS d
        ON d.id = c.id_depto
        INNER JOIN provincia AS p
        ON p.id = d.id_provincia
        GROUP BY c.id_depto, c.anio, provincia, departamento
        ORDER BY c.id_depto ASC, c.anio ASC
    """
    ).df()

max_casos_por_provincia = dd.sql(
    """
    SELECT provincia, anio, MAX(casos_totales) AS maximo
    FROM casos_por_depto
    GROUP BY provincia, anio
    """
    ).df()
codigo_sql = ("""
                SELECT c.provincia, c.anio, c.departamento, m.maximo AS Nro_de_casos
                FROM casos_por_depto AS c
                INNER JOIN max_casos_por_provincia AS m
                ON c.provincia = m.provincia AND c.anio = m.anio AND c.casos_totales = m.maximo
                ORDER BY c.provincia, c.anio
                """)

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% D.i. Mostrar la cantidad de casos total, máxima, mínima 
# y promedio que tuvo la provincia de Buenos Aires en el año 2019.

casos_bsas_2019 = dd.sql(
    """
        SELECT c.id_depto, SUM(cantidad) AS tot_por_depto
        FROM casos AS c
        INNER JOIN departamento AS d
        ON d.id = c.id_depto
        INNER JOIN provincia AS p
        ON p.id = d.id_provincia
        WHERE c.anio = 2019 AND p.id = 6
        GROUP BY c.id_depto
        ORDER BY c.id_depto ASC
    """
    ).df()

codigo_sql = ("""
              SELECT (SELECT MAX(tot_por_depto) FROM casos_bsas_2019) AS maximo,
              (SELECT MIN(tot_por_depto) FROM casos_bsas_2019) AS minimo,
              (SELECT AVG(tot_por_depto) FROM casos_bsas_2019) AS promedio,
              (SELECT SUM(tot_por_depto) FROM casos_bsas_2019) AS cantidad_casos;
                """)
                
alternativa = ("""
               SELECT MAX(tot_por_depto) AS maximo,
               MIN(tot_por_depto) AS minimo,
               AVG(tot_por_depto) AS promedio,
               SUM(tot_por_depto) AS cantidad_casos
               FROM (
                    SELECT c.id_depto, SUM(cantidad) AS tot_por_depto
                    FROM casos AS c
                    INNER JOIN departamento AS d
                    ON d.id = c.id_depto
                    INNER JOIN provincia AS p
                    ON p.id = d.id_provincia
                    WHERE c.anio = 2019 AND p.id = 6
                    GROUP BY c.id_depto
                    ORDER BY c.id_depto ASC
                   )
               
               """)
CONSULTA_SQL = dd.sql(alternativa).df()
# %% D.j. Misma consulta que el ítem anterior, pero sólo para aquellos casos en que la cantidad total es mayor a 1000 casos.


codigo_sql = ("""
               SELECT MAX(tot_por_depto) AS maximo,
               MIN(tot_por_depto) AS minimo,
               AVG(tot_por_depto) AS promedio,
               SUM(tot_por_depto) AS cantidad_casos
               FROM (
                    SELECT c.id_depto, SUM(cantidad) AS tot_por_depto
                    FROM casos AS c
                    INNER JOIN departamento AS d
                    ON d.id = c.id_depto
                    INNER JOIN provincia AS p
                    ON p.id = d.id_provincia
                    WHERE c.anio = 2019 AND p.id = 6
                    GROUP BY c.id_depto
                    HAVING tot_por_depto > 1000
                    ORDER BY c.id_depto ASC
                   )
               
               """)

CONSULTA_SQL = dd.sql(codigo_sql).df()

# %% D.k. Listar los nombres de departamento (y nombre de provincia) que tienen mediciones tanto para el año 2019 como para el año 2020. 
# Para cada uno de ellos devolver la cantidad de casos promedio. Ordenar por nombre de provincia (ascendente) y luego por nombre de departamento (ascendente).

# %% D.l. Devolver una tabla que tenga los siguientes campos: descripción de tipo de evento, id_depto, nombre de departamento, id_provincia, nombre de provincia, total de casos 2019, total de casos 2020.

# %% E. Subconsultas (ALL, ANY)

# %% E.a. Devolver el departamento que tuvo la mayor cantidad de casos sin hacer uso de MAX, ORDER BY ni LIMIT.

# %% E.b. Devolver los tipo de evento que tienen casos asociados. (Utilizando ALL o ANY).

# %% F. Subconsultas (IN, NOT IN)

# %% F.a. Devolver los tipo de evento que tienen casos asociados (Utilizando IN, NOT IN).

# %% F.b. Devolver los tipo de evento que NO tienen casos asociados (Utilizando IN, NOT IN).

# %% G. Subconsultas (EXISTS, NOT EXISTS)

# %% G.a. Devolver los tipo de evento que tienen casos asociados (Utilizando EXISTS, NOT EXISTS).

# %% G.b. Devolver los tipo de evento que NO tienen casos asociados (Utilizando IN, NOT IN).

# %% H. Subconsultas correlacionadas

# %% H.a. Listar las provincias que tienen una cantidad total de casos mayor al promedio de casos del país. Hacer el listado agrupado por año.

# %% H.b. Por cada año, listar las provincias que tuvieron una cantidad total de casos mayor a la cantidad total de casos que la provincia de Corrientes.

# %% I. Más consultas sobre una tabla

# %% I.a. Listar los códigos de departamento y sus nombres, ordenados por estos últimos (sus nombres) de manera descendentes (de la Z a la A). 
# En caso de empate, desempatar por código de departamento de manera ascendente.

# %% I.b. Listar los registros de la tabla provincia cuyos nombres comiencen con la letra M.

# %% I.c. Listar los registros de la tabla provincia cuyos nombres comiencen con la letra S y su quinta letra sea una letra A.

# %% I.d. Listar los registros de la tabla provincia cuyos nombres terminan con la letra A.

# %% I.e. Listar los registros de la tabla provincia cuyos nombres tengan exactamente 5 letras.

# %% I.f. Listar los registros de la tabla provincia cuyos nombres tengan ”do” en alguna parte de su nombre.

# %% I.g. Listar los registros de la tabla provincia cuyos nombres tengan ”do” en alguna parte de su nombre y su código sea menor a 30.

# %% I.h. Listar los registros de la tabla departamento cuyos nombres tengan ”san” en alguna parte de su nombre. 
# Listar sólo id y descripción. Utilizar los siguientes alias para las columnas: codigo_depto y nombre_depto, respectivamente. 
# El resultado debe estar ordenado por sus nombres de manera descendentes (de la Z a la A).

# %% I.i. Devolver aquellos casos de las provincias cuyo nombre terminen con la letra a y el campo cantidad supere 10. 
# Mostrar: nombre de provincia, nombre de departamento, año, semana epidemiológica, descripción de grupo etario y cantidad. 
# Ordenar el resultado por la cantidad (descendente), luego por el nombre de la provincia (ascendente), nombre del departamento (ascendente), 
# año (ascendente) y la descripción del grupo etario (ascendente).

# %% I.j. Idem anterior, pero devolver sólo aquellas tuplas que tienen el máximo en el campo cantidad.

# %% J. Reemplazos

# %% J.a. Listar los id y descripción de los departamentos. Estos últimos sin tildes y en orden alfabético.

# %% J.b. Listar los nombres de provincia en mayúscula, sin tildes y en orden alfabético.
