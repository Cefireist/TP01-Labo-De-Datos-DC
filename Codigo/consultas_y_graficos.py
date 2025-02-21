#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 21 14:09:56 2025

@author: yo
"""

#%% IMPORTO LIBRERIAS
import duckdb as dd  
import pandas as pd 
import os

#%% LEO LAS TABLAS
# obtenemos la ruta del directorio donde se esta ejecutando este codigo
ruta_actual = os.path.dirname(__file__)

# Ruta a la carpeta TablasOriginales
ruta_tablas_originales = os.path.join(ruta_actual, 'TablasModelo')

# Diccionario con las rutas de los archivos CSV
rutas = {
    "Centros_culturales": os.path.join(ruta_tablas_originales, 'Centros_culturales.csv'),
    "Personas": os.path.join(ruta_tablas_originales, 'Personas.csv'),
    "Establecimientos_educativos": os.path.join(ruta_tablas_originales, 'Establecimientos_educativos.csv'),
    "id_establecimientos_educativos": os.path.join(ruta_tablas_originales, 'id_tipo_establecimiento_educativo.csv'), 
    "Departamentos": os.path.join(ruta_tablas_originales, 'Departamentos.csv'),
    "Provincias": os.path.join(ruta_tablas_originales, 'Provincias.csv'),
    
}

# guardo en una variable el csv con el nombre, para eso uso globals que genera una variable dinamicamente 
for nombre, ruta in rutas.items():
    globals()[nombre] = pd.read_csv(ruta)

#%% CONSULTA I
"""
Provincias donde el primario dura 6 años:
Formosa, Tucumán, Catamarca, San Juan,
San Luis, Córdoba, Corrientes, 
Entre Ríos, La Pampa, Buenos Aires, 
Chubut y Tierra del Fuego. 

Provincias donde el primario dura 7 años:
Río Negro, Neuquén, Santa Cruz,
Mendoza, Santa Fe, La Rioja, 
Santiago del Estero, Chaco, Misiones, 
Salta, Jujuy, pero tambien en la Ciudad Autónoma de Buenos Aires.
"""
primario6 = (34,90,10,70,74,14,18,30,42,6,26,94)
primario7 = (62,58,78,50,82,46,86,22,54,66,38,2)

consulta_jardin = dd.sql(
            """
            SELECT id_prov, id_depto, SUM(Casos) AS poblacion_jardin
            FROM Personas
            WHERE Edad <= 5
            GROUP BY id_prov, id_depto
            
            """).df()
        
consulta_primario = dd.sql(
            f"""
            WITH primario6 AS (
            SELECT id_prov, id_depto, SUM(Casos) AS poblacion_primaria
            FROM Personas
            WHERE Edad > 5 AND Edad <= 12 AND id_prov IN {primario6}
            GROUP BY id_prov, id_depto),
            
            primario7 AS (SELECT id_prov, id_depto, SUM(Casos) AS poblacion_primaria
            FROM Personas
            WHERE Edad > 5 AND Edad <= 13 AND id_prov IN {primario7}
            GROUP BY id_prov, id_depto)
            
            SELECT *
            FROM primario6
            UNION
            SELECT *
            FROM primario7 
            """).df()

consulta_secundario = dd.sql(
            f"""
            WITH secundario6 AS(
            SELECT id_prov, id_depto, SUM(Casos) AS poblacion_secundaria
            FROM Personas
            WHERE Edad > 12 AND Edad <= 18 AND id_prov IN {primario6}
            GROUP BY id_prov, id_depto),
            
            secundario7 AS (SELECT id_prov, id_depto, SUM(Casos) AS poblacion_secundaria
            FROM Personas
            WHERE Edad > 13 AND Edad <= 18 AND id_prov IN {primario7}
            GROUP BY id_prov, id_depto)
            
            SELECT *
            FROM secundario6
            UNION
            SELECT *
            FROM secundario7
            """
            ).df()

#%% Consulta II

"""
Para cada departamento informar la provincia y la cantidad de CC con
capacidad mayor a 100 personas. El orden del reporte debe ser alfabético
por provincia y dentro de las provincias, descendente por cantidad de CC de
dicha capacidad.
"""

Consulta2 = dd.sql(
    """
    WITH a AS (
    SELECT id_prov, id_depto, COUNT(*) AS Cantidad_mayor_100
    FROM Centros_culturales
    WHERE Capacidad > 100
    GROUP BY id_prov, id_depto)
    SELECT p.nombre_provincia, d.nombre_departamento, a.Cantidad_mayor_100
    FROM a 
    INNER JOIN Provincias AS p
    ON p.id_prov = a.id_prov
    INNER JOIN Departamentos AS d
    ON d.id_depto = a.id_depto
    GROUP BY nombre_provincia, nombre_departamento, Cantidad_mayor_100
    ORDER BY nombre_provincia ASC, Cantidad_mayor_100 DESC
    """
    ).df()

#%% CONSULTA III

"""
Para cada departamento, indicar provincia, cantidad de CC, cantidad de EE
(de modalidad común) y población total. Ordenar por cantidad EE
descendente, cantidad CC descendente, nombre de provincia ascendente y
nombre de departamento ascendente. No omitir casos sin CC o EE.
"""


Cantidad_cc = dd.sql(
        """
        SELECT id_prov, id_depto, COUNT(*) AS Cantidad_cc
        FROM Centros_culturales
        GROUP BY id_prov, id_depto
        """).df()

Cantidad_ee = dd.sql(
    """
    SELECT id_prov, 0 AS id_depto, COUNT(DISTINCT Cueanexo) AS Cantidad_ee
    FROM Establecimientos_educativos
    WHERE id_prov = 2
    GROUP BY id_prov
    UNION
    SELECT id_prov, id_depto, COUNT(DISTINCT Cueanexo) AS Cantidad_ee
    FROM Establecimientos_educativos
    GROUP BY id_prov, id_depto
    """).df()
Poblacion_total = dd.sql(
    """
    SELECT id_prov, 0 AS id_depto, SUM(Casos) AS Poblacion
    FROM Personas
    WHERE id_prov = 2
    GROUP BY id_prov
    UNION
    SELECT id_prov, id_depto, SUM(Casos) AS Poblacion
    FROM Personas
    GROUP BY id_prov, id_depto
    """
    ).df()

Consulta3 = dd.sql(
    """
    WITH prov_depto AS (
    SELECT d.id_prov, d.id_depto, d.nombre_departamento AS Departamento, 
    p.nombre_provincia AS Provincia
    FROM Departamentos AS d
    INNER JOIN Provincias AS p
    ON d.id_prov = p.id_prov)
    
    SELECT pd.Provincia, pd.Departamento, Cantidad_ee, Cantidad_cc, Poblacion
    FROM prov_depto AS pd
    LEFT JOIN Cantidad_ee AS ee
    ON pd.id_prov = ee.id_prov AND pd.id_depto = ee.id_depto
    LEFT JOIN Cantidad_cc AS cc
    ON pd.id_prov = cc.id_prov AND pd.id_depto = cc.id_depto
    LEFT JOIN Poblacion_total AS pt
    ON pd.id_prov = pt.id_prov AND pd.id_depto = pt.id_depto
    ORDER BY Cantidad_ee DESC, Cantidad_cc DESC, pd.Provincia ASC, 
    pd.Departamento ASC;
    """
    ).df()
#%% CONSULTA IV

"""
Para cada departamento, indicar provincia y qué dominios de mail se usan
más para los CC.
"""
# EL dominio de un mail es todo lo que hay despues del arroba @
Consulta4 = dd.sql(
    """
    WITH Dominios AS (
    SELECT id_prov, id_depto, LOWER(SPLIT_PART(TRIM(Mail), '@', 2)) AS Dominio
    FROM Centros_culturales),
    
    Frecuencias AS (
    SELECT id_prov, id_depto, Dominio, COUNT(Dominio) AS Frecuencias
    FROM Dominios
    GROUP BY id_prov, id_depto, Dominio),
    
    Max_frecuencia AS (
        SELECT id_prov, id_depto, MAX(Frecuencias) AS Mas_frecuente
        FROM Frecuencias
        GROUP BY id_prov, id_depto)
    
    SELECT p.nombre_provincia AS Provincia, d.nombre_departamento AS Departamento,
    f.Dominio AS Dominio_mas_frecuente
    FROM Frecuencias AS f
    INNER JOIN Max_frecuencia AS m
    ON f.id_prov = m.id_prov AND f.id_depto = m.id_depto AND f.Frecuencias = m.Mas_frecuente
    INNER JOIN Departamentos AS d
    ON d.id_prov = f.id_prov AND d.id_depto = f.id_depto
    INNER JOIN Provincias AS p
    ON p.id_prov = f.id_prov
    WHERE f.Dominio IS NOT NULL
    """).df()


#%% GRAFICOS