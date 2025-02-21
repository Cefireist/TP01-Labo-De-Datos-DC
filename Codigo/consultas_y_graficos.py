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
Para cada departamento informar la provincia, cantidad de EE de cada nivel educativo, 
considerando solamente la modalidad común, y cantidad de habitantes por edad según los niveles educativos. 
El orden del reporte debe ser alfabético por provincia y dentro de las provincias, 
descendente por cantidad de escuelas primarias. 
"""

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

consulta1 = dd.sql(
            """
            WITH establecimientos_por_nivel AS (
            SELECT 
                p.nombre_provincia AS provincia,
                d.nombre_departamento AS departamento,
                SUM(CASE WHEN te.tipo_establecimiento = 'Jardin_maternal' THEN 1 ELSE 0 END) AS maternales,
                SUM(CASE WHEN te.tipo_establecimiento = 'Jardin_infantes' THEN 1 ELSE 0 END) AS jardines,
                SUM(CASE WHEN te.tipo_establecimiento = 'Primario' THEN 1 ELSE 0 END) AS primarias,
                SUM(CASE WHEN te.tipo_establecimiento = 'Secundario' THEN 1 ELSE 0 END) AS secundarios,
                SUM(CASE WHEN te.tipo_establecimiento = 'Secundario_tecnico' THEN 1 ELSE 0 END) AS tecnicos
                FROM Establecimientos_educativos ee
                JOIN id_establecimientos_educativos te ON ee.id_tipo_establecimiento = te.id_tipos_establecimiento
                JOIN Departamentos d ON ee.id_prov = d.id_prov AND ee.id_depto = d.id_depto
                JOIN Provincias p ON ee.id_prov = p.id_prov
                WHERE te.tipo_establecimiento IN ('Jardin_maternal', 'Jardin_infantes', 'Primario', 'Secundario', 'Secundario_tecnico')
                GROUP BY provincia, departamento
            ),
            
            poblacion_maternal AS (
                SELECT 
                p.nombre_provincia AS provincia,
                d.nombre_departamento AS departamento,
                SUM(per.Casos) AS poblacion_maternal
                FROM Personas per
                JOIN Departamentos d ON per.id_prov = d.id_prov AND per.id_depto = d.id_depto
                JOIN Provincias p ON per.id_prov = p.id_prov
                WHERE per.Edad BETWEEN 0 AND 2
                GROUP BY provincia, departamento
            ),

            poblacion_jardin AS (
                SELECT 
                p.nombre_provincia AS provincia,
                d.nombre_departamento AS departamento,
                SUM(per.Casos) AS poblacion_jardin
                FROM Personas per
                JOIN Departamentos d ON per.id_prov = d.id_prov AND per.id_depto = d.id_depto
                JOIN Provincias p ON per.id_prov = p.id_prov
                WHERE per.Edad BETWEEN 3 AND 5
                GROUP BY provincia, departamento
            ), 

            primario6 AS (
            SELECT 
                p.nombre_provincia AS provincia,
                d.nombre_departamento AS departamento,
                SUM(per.Casos) AS poblacion_primaria
                FROM Personas per
                JOIN Departamentos d ON per.id_prov = d.id_prov AND per.id_depto = d.id_depto
                JOIN Provincias p ON per.id_prov = p.id_prov
                WHERE per.Edad BETWEEN 6 AND 12 AND per.id_prov IN (34,90,10,70,74,14,18,30,42,6,26,94)
                GROUP BY provincia, departamento
            ),

            primario7 AS (
            SELECT 
                p.nombre_provincia AS provincia,
                d.nombre_departamento AS departamento,
                SUM(per.Casos) AS poblacion_primaria
                FROM Personas per
                JOIN Departamentos d ON per.id_prov = d.id_prov AND per.id_depto = d.id_depto
                JOIN Provincias p ON per.id_prov = p.id_prov
                WHERE per.Edad BETWEEN 6 AND 13 AND per.id_prov IN (62,58,78,50,82,46,86,22,54,66,38,2)
                GROUP BY provincia, departamento
            ), 

            secundario6 AS (
            SELECT 
                p.nombre_provincia AS provincia,
                d.nombre_departamento AS departamento,
                SUM(per.Casos) AS poblacion_secundaria
                FROM Personas per
                JOIN Departamentos d ON per.id_prov = d.id_prov AND per.id_depto = d.id_depto
                JOIN Provincias p ON per.id_prov = p.id_prov
                WHERE per.Edad BETWEEN 13 AND 18 AND per.id_prov IN (34,90,10,70,74,14,18,30,42,6,26,94)
                GROUP BY provincia, departamento
            ),

            secundario7 AS (
            SELECT 
                p.nombre_provincia AS provincia,
                d.nombre_departamento AS departamento,
                SUM(per.Casos) AS poblacion_secundaria
                FROM Personas per
                JOIN Departamentos d ON per.id_prov = d.id_prov AND per.id_depto = d.id_depto
                JOIN Provincias p ON per.id_prov = p.id_prov
                WHERE per.Edad BETWEEN 14 AND 18 AND per.id_prov IN (62,58,78,50,82,46,86,22,54,66,38,2)
                GROUP BY provincia, departamento
            ), 

            poblacion_tecnica AS (
            SELECT 
                p.nombre_provincia AS provincia,
                d.nombre_departamento AS departamento,
                SUM(per.Casos) AS poblacion_tecnica
                FROM Personas per
                JOIN Departamentos d ON per.id_prov = d.id_prov AND per.id_depto = d.id_depto
                JOIN Provincias p ON per.id_prov = p.id_prov
                WHERE per.Edad = 19
                GROUP BY provincia, departamento
            ), 

            poblacion_primaria AS (
            SELECT * FROM primario6
                UNION
            SELECT * FROM primario7
            ), 

            poblacion_secundaria AS (
            SELECT * FROM secundario6
                UNION
            SELECT * FROM secundario7
            )
            
            SELECT 
                ee.provincia,
                ee.departamento,
                ee.maternales,
                pm.poblacion_maternal,
                ee.jardines,
                pj.poblacion_jardin,
                ee.primarias,
                pp.poblacion_primaria,
                ee.secundarios,
                ps.poblacion_secundaria,
                ee.tecnicos,
                pt.poblacion_tecnica
                FROM establecimientos_por_nivel ee
                LEFT JOIN poblacion_maternal pm ON ee.provincia = pm.provincia AND ee.departamento = pm.departamento
                LEFT JOIN poblacion_jardin pj ON ee.provincia = pj.provincia AND ee.departamento = pj.departamento
                LEFT JOIN poblacion_primaria pp ON ee.provincia = pp.provincia AND ee.departamento = pp.departamento
                LEFT JOIN poblacion_secundaria ps ON ee.provincia = ps.provincia AND ee.departamento = ps.departamento
                LEFT JOIN poblacion_tecnica pt ON ee.provincia = pt.provincia AND ee.departamento = pt.departamento
                ORDER BY ee.provincia ASC, ee.primarias DESC;
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
