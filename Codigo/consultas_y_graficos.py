#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 21 14:09:56 2025

@author: saludos
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



#%% CONSULTA III



#%% CONSULTA IV



#%% CONSULTA V


#%% GRAFICOS