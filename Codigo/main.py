#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OBSERVACIONES IMPORTANTES:
    la base de datos con mas departamentos es la de establecimientos educativos, que a
    diferencia del padron de personas, incluye el departamento de ANTARTIDA en Tierra Del 
    Fuego.Tambien en la tabla de centros culturales figura como departamento ciudad autonoma de buenos
    aires en lugar de por cada comuna como en las tablas de padron de personas y de establecimientos
    educativos, asi que agregue a "departamentos" una tupla que sea como la inclusion de todo, 
    asi esta con id_prov = 2 e id_depto = 0 (como figura en centros culturales).
    nota: esta incluido el departamento Antartida, como ahora las arme de otra forma habria que 
    sacarlo con un where si se quiere.
"""

# %% IMPORTACION DE LIBRERIAS

import duckdb as dd  
import pandas as pd 
import os

#%% OBTENCION DE LAS RUTAS DE LOS ARCHIVOS A USAR

# obtenemos la ruta del directorio donde se esta ejecutando este codigo
ruta_actual = os.path.dirname(__file__)

# ruta a la carpeta TablasOriginales que es donde estan las tablas a usar
ruta_tablas_originales = os.path.join(ruta_actual, 'TablasOriginales')

# rutas de los 3 archivos, usamos esta libreria para que el codigo encuentre los archivos siempre
ruta_ee = os.path.join(ruta_tablas_originales, 'padron_establecimientos_educativos_2022.xlsx')
ruta_cc = os.path.join(ruta_tablas_originales, 'centros_culturales_2022.csv')
ruta_pp = os.path.join(ruta_tablas_originales, 'padron_poblacion_2022.xlsX')

# %% LECTURA DE LAS 3 TABLAS, ESTABLECIMIENTOS EDUCATIVOS, CENTROS CULTURALES Y PADRON DE PERSONAS

establecimientos_educativos = pd.read_excel(ruta_ee, header=0, skiprows=6)
centros_culturales = pd.read_csv(ruta_cc)
padron_personas = pd.read_excel(ruta_pp, usecols=[1,2], header=None, skiprows=13)
padron_personas.columns = ["Edad", "Casos"]

""" Le agrego una clave primaria a centros_culturales, no encontre combinacion 
de atributos suficientemente pequena como para que sea la clave primaria asi que le 
agregue un indice
"""
centros_culturales = dd.sql("""
    SELECT ROW_NUMBER() OVER () AS id_cc, *
    FROM centros_culturales;
""").df()


#%% LIMPIEZA TABLA PADRON DE PERSONAS

# se agregan columnas codigo_area y nombre_depto (pasando a mayuscular) para identificar 
# cada departamento, y se borran valores que no sirven
padron_personas = dd.sql(
    """
    SELECT *,
    CASE WHEN Edad LIKE 'AREA%'
        THEN REPLACE(Edad, 'AREA # ', '') 
        ELSE NULL
        END AS codigo_area,
    CASE WHEN Edad LIKE 'AREA%'
        THEN UPPER(Casos)
        ELSE NULL
        END AS nombre_departamento
    FROM padron_personas
    WHERE LOWER(Edad) NOT IN ('nan', 'total', 'edad', 'resumen')
    """).df()

# relleno hacia abajo con el ultimo valor no nulo
padron_personas[["codigo_area", "nombre_departamento"]] = padron_personas[["codigo_area", "nombre_departamento"]].ffill()



# GUARDAMOS LA TABLA PERSONAS CON LOS ATRIBUTOS: edad, número de casos, id_prov, id_depto
# eliminacion de filas usadas para el forward fill, y separo el codigo de area en id_prov 
# e id_depto el CAST sirve para cambiar el tipo de variable, pasamos de string a integer
Personas = dd.sql(
    """
    WITH pp AS (
    SELECT nombre_departamento, CAST(Edad AS INTEGER) AS Edad, 
    CAST(Casos AS INTEGER) AS Casos,
    CAST(SUBSTRING(codigo_area, 1, 2) AS INTEGER) AS id_prov,
    CAST(SUBSTRING(codigo_area, 3, 3) AS INTEGER) AS id_depto
    FROM padron_personas
    WHERE Edad NOT LIKE 'AREA%')
    SELECT id_prov, id_depto, Edad, Casos 
    FROM pp
    """
    ).df()


#%% Tabla ee guarda mucha info y luego voy separando

#%%
ee = dd.sql(
    """
    WITH ee_2 AS (
        SELECT CAST("Código de localidad" AS VARCHAR) AS cod_loc, 
        UPPER(Jurisdicción) AS nombre_provincia, UPPER(Departamento) AS nombre_departamento, *
        FROM establecimientos_educativos)
    
    SELECT Cueanexo, nombre_provincia, nombre_departamento,
    "Nivel inicial - Jardín maternal" AS Jardin_maternal,
    "Nivel inicial - Jardín de infantes" AS Jardin_infantes, Primario, Secundario,
    "Secundario - INET" AS Secundario_tecnico,
    CASE WHEN LENGTH(cod_loc) == 7
    THEN CAST(SUBSTRING(cod_loc, 1, 1) AS INTEGER)
    ELSE CAST(SUBSTRING(cod_loc, 1, 2) AS INTEGER)
    END AS id_prov,
    CASE WHEN nombre_departamento LIKE 'COMUNA%' 
    THEN CAST(SUBSTRING(cod_loc, 3, 2) AS INTEGER) * 7
    WHEN LENGTH(cod_loc) == 7 
    THEN CAST(SUBSTRING(cod_loc, 2, 3) AS INTEGER)
    ELSE CAST(SUBSTRING(cod_loc, 3, 3) AS INTEGER)
    END AS id_depto
    FROM ee_2
    """).df()

#%%
Departamentos = dd.sql(
    """ 
    SELECT DISTINCT id_prov, id_depto, nombre_departamento
    FROM ee
    UNION 
    SELECT DISTINCT ID_PROV, CAST(SUBSTRING(CAST(ID_DEPTO AS VARCHAR), 2, 4) AS INTEGER) AS id_depto, Departamento
    FROM centros_culturales
    WHERE ID_PROV = 2
    """
    ).df()
#%%
Establecimientos_educativos = dd.sql(
    """ 
    SELECT Cueanexo, id_prov, id_depto, Jardin_maternal, Jardin_infantes, 
    Primario, Secundario, Secundario_tecnico,
    FROM ee
    """
    ).df()
#%%
Provincias = dd.sql(
    """
    SELECT DISTINCT id_prov, nombre_provincia
    FROM ee
    """
    ).df()

#%%
Centros_culturales = dd.sql(
    """
    WITH cc AS (
        SELECT id_cc, ID_PROV AS id_prov, CAST(ID_DEPTO AS VARCHAR) AS id_prov_depto,
        "Mail " AS Mail, Capacidad
        FROM centros_culturales)
    SELECT id_cc, id_prov,
    CASE WHEN LENGTH(id_prov_depto) == 4
        THEN CAST(SUBSTRING(id_prov_depto, 2, 4) AS INTEGER)
        ELSE CAST(SUBSTRING(id_prov_depto, 3, 5) AS INTEGER)
        END AS id_depto,
    Mail, Capacidad
    FROM cc
    """
    ).df()

Centros_culturales_final == dd.sql(
    """
    SELECT id_cc,
    CASE 
        WHEN POSITION(' ' IN Mail) > 0 THEN SUBSTRING(email, 1, POSITION(' ' IN Mail) - 1)
        ELSE email
    END AS email_1,
    
    CASE 
        WHEN POSITION(' ' IN Mail) > 0 THEN SUBSTRING(email, POSITION(' ' IN Mail) + 1)
        ELSE NULL
    END AS email_2

    FROM Centros_culturales
    """
    
#%%
#Consultas en SQL

## Provincias donde el primario dura 6 años
primario6 = ["Formosa", "Tucuman", "Catamarca"] 
    
consultai = dd.sql(
            """
            SELECT id_prov, id_depto, SUM(Casos) AS poblacion_jardin, 
            """






    

