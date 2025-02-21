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
        END AS nombre_departamento,
    CASE WHEN Edad = 'RESUMEN'
        THEN Edad
        ELSE NULL
    END AS mascara_para_borrar
    FROM padron_personas
    WHERE LOWER(Edad) NOT IN ('nan', 'total', 'edad')
    """).df()
#%%
# relleno hacia abajo con el ultimo valor no nulo
padron_personas[["codigo_area", "nombre_departamento","mascara_para_borrar"]] = padron_personas[["codigo_area", "nombre_departamento", "mascara_para_borrar"]].ffill()

hola = dd.sql(
       """
       SELECT *
       FROM padron_personas
       WHERE mascara_para_borrar IS NULL
       """
       ).df()
#%%

# GUARDAMOS LA TABLA PERSONAS CON LOS ATRIBUTOS: edad, número de casos, id_prov, id_depto
# eliminacion de filas usadas para el forward fill, y separo el codigo de area en id_prov 
# e id_depto el CAST sirve para cambiar el tipo de variable, pasamos de string a integer

# cambio el id de Ushuaia y RIO GRANDE porque estan distintos a la otra tabla
Personas = dd.sql(
    """
    WITH pp AS (
    SELECT nombre_departamento, CAST(Edad AS INTEGER) AS Edad, 
    CAST(Casos AS INTEGER) AS Casos,
    CAST(SUBSTRING(codigo_area, 1, 2) AS INTEGER) AS id_prov,
    CAST(SUBSTRING(codigo_area, 3, 3) AS INTEGER) AS id_depto
    FROM padron_personas
    WHERE Edad NOT LIKE 'AREA%' AND mascara_para_borrar IS NULL)
    
    SELECT id_prov,
    CASE     
        WHEN id_prov = 94 AND id_depto = 8 THEN 7
        WHEN id_prov = 94 AND id_depto = 15 THEN 14
        ELSE id_depto
        END AS id_depto, Edad, Casos
    FROM pp 
    """).df()


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

# Creo una tabla que vincule con una id al nombre de tipo de establecimiento y armo una tabla
columnas = {
    "id_tipos_establecimiento": [0,1,2,3,4],
    "tipo_establecimiento": ["Jardin_maternal", "Jardin_infantes", "Primario", "Secundario", "Secundario_tecnico"]
}
id_tipo_establecimiento_educativo = pd.DataFrame(columnas)

# Separo para cada cuanexo si tiene uno o varios tipos de establecimiento, asi no hay tantos valores NULL
# La clave primaria de Establecimientos_educativos es Cueanexo junto a id_tipo_establecimiento
Establecimientos_educativos = dd.sql(
    """
    WITH ee2 AS (
        SELECT Cueanexo, id_prov, id_depto, Jardin_maternal, Jardin_infantes, 
        Primario, Secundario, Secundario_tecnico,
        FROM ee
        )
    SELECT Cueanexo, id_prov, id_depto, 0 AS id_tipo_establecimiento
    FROM ee2
    WHERE Jardin_maternal = '1'

    UNION 
    SELECT Cueanexo, id_prov, id_depto, 1 AS id_tipo_establecimiento
    FROM ee2
    WHERE Jardin_infantes = '1'

    UNION
    SELECT Cueanexo, id_prov, id_depto, 2 AS id_tipo_establecimiento
    FROM ee2
    WHERE Primario = '1'

    UNION
    SELECT Cueanexo, id_prov, id_depto, 3 AS id_tipo_establecimiento
    FROM ee2
    WHERE Secundario = '1'

    UNION
    SELECT Cueanexo, id_prov, id_depto, 4 AS id_tipo_establecimiento
    FROM ee2
    WHERE Secundario_tecnico = '1';
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
cc = dd.sql(
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

Centros_culturales = dd.sql(
    """
    WITH primer_mail AS (
    SELECT id_cc, id_prov, id_depto, Capacidad,
    CASE WHEN ',' IN Mail
    THEN SPLIT_PART(TRIM(Mail), ',', 1)
    ELSE SPLIT_PART(TRIM(Mail), ' ', 1) 
    END AS Mail
    FROM cc)
    SELECT id_cc, id_prov, id_depto, Capacidad,
    CASE WHEN '@' IN Mail
    THEN REPLACE(REPLACE(Mail,' ', ''), ',', '')
    ELSE NULL
    END AS Mail
    FROM primer_mail
    """).df()
    
#%% GUARDO LAS TABLAS

carpeta_destino = os.path.join(os.path.dirname(os.path.abspath(__file__)), "TablasModelo")
os.makedirs(carpeta_destino, exist_ok = True)  # Crea la carpeta si no existe

tablas = {
    "Personas": Personas,
    "Centros_culturales": Centros_culturales,
    "Establecimientos_educativos": Establecimientos_educativos,
    "id_tipo_establecimiento_educativo": id_tipo_establecimiento_educativo,
    "Departamentos": Departamentos,
    "Provincias": Provincias
}

for nombre_tabla, df in tablas.items():
    ruta_del_csv = os.path.join(carpeta_destino, f"{nombre_tabla}.csv")
    df.to_csv(ruta_del_csv, index = False)

#%%