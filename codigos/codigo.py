#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 13:29:30 2025

@author: saludos
"""

# %% Importación de librerías
import duckdb as dd  
import pandas as pd
import os

#%% Obtengo las rutas de los archivos
# obtengo la ruta del directorio donde se esta ejecutando este codigo
_ruta_actual = os.path.dirname(__file__)

# construyo la ruta a la carpeta hermana datos-puros que es donde estan los archivos
_ruta_carpeta = os.path.join(_ruta_actual, '../datos-puros')

# Armo las rutas para los 3 archivos, los establecimientos educativos y el padron de personas
# son .xlSx, no .csv
_ruta_cc = os.path.join(_ruta_carpeta, 'centros_culturales_2022.csv')
_ruta_ee = os.path.join(_ruta_carpeta, 'padron_establecimientos_educativos_2022.xlsx')
_ruta_pp = os.path.join(_ruta_carpeta, 'padron_poblacion_2022.xlsX')
# %% Lectura de tablas de centros culturales y establecimientos educativos

cc = pd.read_csv(_ruta_cc)
ee = pd.read_excel(_ruta_ee, header=0, skiprows=6)

#%% Lectura del padron de poblacion, transformo los datos

# leo el archivo omitiendo las primeras 13 filas
pp = pd.read_excel(_ruta_pp, usecols=[1,2,3,4], header=None, skiprows=13)
pp.columns = ["Edad", "Casos", "Porcentaje", "Porcentaje_acumulado"]

# HAgo como una especie de mascara, similar a pandas + numpy, luego uso ffill para rellenar los valores
pp = dd.sql(
    """
    SELECT *,
    CASE WHEN Edad LIKE 'AREA%'
        THEN Edad
        ELSE NULL
        END AS codigo_area,
    CASE WHEN Edad LIKE 'AREA%'
        THEN Casos
        ELSE NULL
        END AS nombre_depto
    FROM pp
    WHERE Edad != 'nan' AND Edad != 'Total' AND Edad != 'Edad' AND Edad != 'RESUMEN'
    """).df()

pp[["codigo_area", "nombre_depto"]] = pp[["codigo_area", "nombre_depto"]].ffill()

# Termino de filtrar, saco las filas sin info y dejo solo la parte numerica del codigo depto
# el CAST sirve para hacer que la variable sea INTEGER (si no queda como string)
pp = dd.sql(
    """
    SELECT Edad, Casos, Porcentaje, Porcentaje_acumulado, 
    REPLACE(codigo_area, 'AREA #', '') AS codigo_area, nombre_depto
    FROM pp
    WHERE Edad NOT LIKE 'AREA%' 
    """
    ).df()

#%% TABLA provincia

provincia = dd.sql(
    """
    SELECT DISTINCT ID_PROV AS id_provincia, UPPER(provincia) AS nombre_provincia 
    FROM cc
    ORDER BY id_provincia;
    """
    ).df()
#%% TABLA departamento

depto = dd.sql(
    """ 
    SELECT DISTINCT CAST(SUBSTRING(codigo_area, 1, 3) AS INTEGER) AS id_provincia,
    CAST(SUBSTRING(codigo_area, 4, 3) AS INTEGER) AS id_departamento,
    UPPER(nombre_depto) AS nombre_depto
    FROM pp
    """
    ).df()

#%% Busco formar una tabla llamada cod_ee con cueanexo, id_depto e id_provincia

# paso a mayusculas todo y cambio los datos de ciudad de buenos aire y tierra del fuego para que 
# haga match con los de la tabla provincia. en esta tabla la clave primaria va a ser cueanexo
cod_ee_normalizado = dd.sql(
    """ 
    SELECT Cueanexo, UPPER(Departamento) AS Departamento,
    CASE WHEN Jurisdicción = 'Ciudad de Buenos Aires' THEN 'CIUDAD AUTÓNOMA DE BUENOS AIRES'
    WHEN Jurisdicción = 'Tierra Del Fuego' THEN 'TIERRA DEL FUEGO, ANTÁRTIDA E ISLAS DEL ATLÁNTICO SUR'
    ELSE UPPER(Jurisdicción)
    END AS provincia,
    FROM ee
    """
    ).df()

# Hago un inner join de esta tabla normalizada con provincia para obtener id_provincia, y luego
# otro inner join para juntar depto (uso nombre departamento e id provincia como claves)
cod_ee = dd.sql(
    """
    SELECT n.Cueanexo, p.id_provincia, d.id_departamento
    FROM cod_ee_normalizado AS n 
    INNER JOIN provincia AS p
    ON n.provincia = p.nombre_provincia
    INNER JOIN depto AS d
    ON p.id_provincia = d.id_provincia AND n.Departamento = d.nombre_depto
    """
    ).df()

#%%