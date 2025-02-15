#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 13:29:30 2025

@author: saludos
"""

# %% Importación de librerías
import duckdb as dd  
import pandas as pd

ruta_carpeta = "/home/saludos/Desktop/TP-01/"

# %% Lectura de tablas

cc = pd.read_csv(ruta_carpeta + "centros_culturales_2022.csv")
ee = pd.read_excel(ruta_carpeta + "padron_establecimientos_educativos_2022.xlsx", header=0, skiprows=6)

# ANALIZO EL PADRON DE POBLACION

# Leer el archivo Excel omitiendo las primeras 13 filas
pp = pd.read_excel(ruta_carpeta + "padron_poblacion_2022.xlsX", usecols=[1,2,3,4], header=None, skiprows=13)
pp.columns = ["Edad", "Casos", "Porcentaje", "Porcentaje_acumulado"]

# HAgo como una especie de mascara, similar a pandas + numpy, luego uso ffill para rellenar los valores
pp = dd.sql(
    """
    SELECT *,
    CASE WHEN Edad LIKE 'AREA%'
        THEN Edad
        ELSE NULL
        END AS Codigo_depto,
    CASE WHEN Edad LIKE 'AREA%'
        THEN Casos
        ELSE NULL
        END AS Nombre_depto
    FROM pp
    WHERE Edad != 'nan' AND Edad != 'Total' AND Edad != 'Edad'
    """).df()

pp[["Codigo_depto", "Nombre_depto"]] = pp[["Codigo_depto", "Nombre_depto"]].ffill()

# Termino de filtrar, saco las filas sin info y dejo solo la parte numerica del codigo depto
pp = dd.sql(
    """
    SELECT Edad, Casos, Porcentaje, Porcentaje_acumulado, 
    CAST(REPLACE(Codigo_depto, 'AREA #', '') AS INTEGER) AS Codigo_depto, Nombre_depto
    FROM pp
    WHERE Edad NOT LIKE 'AREA%'
    """
    ).df()
#%%
print("centros culturales")
print(cc.head(0))
print("")

print("establecimientos educativos")
print(ee.head(0))
print("")

print("padron personas")
print(pp.head(0))
print("")
#%% Empiezo a extraer datos

# TABLA provincia

provincia = dd.sql(
    """
    SELECT DISTINCT ID_PROV AS id_provincia, provincia AS nombre_provincia 
    FROM cc
    ORDER BY id;
    """
    ).df()
#%% TABLA departamento, observo que no coinciden los numeros entre la tabla de centros culturales
# y la de padron, padron tiene una base mayor pero no tiene por ejemplo los datos de "2000" -> capital federal
# por eso hago un full join

depto1 = dd.sql(
    """
    SELECT DISTINCT Codigo_depto AS id_departamento, Nombre_depto AS nombre_departamento
    FROM pp
    ORDER BY id_departamento
    """
    ).df()

depto2 = dd.sql(
    """
    SELECT DISTINCT departamento AS nombre_departamento, Jurisdicción AS nombre_provincia
    FROM ee
    """
    ).df()

depto3 = dd.sql(
    """
    SELECT d1.id_departamento, d1.nombre_departamento, d2.nombre_provincia
    FROM depto1 AS d1
    INNER JOIN depto2 AS d2
    ON UPPER(d1.nombre_departamento) = UPPER(d2.nombre_departamento)
    """
    ).df()



#%%