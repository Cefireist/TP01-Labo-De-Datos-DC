#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OBSERVACIONES IMPORTANTES:
    la base de datos con mas departamentos es la de establecimientos educativos, que a
    diferencia del padron de personas, incluye el departamento de ANTARTIDA en Tierra Del 
    Fuego. Como no tenemos datos de la poblacion alli ya que no figuran en el padron de personas,
    lo sacamos de las tablas, ya que si no reportariamos nan por ejemplo en el primer inciso.
    Tambien en la tabla de centros culturales figura como departamento ciudad autonoma de buenos
    aires en lugar de por cada comuna como en las tablas de padron de personas y de establecimientos
    educativos, asi que agregue a "departamentos" una tupla que sea como la inclusion de todo, 
    asi esta con id_prov = 2 e id_depto = 0 (como figura en centros culturales).
    Las tablas finales a usar serian centros_culturales, departamento, grupo_etario, provincia
    y establecimientos educativos.
"""

# %% Importación de librerías
import duckdb as dd  
import pandas as pd 
import os

#%% Obtencion rutas de los archivos

# ruta del directorio donde se esta ejecutando este codigo
ruta_actual = os.path.dirname(__file__)
# ruta a la carpeta TablasOriginales que es donde estan las tablas
ruta_tablas_originales = os.path.join(ruta_actual, 'TablasOriginales')

# rutas de los 3 archivos
ruta_ee = os.path.join(ruta_tablas_originales, 'padron_establecimientos_educativos_2022.xlsx')
ruta_cc = os.path.join(ruta_tablas_originales, 'centros_culturales_2022.csv')
ruta_pp = os.path.join(ruta_tablas_originales, 'padron_poblacion_2022.xlsX')

# %% Lectura padron establecimientos educativos y centros culturales
establecimientos_educativos = pd.read_excel(ruta_ee, header=0, skiprows=6)
centros_culturales = pd.read_csv(ruta_cc)

# Le agrego una clave primaria a centros_culturales, 
# no encontre combinacion de atributos suficientemente pequena como para que sea la clave primaria asi que le agregue un indice
centros_culturales = dd.sql("""
    SELECT ROW_NUMBER() OVER () AS id_cc, *
    FROM centros_culturales;
""").df()


#%% Lectura y limpieza del padron de poblacion

# leo el archivo omitiendo las primeras 13 filas y seleccionando columnas a usar
poblacion = pd.read_excel(ruta_pp, usecols=[1,2,3,4], header=None, skiprows=13)
poblacion.columns = ["Edad", "Casos", "Porcentaje", "Porcentaje_acumulado"]

## Limpieza tabla poblacion
# se agregan columnas codigo_area y nombre_depto
poblacion = dd.sql(
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
    FROM poblacion
    WHERE LOWER(Edad) NOT IN ('nan', 'total', 'edad', 'resumen')
    """).df()

# relleno hacia abajo con el ultimo valor no nulo
poblacion[["codigo_area", "nombre_depto"]] = poblacion[["codigo_area", "nombre_depto"]].ffill()

# eliminacion de filas sin info ; formateo codigo_area dejando solo el valor numerico
poblacion = dd.sql(
    """
    SELECT Edad, Casos, Porcentaje, Porcentaje_acumulado, 
    REPLACE(codigo_area, 'AREA # ', '') AS codigo_area, nombre_depto
    FROM poblacion
    WHERE Edad NOT LIKE 'AREA%' 
    """
    ).df()

#%% Tabla "grupo_etario_personas": guarda la edad, el número de casos, id_provincia e id_depto
# el CAST sirve para hacer que la variable sea INTEGER (si no queda como string)
grupo_etario_personas = dd.sql(
    """ 
    SELECT CAST(SUBSTRING(codigo_area, 1, 2) AS INTEGER) AS id_provincia,
    CAST(SUBSTRING(codigo_area, 3, 3) AS INTEGER) AS id_departamento, Edad, Casos
    FROM poblacion
    """
    ).df()

#%% Tabla "departamentos": guarda el nombre, id_departamento e id_provincia

# le agrego como caso particular el departamento "Ciudad Autonoma De Buenos Aires" de id_depto = 0
# y id_prov = 2 ya que en centros culturales no hay info sobre cada comuna
departamentos = dd.sql(
    """ 
    SELECT DISTINCT CAST(SUBSTRING(codigo_area, 1, 2) AS INTEGER) AS id_provincia,
    CAST(SUBSTRING(codigo_area, 3, 3) AS INTEGER) AS id_departamento,
    UPPER(nombre_depto) AS nombre_depto
    FROM poblacion
    UNION 
    SELECT DISTINCT ID_PROV, CAST(SUBSTRING(CAST(ID_DEPTO AS VARCHAR), 2, 4) AS INTEGER) AS id_depto, Departamento
    FROM centros_culturales
    WHERE ID_PROV = 2
    """
    ).df()

#%% TABLA "provincias": almacena nombre y identificador de cada provincia

provincias = dd.sql(
    """
    SELECT DISTINCT ID_PROV AS id_provincia, UPPER(provincia) AS nombre_provincia 
    FROM centros_culturales
    ORDER BY id_provincia;
    """
    ).df()

#%% recorte de info extraida de centros_culturales

# extraigo id_cc id_depto id_prov capacidad mail
centros_culturales = dd.sql(
    """
    SELECT DISTINCT id_cc, ID_PROV AS id_prov, 
    CAST(RIGHT(CAST(id_depto AS VARCHAR), LENGTH(CAST(id_depto AS VARCHAR)) - LENGTH(CAST(id_prov AS VARCHAR))) AS INTEGER) AS id_depto,
    Capacidad, "Mail " AS Mail
    FROM centros_culturales;
    """).df()

#%% Tabla "cod_ee_normalizado": con cueanexo, id_depto e id_provincia, etc.

# paso a mayusculas todo y cambio los datos de ciudad de buenos aires y tierra del fuego para que 
# haga match con los de la tabla provincia. en esta tabla la clave primaria va a ser cueanexo
cod_ee_normalizado = dd.sql(
    """ 
    SELECT UPPER(Departamento) AS Departamento, *,
    CASE WHEN Jurisdicción = 'Ciudad de Buenos Aires' THEN 'CIUDAD AUTÓNOMA DE BUENOS AIRES'
    WHEN Jurisdicción = 'Tierra Del Fuego' THEN 'TIERRA DEL FUEGO, ANTÁRTIDA E ISLAS DEL ATLÁNTICO SUR'
    ELSE UPPER(Jurisdicción)
    END AS provincia,
    FROM establecimientos_educativos
    """
    ).df()

# Hago un inner join de esta tabla normalizada con provincia para obtener id_provincia, y luego
# otro inner join para juntar depto (uso nombre departamento e id provincia como claves)
establecimientos_educativos = dd.sql(
    """
    SELECT n.Cueanexo, p.id_provincia, d.id_departamento,
    "Nivel inicial - Jardín maternal" AS jar_mat,
    "Nivel inicial - Jardín de infantes" AS jar_inf, Primario, Secundario,
    "Secundario - INET" AS Secundario_tecnico
    FROM cod_ee_normalizado AS n 
    INNER JOIN provincias AS p
    ON n.provincia = p.nombre_provincia
    INNER JOIN departamentos AS d
    ON p.id_provincia = d.id_provincia AND n.Departamento = d.nombre_depto
    """
    ).df()

#%%

