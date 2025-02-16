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

#%% Obtengo las rutas de los archivos
# obtengo la ruta del directorio donde se esta ejecutando este codigo
_ruta_actual = os.path.dirname(__file__)

# construyo la ruta a la carpeta hermana datos-puros que es donde estan los archivos
_ruta_carpeta = os.path.join(_ruta_actual, './TablasOriginales')

# Armo las rutas para los 3 archivos, los establecimientos educativos y el padron de personas
# son .xlSx, no .csv
_ruta_cc = os.path.join(_ruta_carpeta, 'centros_culturales_2022.csv')
_ruta_ee = os.path.join(_ruta_carpeta, 'padron_establecimientos_educativos_2022.xlsx')
_ruta_pp = os.path.join(_ruta_carpeta, 'padron_poblacion_2022.xlsX')
# %% Lectura de tablas de centros culturales y establecimientos educativos

_cc = pd.read_csv(_ruta_cc)
# Le agrego una clave primaria, no encontre combinacion de atributos suficientemente pequena
# como para que sea la clave primaria asi que le agregue un indice
cc = dd.sql("""
    SELECT ROW_NUMBER() OVER () AS id_cc, *
    FROM _cc;
""").df()

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

# Finalmente solo me quedo con id_provincia, id_depto, edad y numero de casos
grupo_etario_personas = dd.sql(
    """ 
    SELECT CAST(SUBSTRING(codigo_area, 1, 3) AS INTEGER) AS id_provincia,
    CAST(SUBSTRING(codigo_area, 4, 3) AS INTEGER) AS id_departamento, Edad, Casos
    FROM pp
    """
    ).df()

#%% TABLA departamento, tiene el nombre de departamento, el id_departamento, el id_provincia
# le agrego como caso particular el departamento "Ciudad Autonoma De Buenos Aires" de id_depto = 0
# y id_prov = 2 ya que en centros culturales no hay info sobre cada comuna

departamento = dd.sql(
    """ 
    SELECT DISTINCT CAST(SUBSTRING(codigo_area, 1, 3) AS INTEGER) AS id_provincia,
    CAST(SUBSTRING(codigo_area, 4, 3) AS INTEGER) AS id_departamento,
    UPPER(nombre_depto) AS nombre_depto
    FROM pp
    UNION 
    SELECT DISTINCT ID_PROV, CAST(SUBSTRING(CAST(ID_DEPTO AS VARCHAR), 2, 4) AS INTEGER) AS id_depto, Departamento
    FROM cc
    WHERE ID_PROV = 2
    """
    ).df()

#%%
centros_culturales = dd.sql(
    """
    SELECT DISTINCT id_cc, ID_PROV AS id_prov, 
    CAST(RIGHT(CAST(id_depto AS VARCHAR), LENGTH(CAST(id_depto AS VARCHAR)) - LENGTH(CAST(id_prov AS VARCHAR))) AS INTEGER) AS id_depto,
    Capacidad, "Mail " AS Mail
    FROM cc;
    """).df()
    
#%% TABLA provincia

provincia = dd.sql(
    """
    SELECT DISTINCT ID_PROV AS id_provincia, UPPER(provincia) AS nombre_provincia 
    FROM cc
    ORDER BY id_provincia;
    """
    ).df()

#%% Busco formar una tabla llamada cod_ee con cueanexo, id_depto e id_provincia, etc.

# paso a mayusculas todo y cambio los datos de ciudad de buenos aire y tierra del fuego para que 
# haga match con los de la tabla provincia. en esta tabla la clave primaria va a ser cueanexo
_cod_ee_normalizado = dd.sql(
    """ 
    SELECT UPPER(Departamento) AS Departamento, *,
    CASE WHEN Jurisdicción = 'Ciudad de Buenos Aires' THEN 'CIUDAD AUTÓNOMA DE BUENOS AIRES'
    WHEN Jurisdicción = 'Tierra Del Fuego' THEN 'TIERRA DEL FUEGO, ANTÁRTIDA E ISLAS DEL ATLÁNTICO SUR'
    ELSE UPPER(Jurisdicción)
    END AS provincia,
    FROM ee
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
    FROM _cod_ee_normalizado AS n 
    INNER JOIN provincia AS p
    ON n.provincia = p.nombre_provincia
    INNER JOIN departamento AS d
    ON p.id_provincia = d.id_provincia AND n.Departamento = d.nombre_depto
    """
    ).df()




    
#%%


#%%