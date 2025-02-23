#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Laboratorio de datos - Verano 2025
Trabajo Práctico N° 1 

Integrantes:
- Sebastian Ceffalotti - sebastian.ceffalotti@gmail.com
- Aaron Cuellar - aaroncuellar2003@gmail.com
- Rodrigo Coppa - rodrigo.coppa98@gmail.com

Descripción:
Este script realiza la lectura y limpieza de las fuentes de datos dadas, 
ademas genera las consultas y visualizaciones pedidas.

Detalles técnicos:
- Lenguaje: Python
- Librerías utilizadas: numpy, matplotlib, duckdb, pandas, seaborn y scikit-learn

"""

# %% IMPORTACION DE LIBRERIAS

import duckdb as dd  
import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import r2_score
import os

#%% OBTENCION DE LAS RUTAS DE LOS ARCHIVOS ORIGINALES A USAR

# obtenemos la ruta del directorio donde se esta ejecutando este codigo, usamos OS para que
# sea independiente de donde se abra
_ruta_actual = os.path.dirname(__file__)

# ruta a la carpeta TablasOriginales que es donde estan las tablas a usar
_ruta_tablas_originales = os.path.join(_ruta_actual, 'TablasOriginales')

# rutas de los 3 archivos, usamos esta libreria para que el codigo encuentre los archivos siempre
_ruta_ee = os.path.join(_ruta_tablas_originales, 'padron_establecimientos_educativos_2022.xlsx')
_ruta_cc = os.path.join(_ruta_tablas_originales, 'centros_culturales_2022.csv')
_ruta_pp = os.path.join(_ruta_tablas_originales, 'padron_poblacion_2022.xlsX')


# %% LECTURA DE LAS 3 FUENTES DE DATOS, ESTABLECIMIENTOS EDUCATIVOS, CENTROS CULTURALES Y PADRON DE PERSONAS

_establecimientos_educativos_original = pd.read_excel(_ruta_ee, header=0, skiprows=6)
_centros_culturales_original = pd.read_csv(_ruta_cc)
_padron_personas = pd.read_excel(_ruta_pp, usecols=[1,2], header=None, skiprows=13)
_padron_personas.columns = ["Edad", "Casos"]

# Le agregamos una clave primaria numerica a centros_culturales
Centros_culturales = dd.sql("""
    SELECT ROW_NUMBER() OVER () AS id_cc, *
    FROM _centros_culturales_original;
""").df()

#%% LIMPIEZA TABLA PADRON DE PERSONAS

# se agregan columnas codigo_area y nombre_depto (pasando a mayusculas) para identificar 
# cada departamento, y se borran valores que no sirven. ademas se crea una mascara para rellenar
# con valores. la que es de resumen es porque al final del archivo hay una tabla resumen que da toda
# la info del pais junta, la queremos eliminar.
_padron_personas = dd.sql(
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
    FROM _padron_personas
    WHERE LOWER(Edad) NOT IN ('nan', 'total', 'edad')
    """).df()

# relleno hacia abajo con el ultimo valor no nulo usando forward fill. 
_padron_personas[["codigo_area", "nombre_departamento","mascara_para_borrar"]] = _padron_personas[["codigo_area", "nombre_departamento", "mascara_para_borrar"]].ffill()

#%%

# eliminamos filas usadas para el forward fill de pandas y los datos asociados a resumen,
# separamos el codigo de area en id_prov e id_depto, el CAST sirve para cambiar el tipo de
# variable, pasamos de string a integer
# se cambia el id de Ushuaia y Rio Grande porque estan distintos a las tablas de CC y EE
Poblacion = dd.sql(
    """
    WITH pp AS (
    SELECT nombre_departamento, CAST(Edad AS INTEGER) AS Edad, 
    CAST(Casos AS INTEGER) AS Casos,
    CAST(SUBSTRING(codigo_area, 1, 2) AS INTEGER) AS id_prov,
    CAST(SUBSTRING(codigo_area, 3, 3) AS INTEGER) AS id_depto
    FROM _padron_personas
    WHERE Edad NOT LIKE 'AREA%' AND mascara_para_borrar IS NULL)
    
    SELECT id_prov, 
    CASE 
        WHEN id_prov = 94 AND id_depto = 8 THEN 7
        WHEN id_prov = 94 AND id_depto = 15 THEN 14
        ELSE id_depto
    END AS id_depto, Edad, Casos 
    FROM pp
    """).df()


#%% tabla Tipos_de_establecimientos

# Asociamos a cada tipo de establecimiento un id numerico, y guardamos un dataframe
datos_establecimientos = {
    "id_tipo_establecimiento": [0,1,2,3,4],
    "tipo_establecimiento": ["Jardin_maternal", "Jardin_infantes", "Primario", "Secundario", "Secundario_tecnico"]
}

Tipos_de_establecimientos = pd.DataFrame(datos_establecimientos)


# guardamos una tabla con la informacion para armar Provincias, Departamentos, 
# Establecimientos_educativos y ee_tipo_establecimiento
ee_info = dd.sql(
    """
    WITH ee_2 AS (
        SELECT CAST("Código de localidad" AS VARCHAR) AS cod_loc,
        UPPER(Jurisdicción) AS nombre, UPPER(Departamento) AS nombre_departamento, *
        FROM _establecimientos_educativos_original
        WHERE Común = '1')
    
    SELECT Cueanexo, nombre, nombre_departamento, Ámbito, Sector,
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
#%% tabla ee_tipo_establecimiento

# teniendo en cuenta el id separamos las columnas de cada tipo de establecimiento educativo

ee_tipo_establecimiento = dd.sql(
    """
    SELECT Cueanexo, 0 AS id_tipo_establecimiento
    FROM ee_info
    WHERE Jardin_maternal = '1'

    UNION 
    
    SELECT Cueanexo, 1 AS id_tipo_establecimiento
    FROM ee_info
    WHERE Jardin_infantes = '1'

    UNION
    
    SELECT Cueanexo, 2 AS id_tipo_establecimiento
    FROM ee_info
    WHERE Primario = '1'

    UNION
    
    SELECT Cueanexo, 3 AS id_tipo_establecimiento
    FROM ee_info
    WHERE Secundario = '1'

    UNION
    
    SELECT Cueanexo, 4 AS id_tipo_establecimiento
    FROM ee_info
    WHERE Secundario_tecnico = '1';
    """).df()
#%% Tabla establecimientos_educativos, seleccionamos atributos de ee_info

Establecimientos_educativos = dd.sql(
    """
    SELECT Cueanexo, id_prov, id_depto, Ámbito AS ambito, Sector AS sector
    FROM ee_info
    """).df()

#%% Tabla departamentos, seleccionamos los atributos de ee_info

# Agregamos todo caba como un departamento tambien teniendo el cuenta la tabla de CC
Departamentos = dd.sql(
    """ 
    SELECT DISTINCT id_prov, id_depto, nombre_departamento AS nombre
    FROM ee_info
    UNION 
    SELECT DISTINCT ID_PROV, 
    CAST(SUBSTRING(CAST(ID_DEPTO AS VARCHAR), 2, 4) AS INTEGER) AS id_depto, Departamento
    FROM Centros_culturales
    WHERE ID_PROV = 2
    """).df()

#%% Tabla provincias
Provincias = dd.sql(
    """
    SELECT DISTINCT id_prov, nombre
    FROM ee_info
    """).df()


#%% Tabla cc_info guarda info relacionada a centros_culturales para armar otras tablas

cc_info = dd.sql(
    """
    WITH cc_info AS (
        SELECT id_cc, ID_PROV AS id_prov, CAST(ID_DEPTO AS VARCHAR) AS id_prov_depto,
        "Mail " AS Mail, Capacidad
        FROM Centros_culturales)
    SELECT id_cc, id_prov,
    CASE WHEN LENGTH(id_prov_depto) == 4
        THEN CAST(SUBSTRING(id_prov_depto, 2, 4) AS INTEGER)
        ELSE CAST(SUBSTRING(id_prov_depto, 3, 5) AS INTEGER)
        END AS id_depto,
    Mail, Capacidad
    FROM cc_info
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
        FROM cc_info)
    
    SELECT id_cc, id_prov, id_depto, Capacidad,
    CASE WHEN '@' IN Mail
    THEN REPLACE(REPLACE(Mail,' ', ''), ',', '')
    ELSE NULL
    END AS Mail
    FROM primer_mail
    """).df()

#%% GUARDADO DE TABLAS EN TABLASMODELO

_carpeta_destino = os.path.join(os.path.dirname(os.path.abspath(__file__)), "TablasModelo")
os.makedirs(_carpeta_destino, exist_ok = True)  # Crea la carpeta si no existe

_tablas = {
    "Poblacion": Poblacion,
    "Centros_culturales": Centros_culturales,
    "Establecimientos_educativos": Establecimientos_educativos,
    "Tipos_de_establecimientos": Tipos_de_establecimientos,
    "Departamentos": Departamentos,
    "Provincias": Provincias,
    "ee_tipo_establecimiento": ee_tipo_establecimiento
}

for nombre_tabla, df in _tablas.items():
    _ruta_del_csv = os.path.join(_carpeta_destino, f"{nombre_tabla}.csv")
    df.to_csv(_ruta_del_csv, index = False)
    
#%% LECTURA DE LAS TABLAS MODELO

_ruta_tablas_modelo = os.path.join(_ruta_actual, 'TablasModelo')
# Diccionario con las rutas de los archivos CSV
_rutas = {
    "Centros_culturales": os.path.join(_ruta_tablas_modelo, 'Centros_culturales.csv'),
    "Poblacion": os.path.join(_ruta_tablas_modelo, 'Poblacion.csv'),
    "Establecimientos_educativos": os.path.join(_ruta_tablas_modelo, 'Establecimientos_educativos.csv'),
    "Tipos_de_establecimientos": os.path.join(_ruta_tablas_modelo, 'Tipos_de_establecimientos.csv'), 
    "Departamentos": os.path.join(_ruta_tablas_modelo, 'Departamentos.csv'),
    "Provincias": os.path.join(_ruta_tablas_modelo, 'Provincias.csv'),
    "ee_tipo_establecimiento": os.path.join(_ruta_tablas_modelo, 'ee_tipo_establecimiento.csv')
}

# guardo en una variable el csv con el nombre, para eso uso globals que genera una variable dinamicamente 
for nombre, ruta in _rutas.items():
    globals()[nombre] = pd.read_csv(ruta)
    
#%% ANALISIS DE DATOS
 
#%%
# CONSULTA I 
"""
Para cada departamento informar la provincia, cantidad de EE de cada nivel educativo, 
considerando solamente la modalidad común, y cantidad de habitantes por edad según los niveles educativos. 
El orden del reporte debe ser alfabético por provincia y dentro de las provincias, 
descendente por cantidad de escuelas primarias. 
"""

"""
Provincias donde el primario dura 6 años: Formosa, Tucumán, Catamarca, San Juan, San Luis,
Córdoba, Corrientes, Entre Ríos, La Pampa, Buenos Aires, Chubut y Tierra del Fuego. 

Provincias donde el primario dura 7 años: Río Negro, Neuquén, Santa Cruz, Mendoza, Santa Fe, La Rioja, 
Santiago del Estero, Chaco, Misiones, Salta, Jujuy, pero tambien en la Ciudad Autónoma de Buenos Aires.

En donde el primario dura 6 anios el secundario dura 5
"""

# Guardamos los id de la provincias segun la duracion del primario de 6 o 7 annios
primario6 = (34,90,10,70,74,14,18,30,42,6,26,94)
primario7 = (62,58,78,50,82,46,86,22,54,66,38,2)

# Guardamos en Poblacion_jardin a las personas entre 0 y 5 anios, incluyendo jardin maternal y de infantes
Poblacion_jardin = dd.sql(
            """
            SELECT id_prov, id_depto, SUM(Casos) AS Poblacion_jardin
            FROM Poblacion
            WHERE Edad <= 5
            GROUP BY id_prov, id_depto
            """).df()

# Guardamos en Poblacion_primaria las personas en edad de primaria teniendo en cuenta la provincia
Poblacion_primaria = dd.sql(
            f"""
            WITH primario6 AS (
            SELECT id_prov, id_depto, SUM(Casos) AS Poblacion_primaria
            FROM Poblacion
            WHERE Edad > 5 AND Edad <= 12 AND id_prov IN {primario6}
            GROUP BY id_prov, id_depto),
            
            primario7 AS (SELECT id_prov, id_depto, SUM(Casos) AS Poblacion_primaria
            FROM Poblacion
            WHERE Edad > 5 AND Edad <= 13 AND id_prov IN {primario7}
            GROUP BY id_prov, id_depto)
            
            SELECT *
            FROM primario6
            UNION
            SELECT *
            FROM primario7 
            """).df()
            
# Guardamos en Poblacion secundaria la cantidad de personas en edad de seduncaria, teniendo en cuenta
# las provincias, pero incluyendo tambien la gente en edad para las escuelas tecnicas (duran 1 anio mas)
Poblacion_secundaria = dd.sql(
            f"""
            WITH secundario6 AS(
            SELECT id_prov, id_depto, SUM(Casos) AS Poblacion_secundaria
            FROM Poblacion
            WHERE Edad > 12 AND Edad <= 19 AND id_prov IN {primario6}
            GROUP BY id_prov, id_depto),
            
            secundario5 AS (SELECT id_prov, id_depto, SUM(Casos) AS Poblacion_secundaria
            FROM Poblacion
            WHERE Edad > 13 AND Edad <= 19 AND id_prov IN {primario7}
            GROUP BY id_prov, id_depto)
            
            SELECT *
            FROM secundario6
            UNION
            SELECT *
            FROM secundario5
            """
            ).df()

# Se guarda en Cantidad_ee cuantos establecimientos educativos hay en cada departamento, se incluyen
# TODOS los ee, incluso aquellos en los que no hay ningun establecimiento en modalidad comun (se cuenta por
# cueanexo)
Cantidad_ee = dd.sql(
    """
    WITH Conteo AS (
    SELECT ee.id_prov, ee.id_depto, ete.id_tipo_establecimiento, COUNT(*) AS Cantidad
    FROM Establecimientos_educativos AS ee
    INNER JOIN ee_tipo_establecimiento AS ete 
    ON ee.Cueanexo = ete.Cueanexo
    GROUP BY id_prov, id_depto, id_tipo_establecimiento
    ORDER BY id_prov, id_depto, id_tipo_establecimiento)
    
    SELECT id_prov, id_depto,
    SUM(CASE WHEN id_tipo_establecimiento IN (0, 1) THEN Cantidad ELSE 0 END) AS Jardines,
    SUM(CASE WHEN id_tipo_establecimiento = 2 THEN Cantidad ELSE 0 END) AS Primarias,
    SUM(CASE WHEN id_tipo_establecimiento IN (3, 4) THEN Cantidad ELSE 0 END) AS Secundario
    FROM Conteo
    GROUP BY id_prov, id_depto
    """).df()

# Se juntan los datos de las tablas anteriores y se ordena de acuerdo a la consigna
Consulta1 = dd.sql(
    """
    WITH prov_depto AS (
    SELECT p.nombre AS Provincia, d.nombre AS Departamento, 
    p.id_prov, d.id_depto
    FROM Departamentos AS d
    INNER JOIN Provincias AS p
    ON d.id_prov = p.id_prov),
    
    Poblaciones AS (
        SELECT pp.id_prov, pp.id_depto, Poblacion_jardin, Poblacion_primaria, Poblacion_secundaria
        FROM Poblacion_jardin AS pj
        INNER JOIN Poblacion_primaria AS pp
        ON pj.id_prov = pp.id_prov AND pj.id_depto = pp.id_depto
        INNER JOIN Poblacion_secundaria AS ps
        ON pj.id_prov = ps.id_prov AND pj.id_depto = ps.id_depto
        )
    
    SELECT Provincia, Departamento, Jardines, Poblacion_jardin, 
    Primarias, Poblacion_primaria, Secundario, Poblacion_secundaria
    FROM prov_depto AS pd
    INNER JOIN Poblaciones AS pob
    ON pd.id_prov = pob.id_prov AND pd.id_depto = pob.id_depto
    INNER JOIN Cantidad_ee AS ce
    ON pd.id_prov = ce.id_prov AND pd.id_depto = ce.id_depto
    ORDER BY Provincia ASC, Primarias DESC
    """).df()

#%% Consulta II

"""
Para cada departamento informar la provincia y la cantidad de CC con
capacidad mayor a 100 personas. El orden del reporte debe ser alfabético
por provincia y dentro de las provincias, descendente por cantidad de CC de
dicha capacidad.
"""

# Se cuentan para cada departamento cuantos CC hay con capacidad mayor a 100, luego se hace un 
# Inner join para obtener el nombre de la provincia y departamento cuando se cumple la condicion
Consulta2 = dd.sql(
    """
    WITH a AS (
    SELECT id_prov, id_depto, COUNT(*) AS Cantidad_mayor_100
    FROM Centros_culturales
    WHERE Capacidad > 100
    GROUP BY id_prov, id_depto)
    
    SELECT p.nombre AS Provincia, d.nombre AS Departamento, a.Cantidad_mayor_100
    FROM a 
    INNER JOIN Provincias AS p
    ON p.id_prov = a.id_prov
    INNER JOIN Departamentos AS d
    ON d.id_depto = a.id_depto AND d.id_prov = a.id_prov
    GROUP BY p.nombre, d.nombre, Cantidad_mayor_100
    ORDER BY p.nombre ASC, Cantidad_mayor_100 DESC
    """
    ).df()

#%% CONSULTA III

"""
Para cada departamento, indicar provincia, cantidad de CC, cantidad de EE
(de modalidad común) y población total. Ordenar por cantidad EE
descendente, cantidad CC descendente, nombre de provincia ascendente y
nombre de departamento ascendente. No omitir casos sin CC o EE.
"""

# Guardamos la cantidad de CC por cada departamento
Cantidad_cc = dd.sql(
        """
        SELECT id_prov, id_depto, COUNT(*) AS Cantidad_cc
        FROM Centros_culturales
        GROUP BY id_prov, id_depto
        """).df()
        
# Guardamos la cantidad de EE por cada depto, tomamos cada comuna de caba y caba completo 
# ya que dec entros culturales no hay info por comunas
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

# Guardamos la cantidad de Poblacion por cada depto, tomamos cada comuna de caba y caba completo 
Poblacion_total = dd.sql(
    """
    SELECT id_prov, 0 AS id_depto, SUM(Casos) AS Personas
    FROM Poblacion
    WHERE id_prov = 2
    GROUP BY id_prov
    UNION
    SELECT id_prov, id_depto, SUM(Casos) AS Personas
    FROM Poblacion
    GROUP BY id_prov, id_depto
    """
    ).df()

# Hacemos una tabla temporal prov_depto, con la que hacemos left join ya que es
# la mas completa de todas
Consulta3 = dd.sql(
    """
    WITH prov_depto AS (
    SELECT d.id_prov, d.id_depto, d.nombre AS Departamento, 
    p.nombre AS Provincia
    FROM Departamentos AS d
    INNER JOIN Provincias AS p
    ON d.id_prov = p.id_prov)
    
    SELECT pd.Provincia, pd.Departamento, Cantidad_ee, Cantidad_cc, Personas
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

"""
EL dominio de un mail es todo lo que hay despues del arroba @, ademas no se distingue entre
mayusculas y minusculas. Buscamos el @, separamos la cadena y nos quedamos con la parte del
dominio, calculamos la frecuencia en cada departamento, vemos cual es la maxima en cada departamento
y luego hacemos un inner join, se obtiene el dominio mas frecuente y en caso de empate se muestran todos.
Ademas se quitan los null
"""
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
    
    SELECT p.nombre AS Provincia, d.nombre AS Departamento,
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

#%% GRAFICO I

"""
Cantidad de CC por provincia. Mostrarlos ordenados de manera decreciente 
por dicha cantidad. 
"""

visualizacion1 = dd.sql(
    """
    SELECT 
        p.nombre AS provincia,
        COUNT(cc.id_cc) AS cantidad_cc,
        FROM Centros_culturales AS cc
        JOIN Provincias AS p ON cc.id_prov = p.id_prov
        GROUP BY provincia
        ORDER BY cantidad_cc DESC
    """
    ).df()

# Grafico de barras horizontal
plt.figure(figsize=(12, 6))
plt.barh(visualizacion1["provincia"], visualizacion1["cantidad_cc"], color="steelblue")
plt.xlabel("Cantidad de Centros Culturales")
plt.ylabel("Provincia")
plt.title("Cantidad de Centros Culturales por Provincia")
plt.grid(True, axis='x', linestyle='--', alpha=0.7)
plt.gca().invert_yaxis()  
plt.show()


# %% GRAFICO II

"""
Graficar la cantidad de EE de los departamentos en función de la población, 
separando por nivel educativo y su correspondiente grupo etario (identificándolos por colores). 
Se pueden basar en la primera consulta SQL para realizar este gráfico. 
"""

"""
Se hace un plot de la cantidad de EE en funcion de la poblacion, cada punto representa un
departamento, y el color el nivel educativo. se grafica ademas la recta que mejor aproxima
para ver un poco la tendencia
"""
# y = mx + b
def ajuste_lineal(x, y, color, label, ax):
    m, b = np.polyfit(x, y, deg=1)
    x_recta = np.linspace(min(x), max(x), 1000)
    y_pred = m * x_recta + b
    
    r2 = r2_score(y, m*x+b)
    print(f"r2 {label}: {r2:.2f}")
    
    ax.scatter(x, y, marker=".", color=color, label=f"{label}: $R^2 = $ {r2:.2f}", alpha=0.75)
    ax.plot(x_recta, y_pred, color=color, lw=4)
    

    

poblacion_jardin = Consulta1["Poblacion_jardin"]
cantidad_jardin = Consulta1["Jardines"]

poblacion_primaria = Consulta1["Poblacion_primaria"]
cantidad_primaria = Consulta1["Primarias"]

poblacion_secundaria = Consulta1["Poblacion_secundaria"]
cantidad_secundaria = Consulta1["Secundario"]

fig, axs = plt.subplots(1, 2, figsize=(16, 7), gridspec_kw={"wspace": 0.23})

axs[0].set_title("Cantidad de EE en función de la población", fontsize=16, fontweight="bold")
ajuste_lineal(poblacion_jardin, cantidad_jardin, "red", "Jardin", axs[0])
ajuste_lineal(poblacion_primaria, cantidad_primaria, "green", "Primario", axs[0])
ajuste_lineal(poblacion_secundaria, cantidad_secundaria, "blue", "Secundario", axs[0])
axs[0].set_xlabel("Población", fontsize=14)
axs[0].set_ylabel("Cantidad de EE", fontsize=16)
axs[0].legend(loc="lower right", fontsize=12)
axs[0].grid(True)

axs[1].set_title("Cantidad de EE en función de la población (rango acotado)", fontsize=16, fontweight="bold")
ajuste_lineal(poblacion_jardin, cantidad_jardin, "red", "Jardin", axs[1])
ajuste_lineal(poblacion_primaria, cantidad_primaria, "green", "Primario", axs[1])
ajuste_lineal(poblacion_secundaria, cantidad_secundaria, "blue", "Secundario", axs[1])
axs[1].set_xlabel("Población", fontsize=14)
axs[1].set_ylabel("Cantidad de EE", fontsize=16)
axs[1].legend(loc="lower right", fontsize=12)
axs[1].grid(True)
axs[1].set_xlim(0, 15000)
axs[1].set_ylim(0, 80)

plt.tight_layout()
plt.show()
 

#%% GRAFICO III

"""
para cada provincia, se esta calculando en base a sus departamentos la mediana 
de cantidad de EE.
"""
datos = dd.sql(
    """
    SELECT DISTINCT p.nombre, id_depto, COUNT(*) AS Cantidad
    FROM Establecimientos_educativos AS ee
    INNER JOIN Provincias AS p
    ON p.id_prov = ee.id_prov 
    GROUP BY p.nombre, id_depto
    """).df()

medianas = datos.groupby("nombre")["Cantidad"].median()
medianas_ordenadas = medianas.sort_values()
# saco los indices para ordenar con medianas
indice = medianas_ordenadas.index

plt.figure(figsize=(10, 6))
sns.boxplot(data=datos, x="nombre", y="Cantidad", order = indice, color = "lightblue")

plt.title("Cantidad de EE por Departamento en cada Provincia", fontsize=16)
plt.xlabel("Provincia", fontsize=12)
plt.ylabel("Cantidad de Establecimientos Educativos", fontsize=12)
plt.xticks(rotation=90) 
plt.grid(True)
plt.show()
#%% GRAFICO IV

# por provincia obtener la cant de cc y ee cada 1000 habitantes

# cant habitantes por prov
poblacion_por_provincia = dd.sql("""
                                 SELECT id_prov, SUM(Casos) AS cantidad
                                 FROM Poblacion
                                 GROUP BY id_prov
                                 """).df()
poblacion_por_provincia["cantidad"] = poblacion_por_provincia["cantidad"].astype(int)

# cant cc por prov
cc_por_provincia = dd.sql("""
                            SELECT id_prov, COUNT(*) AS cantidad
                            FROM Centros_culturales
                            GROUP BY id_prov
                          """).df()

# cant ee por prov
ee_por_provincia = dd.sql("""
                            SELECT id_prov, COUNT(*) AS cantidad
                            FROM Establecimientos_educativos
                            GROUP BY id_prov
                          """).df()

# armo tabla con nombre provincia y cantidad de ee y cc cada mil habitantes 
cantidad_eecc_cada_mil = dd.sql("""
     SELECT p.id_prov, p.nombre, ((ccp.cantidad*1000) / ppp.cantidad) AS cant_cc_cada_mil, 
     ((eep.cantidad*1000) / ppp.cantidad) AS cant_ee_cada_mil
     FROM Provincias p
     INNER JOIN cc_por_provincia ccp ON ccp.id_prov = p.id_prov
     INNER JOIN poblacion_por_provincia ppp ON ppp.id_prov = p.id_prov 
     INNER JOIN ee_por_provincia eep ON eep.id_prov = p.id_prov
     ORDER BY cant_ee_cada_mil DESC
     """).df()


fig, ax = plt.subplots(2, 1, figsize=(10, 12))

# Primer scatter (cantidad de EE y CC por cada mil habitantes)
ax[0].scatter(cantidad_eecc_cada_mil["nombre"], cantidad_eecc_cada_mil["cant_ee_cada_mil"], label="Establecimientos Educativos", color="royalblue", s=150, zorder=2)
ax[0].scatter(cantidad_eecc_cada_mil["nombre"], cantidad_eecc_cada_mil["cant_cc_cada_mil"], label="Centros Culturales", color="orange", s=150, zorder=2)

ax[0].set_ylabel("Cantidad cada 1000 habitantes") 
ax[0].set_xlabel("Provincia")
ax[0].set_xticklabels(cantidad_eecc_cada_mil["nombre"], rotation=60, ha="right")  
ax[0].set_title("Cantidad de EE y CC cada 1000 habitantes por provincia")
ax[0].legend()
ax[0].grid(zorder=1)

# Segundo scatter (relación EE/CC)
ax[1].scatter(cantidad_eecc_cada_mil["nombre"],  cantidad_eecc_cada_mil["cant_ee_cada_mil"]/cantidad_eecc_cada_mil["cant_cc_cada_mil"], label="Cant EE/Cant CC por provincia", color="royalblue", s=150, zorder=2)

ax[1].set_ylabel("Cant EE/Cant CC") 
ax[1].set_xlabel("Provincia")
ax[1].set_xticklabels(cantidad_eecc_cada_mil["nombre"], rotation=60, ha="right")  
ax[1].set_title("Relación de EE y CC cada 1000 habitantes por provincia")
ax[1].grid(zorder=1)

plt.tight_layout()
plt.show()

#%%  Tablas extra de ayuda


#%% ANEXO GQM

 # GQM DE LA TABLA POBLACION
import pandas as pd


df = pd.read_excel(_ruta_pp, usecols=[1,2], header=None, skiprows=13) 
df.columns = ["Edad", "Casos"]

# convertimos en numeros la primera columna, si no es un numero entonces queda null
df["Edad"] = pd.to_numeric(df["Edad"], errors='coerce')

# contamos las filas donde no hay numeros,que son las filas delimitadoras
filas_no_numericas = df[df["Edad"].isna()]

# calculamos el porcentaje
total_filas = len(df)
filas_no_numericas_count = len(filas_no_numericas)
porcentaje_no_numericas = (filas_no_numericas_count / total_filas) * 100

# imprimimos resultados
print(f"Total de filas en la tabla: {total_filas}")
print(f"Cantidad de filas donde la primera columna no es un número: {filas_no_numericas_count}")
print(f"Porcentaje de filas delimitadoras: {porcentaje_no_numericas:.2f}%")

#%%
# GQM DE LA TABLA CENTROS CULTURALES

# cantidad de tuplas en cc_info
total_registros = len(cc_info)

# nos quedamos con las filas sin null para contar
cc_mails = cc_info[cc_info["Mail"].notna()]

# contamos las celdas con múltiples direcciones de correo (es decir con mas de un arroba)
correos_multiples = cc_mails[cc_mails["Mail"].str.count("@") > 1]
cantidad_correos_multiples = len(correos_multiples)

# contamos correos inválidos, es decir sin arroba (incluye los que tienen valores ambiguos)
# o con espacios internos
correos_invalidos = cc_mails[
    (cc_mails["Mail"].str.contains(" ", na=False)) |  # Tiene espacios internos
    (cc_mails["Mail"].str.contains("@", na=False) == False)  # No contiene "@"
]

cantidad_correos_invalidos = len(correos_invalidos)

# Cálculo de porcentajes sobre el total de registros en la tabla original
porcentaje_multiples = (cantidad_correos_multiples / total_registros) * 100
porcentaje_invalidos = (cantidad_correos_invalidos / total_registros) * 100

# Mostrar resultados
print(f"Total de  tuplas en la tabla: {total_registros}")
print(f"Porcentaje de correos invalidos: {cantidad_correos_invalidos} ({porcentaje_invalidos:.2f}%)")
print(f"Porcentaje de correos multiples: {cantidad_correos_multiples} ({porcentaje_multiples:.2f}%)")

#%% GQM de la tabla establecimientos educativos


# seleccionamos las columnas en donde vamos a contar "no unos"
columnas_interes = ['Jardin_maternal', 'Jardin_infantes', 'Primario', 'Secundario', 'Secundario_tecnico']

# Contamos cuantas veces el valor no es '1'
total_no_unos = (ee_info[columnas_interes] != '1').sum().sum()

# calculamos el porcentaje de no unos sobre el total de datos (son 5 columnas)
porcentaje_no_unos = total_no_unos / (5 * len(ee_info))

# Imprimir resultado
print(f'Porcentaje de valores no unos: {porcentaje_no_unos:.2%}')



#%%