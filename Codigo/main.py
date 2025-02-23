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

Notas adicionales:
ejecutar el siguiente codigo en la terminal para verificar que tenga instalada las librerias usadas en ese script:
pip install duckdb numpy pandas matplotlib seaborn scikit-learn
"""

# %% IMPORTACION DE LIBRERIAS

import duckdb as dd  
import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import r2_score
import os

#%% OBTENCION DE LAS RUTAS DE LOS ARCHIVOS A USAR

# obtenemos la ruta del directorio donde se esta ejecutando este codigo
_ruta_actual = os.path.dirname(__file__)

# ruta a la carpeta TablasOriginales que es donde estan las tablas a usar
_ruta_tablas_originales = os.path.join(_ruta_actual, 'TablasOriginales')
# Ruta a la carpeta TablasModelo
_ruta_tablas_modelo = os.path.join(_ruta_actual, 'TablasModelo')

# rutas de los 3 archivos, usamos esta libreria para que el codigo encuentre los archivos siempre
_ruta_ee = os.path.join(_ruta_tablas_originales, 'padron_establecimientos_educativos_2022.xlsx')
_ruta_cc = os.path.join(_ruta_tablas_originales, 'centros_culturales_2022.csv')
_ruta_pp = os.path.join(_ruta_tablas_originales, 'padron_poblacion_2022.xlsX')


# Diccionario con las rutas de los archivos CSV
_rutas = {
    "Centros_culturales": os.path.join(_ruta_tablas_modelo, 'Centros_culturales.csv'),
    "Poblacion": os.path.join(_ruta_tablas_modelo, 'Poblacion.csv'),
    "Establecimientos_educativos": os.path.join(_ruta_tablas_modelo, 'Establecimientos_educativos.csv'),
    "Modalidades": os.path.join(_ruta_tablas_modelo, 'Modalidades.csv'), 
    "Departamentos": os.path.join(_ruta_tablas_modelo, 'Departamentos.csv'),
    "Provincias": os.path.join(_ruta_tablas_modelo, 'Provincias.csv'),
    "ee_modalidades": os.path.join(_ruta_tablas_modelo, 'ee_modalidades.csv')
}

# guardo en una variable el csv con el nombre, para eso uso globals que genera una variable dinamicamente 
for nombre, ruta in _rutas.items():
    globals()[nombre] = pd.read_csv(ruta)

# %% LECTURA DE LAS 3 FUENTES DE DATOS, ESTABLECIMIENTOS EDUCATIVOS, CENTROS CULTURALES Y PADRON DE PERSONAS

establecimientos_educativos_original = pd.read_excel(_ruta_ee, header=0, skiprows=6)
_centros_culturales_original = pd.read_csv(_ruta_cc)
_padron_personas = pd.read_excel(_ruta_pp, usecols=[1,2], header=None, skiprows=13)
_padron_personas.columns = ["Edad", "Casos"]


""" Le agrego una clave primaria a centros_culturales, no encontre combinacion 
de atributos suficientemente pequena como para que sea la clave primaria asi que le 
agregue un indice
"""
Centros_culturales = dd.sql("""
    SELECT ROW_NUMBER() OVER () AS id_cc, *
    FROM _centros_culturales_original;
""").df()



#%% LIMPIEZA TABLA PADRON DE PERSONAS

# se agregan columnas codigo_area y nombre_depto (pasando a mayusculas) para identificar 
# cada departamento, y se borran valores que no sirven
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

# relleno hacia abajo con el ultimo valor no nulo
_padron_personas[["codigo_area", "nombre_departamento","mascara_para_borrar"]] = _padron_personas[["codigo_area", "nombre_departamento", "mascara_para_borrar"]].ffill()

#%%

# GUARDAMOS LA TABLA PERSONAS CON LOS ATRIBUTOS: edad, número de casos, id_prov, id_depto
# eliminacion de filas usadas para el forward fill, y separo el codigo de area en id_prov 
# e id_depto el CAST sirve para cambiar el tipo de variable, pasamos de string a integer
# cambio el id de Ushuaia y RIO GRANDE porque estan distintos a la otra tabla
Poblacion = dd.sql(
    """
    WITH pp AS (
    SELECT nombre_departamento, CAST(Edad AS INTEGER) AS Edad, 
    CAST(Casos AS INTEGER) AS Casos,
    CAST(SUBSTRING(codigo_area, 1, 2) AS INTEGER) AS id_prov,
    CAST(SUBSTRING(codigo_area, 3, 3) AS INTEGER) AS id_depto
    FROM _padron_personas
    WHERE Edad NOT LIKE 'AREA%' AND mascara_para_borrar IS NULL)
    SELECT id_prov, id_depto, Edad, Casos 
    FROM pp
    """
    ).df()


#%% Tabla ee_info guarda mucha info relacionada a establecimientos_educativos y luego voy separando

ee_info = dd.sql(
    """
    WITH ee_2 AS (
        SELECT CAST("Código de localidad" AS VARCHAR) AS cod_loc, 
        UPPER(Jurisdicción) AS nombre, UPPER(Departamento) AS nombre_departamento, *
        FROM _establecimientos_educativos_original)
    
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

#%% Tabla departamentos     
       
Departamentos = dd.sql(
    """ 
    SELECT DISTINCT id_prov,  
    CASE 
        WHEN id_prov = 94 AND id_depto = 7 THEN 8
        WHEN id_prov = 94 AND id_depto = 14 THEN 15
        ELSE id_depto
        END AS id_depto, 
        nombre_departamento AS nombre
    FROM ee_info
    UNION 
    SELECT DISTINCT ID_PROV, CAST(SUBSTRING(CAST(ID_DEPTO AS VARCHAR), 2, 4) AS INTEGER) AS id_depto, Departamento
    FROM Centros_culturales
    WHERE ID_PROV = 2
    """
    ).df()
#%%

# tabla Modalidades
datos_modalidades = {
    "id_mod": [0,1,2,3,4],
    "modalidad": ["Jardin_maternal", "Jardin_infantes", "Primario", "Secundario", "Secundario_tecnico"]
}
# tipo_establecimiento -> modalidad  ; id_tipo_establecimiento_educativo -> Modalidades


Modalidades = pd.DataFrame(datos_modalidades)

# Separo para cada cueanexo si tiene uno o varios tipos de establecimiento, asi no hay tantos valores NULL
# La clave primaria de Establecimientos_educativos es Cueanexo junto a id_tipo_establecimiento
Establecimientos_educativos = dd.sql(
    """
    SELECT Cueanexo, id_prov, id_depto, Ámbito, Sector
    FROM ee_info
    """
).df()


#%% tabla ee_modalidades

ee_modalidades = dd.sql(
    """
    SELECT Cueanexo, 0 AS id_modalidad
    FROM ee_info
    WHERE Jardin_maternal = '1'

    UNION 
    
    SELECT Cueanexo, 1 AS id_modalidad
    FROM ee_info
    WHERE Jardin_infantes = '1'

    UNION
    
    SELECT Cueanexo, 2 AS id_modalidad
    FROM ee_info
    WHERE Primario = '1'

    UNION
    
    SELECT Cueanexo, 3 AS id_modalidad
    FROM ee_info
    WHERE Secundario = '1'

    UNION
    
    SELECT Cueanexo, 4 AS id_modalidad
    FROM ee_info
    WHERE Secundario_tecnico = '1';
    """
).df()

#%% Tabla provincias
Provincias = dd.sql(
    """
    SELECT DISTINCT id_prov, nombre
    FROM ee_info
    """
    ).df()

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
    
#%% GUARDADO DE TABLAS DINAMICAMENTE EN TABLASMODELO
_carpeta_destino = os.path.join(os.path.dirname(os.path.abspath(__file__)), "TablasModelo")
os.makedirs(_carpeta_destino, exist_ok = True)  # Crea la carpeta si no existe

_tablas = {
    "Poblacion": Poblacion,
    "Centros_culturales": Centros_culturales,
    "Establecimientos_educativos": Establecimientos_educativos,
    "Modalidades": Modalidades,
    "Departamentos": Departamentos,
    "Provincias": Provincias,
    "ee_modalidades": ee_modalidades
}

for nombre_tabla, df in _tablas.items():
    _ruta_del_csv = os.path.join(_carpeta_destino, f"{nombre_tabla}.csv")
    df.to_csv(_ruta_del_csv, index = False)
    
    
#%% ANALISIS DE DATOS 

# CONSULTA I 

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

"""
Para cada departamento informar la provincia, cantidad de EE de cada nivel
educativo, considerando solamente la modalidad común, y cantidad de
habitantes por edad según los niveles educativos. El orden del reporte debe
ser alfabético por provincia y dentro de las provincias, descendente por
cantidad de escuelas primarias.
"""

"""
Para cada departamento informar la provincia, cantidad de EE de cada nivel
educativo, considerando solamente la modalidad común, y cantidad de
habitantes por edad según los niveles educativos. El orden del reporte debe
ser alfabético por provincia y dentro de las provincias, descendente por
cantidad de escuelas primarias.
"""
primario6 = (34,90,10,70,74,14,18,30,42,6,26,94)
primario7 = (62,58,78,50,82,46,86,22,54,66,38,2)

Poblacion_jardin = dd.sql(
            """
            SELECT id_prov, id_depto, SUM(Casos) AS Poblacion_jardin
            FROM Poblacion
            WHERE Edad <= 5
            GROUP BY id_prov, id_depto
            """).df()
            
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
            
Poblacion_secundaria = dd.sql(
            f"""
            WITH secundario6 AS(
            SELECT id_prov, id_depto, SUM(Casos) AS Poblacion_secundaria
            FROM Poblacion
            WHERE Edad > 12 AND Edad <= 18 AND id_prov IN {primario6}
            GROUP BY id_prov, id_depto),
            
            secundario7 AS (SELECT id_prov, id_depto, SUM(Casos) AS Poblacion_secundaria
            FROM Poblacion
            WHERE Edad > 13 AND Edad <= 18 AND id_prov IN {primario7}
            GROUP BY id_prov, id_depto)
            
            SELECT *
            FROM secundario6
            UNION
            SELECT *
            FROM secundario7
            """
            ).df()

Cantidad_ee = dd.sql(
    """
    WITH Conteo AS (
    SELECT ee.id_prov, ee.id_depto, em.id_modalidad, COUNT(*) AS Cantidad
    FROM Establecimientos_educativos AS ee
    INNER JOIN ee_modalidades AS em ON ee.Cueanexo = em.Cueanexo
    GROUP BY id_prov, id_depto, id_modalidad
    ORDER BY id_prov, id_depto, id_modalidad)
    
    SELECT id_prov, id_depto,
    SUM(CASE WHEN id_modalidad IN (0, 1) THEN Cantidad ELSE 0 END) AS Jardines,
    SUM(CASE WHEN id_modalidad = 2 THEN Cantidad ELSE 0 END) AS Primarias,
    SUM(CASE WHEN id_modalidad IN (3, 4) THEN Cantidad ELSE 0 END) AS Secundario
    FROM Conteo
    GROUP BY id_prov, id_depto
    """).df()

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
    
    """).df()

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
    SELECT p.nombre, d.nombre, a.Cantidad_mayor_100
    FROM a 
    INNER JOIN Provincias AS p
    ON p.id_prov = a.id_prov
    INNER JOIN Departamentos AS d
    ON d.id_depto = a.id_depto
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

# y = mx + b
def ajuste_lineal(x, y, color, label, ax):
    m, b = np.polyfit(x, y, deg=1)
    x_recta = np.linspace(min(x), max(x), 1000)
    y_pred = m * x_recta + b
    
    ax.scatter(x, y, marker=".", color=color, label=label, alpha=0.75)
    ax.plot(x_recta, y_pred, color=color, lw=4)
    
    r2 = r2_score(y, m*x+b)
    
    print(f"r2 {label}: " + str(r2))
    

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

indice = medianas_ordenadas.index

plt.figure(figsize=(10, 6))
sns.boxplot(data=datos, x="nombre", y="Cantidad", order = indice, color = "lightblue")

plt.title("Cantidad de EE por Departamento en cada Provincia", fontsize=16)
plt.xlabel("Provincia", fontsize=12)
plt.ylabel("Cantidad de Establecimientos Educativos", fontsize=12)
plt.xticks(rotation=90)  # Rotar etiquetas del eje x para mejor legibilidad
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
                                 SELECT p.id_prov, p.nombre, ((ccp.cantidad*1000) / ppp.cantidad) AS cant_cc_cada_mil, ((eep.cantidad*1000) / ppp.cantidad) AS cant_ee_cada_mil
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


#%%
 # GQM DE LA TABLA POBLACION
import pandas as pd

# Cargar el archivo (ajustar la ruta según corresponda)

df = pd.read_excel(_ruta_pp, usecols=[1,2], header=None, skiprows=13) # Ajustar 'skiprows' si hay encabezados
df.columns = ["Edad", "Casos"]

# Intentar convertir la primera columna a números, los valores no numéricos se convierten en NaN
df["Edad"] = pd.to_numeric(df["Edad"], errors='coerce')

# Contar filas donde la conversión falló (valores que no son números enteros)
filas_no_numericas = df[df["Edad"].isna()]

# Calcular el porcentaje de estas filas en el dataset
total_filas = len(df)
filas_no_numericas_count = len(filas_no_numericas)
porcentaje_no_numericas = (filas_no_numericas_count / total_filas) * 100

# Mostrar resultados
print(f"Total de filas en el dataset: {total_filas}")
print(f"Cantidad de filas donde la primera columna no es un número: {filas_no_numericas_count}")
print(f"Porcentaje de filas afectadas: {porcentaje_no_numericas:.2f}%")

#%%
# GQM DE LA TABLA CENTROS CULTURALES



# Total de registros en la tabla original
total_registros = len(cc_info)

# Filtrar solo registros con mail no nulo para las métricas
cc_mails = cc_info[cc_info["Mail"].notna()]

# Contar registros con múltiples direcciones de correo (más de un "@")
correos_multiples = cc_mails[cc_mails["Mail"].str.count("@") > 1]
cantidad_correos_multiples = len(correos_multiples)

# Contar correos inválidos (sin "@" o con espacios internos o valores ambiguos)
correos_invalidos = cc_mails[
    (~cc_mails["Mail"].str.contains("@", na=False)) |  # No contiene "@"
    (cc_mails["Mail"].str.strip().str.contains(r"\s", regex=True)) |  # Espacios internos
    (cc_mails["Mail"].str.strip().isin(["s/d", "-"]))  # Valores ambiguos
]
cantidad_correos_invalidos = len(correos_invalidos)

# Cálculo de porcentajes sobre el total de registros en la tabla original
porcentaje_multiples = (cantidad_correos_multiples / total_registros) * 100
porcentaje_invalidos = (cantidad_correos_invalidos / total_registros) * 100

# Mostrar resultados
print(f"Total de registros en la tabla: {total_registros}")
print(f"Correos inválidos (sin '@', con espacios internos, o valores ambiguos): {cantidad_correos_invalidos} ({porcentaje_invalidos:.2f}%)")
print(f"Correos con múltiples direcciones en una celda: {cantidad_correos_multiples} ({porcentaje_multiples:.2f}%)")

#%% GQM de la tabla establecimientos educativos



# Contar los valores nulos en las columnas que has decidido separar
columnas_interes = ['Jardin_maternal', 'Jardin_infantes', 'Primario', 'Secundario', 'Secundario_tecnico']

# Contar cuántas veces el valor NO es "1"
total_no_unos = (ee_info[columnas_interes] != '1').sum().sum()

# Calcular el porcentaje de "no unos" sobre el total de datos esperados
porcentaje_no_unos = total_no_unos / (5 * len(ee_info))

# Imprimir resultado
print(f'Porcentaje de valores distintos de 1: {porcentaje_no_unos:.2%}')



#%%