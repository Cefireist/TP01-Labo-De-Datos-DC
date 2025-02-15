# %% INTEGRANTES

# Nombre: Aaron Cuellar
# Mail: aaroncuellar2003@gmail.com
# LU: 810/23
    
# Nombre: Sebastian Ceffalotti
# Mail: sebastian.ceffalotti@gmail.com
# LU: 394/23

# Nombre: Rodrigo Copa
# Mail: roodrigo.coppa98@gmail.om
# LU: 471/22

# %% IMPORTO LAS LIBRERIAS
import numpy as np
import pandas as pd

# %% LEO LOS ARCHIVOS
carpeta = r"C:\Users\Usuario\Downloads\\"

data_CC = pd.read_csv(carpeta + "centros_culturales.csv")

data_EE = pd.read_excel(carpeta + "2025.01.06_padron_oficial_establecimientos_educativos_die.xlsx")

data_padron = pd.read_csv(carpeta + "padron_poblacion.xlsX - Output.csv")

# %%