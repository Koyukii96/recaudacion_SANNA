import os
import zipfile
from pathlib import Path
from glob import glob
import shutil
import chardet



def get_charset(file):
    with open(file, 'rb') as f:
        rawdata = f.read()
    cs= chardet.detect(rawdata)['encoding']
   
   
    return cs


def borra_archivos(ruta, tipo):

    directory = ruta
    test = os.listdir(directory)

    for item in test:
        if item.endswith(tipo):
            os.remove(os.path.join(directory, item))

def borra_pantalla():
    # for windows
    if os.name == 'nt':
        _ = os.system('cls')

    # for mac and linux(here, os.name is 'posix')
    else:
        _ = os.system('clear')

def comprimir(destino, nombre_zip, lista_archivos):

    print(destino)
    print(nombre_zip)
    print(lista_archivos)

    with zipfile.ZipFile(destino+"\\"+nombre_zip, 'w', zipfile.ZIP_DEFLATED) as myzip:
        for archivo in lista_archivos:
            myzip.write(archivo)
    myzip.close()