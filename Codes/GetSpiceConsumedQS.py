# -*- coding: utf-8 -*-
import boto3
import pandas as pd
import os
import botocore
import datetime
import io
from config.database_config import RUTA_LIST_DATASETS_QS

ACCOUNT_ID = '434217907064'

"""
    Script que crea un fichero csv en S3 con todos los datasets que hay creados en Quicksight, indicando su nombre, id, y lo que ocupa de Spice. Para ello, hace lo siguiente:
        1. Se conecta a Quicksight y extrae los siguientes datos de los datasets: Name, Id, ConsumedSpiceCapacityInBytes, ImportMode.
        2. Pasa los datos a un Dataframe y le añade la columna de Fecha.
        3. Lee el fichero ListDatasetsQuickSight.csv el cual contiene datos antiguos y extrae su contenido en un dataframe.
        4. Une los dos dataframes, para tener los datos antiguos y los actuales.
        5. Sobrescribe el fichero ListDatasetsQuickSight.csv con los datos nuevos.
        6. Este fichero luego se tiene que subir a QuickSight para que pueda ser visualizado en los dashboards.
        7. Antes de subir el fichero a QuickSight se debe revisar el .CSV si los ultimos datos que extraen conserva el mismo tipo de datos, columnas , estructura, con
           los datos antiguos. No deberia haber diferencias, pero en algunas filas (Las de la ultima ejecución) a veces se ven datos que no corresponde con la estructura
           que se tiene en el .CSV ó caracteres especiales como: " ó '; en algunas filas que generan un error en la creacion del dataset en QuickSight.
        8. Luego de revisar el .CSV, este se utiliza para crear un nuevo dataset y reemplazar por el que ya existe en QuickSight. En QuickSight revisar que el campo
           Fecha del nuevo dataset tenga el mismo tipo de dato con el que se viene usando al momento de reemplazar el dataset.  
        9. Este fichero que ha sido revisado y corregido dependiendo del caso, debe ser subido al bucket de S3. Para que en una nueva ejecucion se tengan los datos antiguos
           con una estructura correcta en sus datos. En una nueva ejecucion se tienen que repetir todos estos pasos.  
        10. Este código se puede ejecutar en PRO y PRE, pero se debe tener el usuario de AWS-CLI configurado en el servidor con su respectiva ACCESS KEY ID y SECRET KEY.
        11. La ejecucion en PRE y PRO dan el mismo resultado, la diferencia es que el .CSV se lee y se almacena en rutas diferentes.   
"""

def get_results_token(client, datos, datasets_list):
    dataset_data = {
        "Name" : "",
        "Id" : "",
        "ImportMode": "",
        "Size" : ""
    }

    for dataset in datasets_list:
        try:
            dataset_data["Name"] = dataset["Name"]
            dataset_data["ImportMode"] = dataset["ImportMode"]
            dataset_data["Id"] = dataset["DataSetId"]
            dataset_info = client.describe_data_set(AwsAccountId = ACCOUNT_ID, DataSetId = str(dataset_data["Id"]))
            dataset_data["Size"] = dataset_info["DataSet"]["ConsumedSpiceCapacityInBytes"]
            datos.append(dataset_data.copy())
        except botocore.exceptions.ClientError:
            print("Dataset No tomado en cuenta: ")
            print(dataset_data["Name"] + " - " + dataset_data["Id"])

def upload_data(df):
    # Si se utiliza la version 1.3.o o superior de pandas, hay que modificar los parametros de error_bad_lines y warn_bad_lines por on_bad_lines='warn'
    print("Leyendo Datos antiguos: ")
    df_previous = pd.read_csv(RUTA_LIST_DATASETS_QS, sep=',', error_bad_lines=False, warn_bad_lines=True)#;
    print(df_previous)
    print("Concatenando datos antiguos y el actual")
    df = pd.concat([df,df_previous])
    print("Subiendo datos concatenados al Bucket de S3")
    print(df)
    df.to_csv(RUTA_LIST_DATASETS_QS, index=False, sep=',')#;

def get_datasets():
    
    datos = []
    client = boto3.client('quicksight')

    datasets_list = client.list_data_sets(AwsAccountId = ACCOUNT_ID)
    get_results_token(client, datos, datasets_list["DataSetSummaries"])
    while "NextToken" in datasets_list:
        datasets_list = client.list_data_sets(AwsAccountId = ACCOUNT_ID, NextToken =  datasets_list["NextToken"])
        get_results_token(client, datos, datasets_list["DataSetSummaries"])
    
    df = pd.DataFrame(datos)
    
    now = datetime.datetime.now()
    df['Fecha']= str(now.day) + '/' + str(now.month) + '/' + str(now.year)
    
    upload_data(df)

def ejecutar():
    print("COMIENZA LA CAPTURA DE DATASETS EN AWS")
    get_datasets()
    print("FIN")

if __name__ == "__main__":
    ejecutar()