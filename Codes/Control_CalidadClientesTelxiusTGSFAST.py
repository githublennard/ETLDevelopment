# coding=utf8
import boto3
import time
import sys
import json
import datetime
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import Row, SparkSession, Column
from pyspark import sql
import numpy as np
import config.database_config as database_config
import math
from database_utils import *
import traceback
import pandas as pd
from pyspark.sql.types import *
from util import read_SF, read_FAST, query_S3
import control
from pyspark.sql.types import BooleanType
from util import *
import sys
from datetime import *
from pyspark.sql.functions import regexp_replace, when, substring_index, col, trim,ltrim,rtrim
from pyspark.sql import functions as F

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

spark = SparkSession \
    .builder \
    .appName("DATAWALL: Control_CalidadClientesTelxiusTGSFAST") \
    .getOrCreate()

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Desarrollo del control:

class Control_CalidadClientesTelxiusTGSFAST(control.Control):
    """ 
        Descripción: Control Calidad clientes Telxius - TGS en FAST. DAT-777.

        Input DATA: 
            - Tabla "BUSINESS_CUST_ACC" de FAST
            - Tabla "TGS_TOP_PR_INST" de FAST

        Pasos:

        1. Creación del DataFrame con todos los clientes de la Tabla "BUSINESS_CUST_ACC" de FAST 
            · Este DataFrame general se filtra por clientes ACCT (Telxius).
            . Este DataFrame general se filtra por clientes NO ACCT (Telxius) incluyendo datos null del campo TAX_IDENT_NUMBER".
            · Se implementan filtros en cada una de estos Sub-Dataframes para no tener cuentas repetidas en el dataframe que se escribe en PostgreSQL.

        2. Creación del DataFrame con todos los servicios de la Tabla "TGS_TOP_PR_INST" de FAST
            · Este DataFrame general se modifica para trabajar con las columnas necesarias para hacer el match con los clientes.
            · Se implementan filtros y se agrupan para no tener cuentas repetidas.
            . El status de los productos debe ser 'Active'.

        3. Creación del DataFrame de Clientes con la marca de Telxius (ACCT) pero que tienen servicios que NO son de Telxius.
            · Se aplica un join de clientes Telxius con todos los servicios luego se filtra por servicios que NO son de Telxius.
            
        4. Creación del DataFrame de Clientes NO Telxius (ACCT) pero que SÍ tienen servicios de Telxius.
            . Se aplica un join de clientes NO Telxius con todos los servicios luego se filtra por servicios que SI son de Telxius.
        
        5. Creación del DataFrame de clientes compartidos que usan algun servicio de Telxius y NO Telxius.
            . Se hace un join de clientes en General con todos los servicios, este seria un dataframe auxiliar para hacer match.
            . Se aplica un filtro con Clientes que tienen algun servicio de Telxius en dfAuxiliar.  
            . Se aplica un filtro con Clientes que NO tienen algun servicio de Telxius en dfAuxiliar.
            . Se realiza un Match por OBJECT_ID entre dfCliSiTelx(Tabla con cliente asociado a un servicio Telxius) y dfCliNoTelx(Tabla con cliente No asociado a un servicio Telxius).
              Generando asi una lista de clientes que sirve para identificar al cliente que usa servicios Telxius y No Telxius.
            . La lista anterior nos permite realizar un Match con la Tabla General de Clientes que tienen algun servicio asociado a su cuenta,
              de esta Match se obtiene la lista de clientes compartidos.
    """
    
    def __init__(self, dfBusAcTel, dfBusAcNoTel, dfTgs_Top, dfBusAcAll):
        self.dfBusAcTel = dfBusAcTel 
        self.dfBusAcNoTel = dfBusAcNoTel
        self.dfTgs_Top = dfTgs_Top
        self.dfBusAcAll = dfBusAcAll
    
    def apply(self):
        dfBusAcTel = self.dfBusAcTel
        dfBusAcNoTel = self.dfBusAcNoTel
        dfTgs_Top = self.dfTgs_Top
        dfBusAcAll = self.dfBusAcAll     
        ###Clientes con la marca de Telxius (ACCT) pero que tienen servicios que NO son de Telxius.
        print("Haciendo un join de clientes Telxius con todos los servicios.")
        dfAux = dfBusAcTel.join(dfTgs_Top, dfBusAcTel.OBJECT_ID == dfTgs_Top.CUSTOMER_ACCOUNT_TOP, "inner")
        print("Total Usuarios con algun servicio: ",dfAux.count())
        print("Clientes con la marca de Telxius (ACCT) pero que tienen servicios que NO son de Telxius.")
        dfCliTelxSerNoTelx = dfAux.filter(~col('PRODUCT_CODE_TOP').isin(['CLOUD_DDOS'\
                                                                        , 'CPE_DDOS'\
                                                                        , 'ITS Resale'\
                                                                        , 'ITS'\
                                                                        , 'LOCAL_PLATFORM'\
                                                                        , 'LOCAL_PLATFORM_FC'\
                                                                        , 'MANAGED_DDOS'\
                                                                        , 'Local Platform Final Customer']))
        print("Numero de Filas: ",dfCliTelxSerNoTelx.count())
        print("Numero de Columnas: ",len(dfCliTelxSerNoTelx.columns))
        write_one_to_many_in_db(dfCliTelxSerNoTelx, '"DQA"."ControlClientesTelxiusServiciosNoTelxius"', ClTipControl=66)
        print('Written in BBDD ControlClientesTelxiusServiciosNoTelxius')
        ###Clientes NO Telxius (ACCT) pero que SÍ tienen servicios de Telxius.
        print("Haciendo un join de clientes NO Telxius con todos los servicios.")
        dfAux = dfBusAcNoTel.join(dfTgs_Top, dfBusAcNoTel.OBJECT_ID == dfTgs_Top.CUSTOMER_ACCOUNT_TOP, "inner")
        print("Total Usuarios: ",dfAux.count())
        print("Clientes NO Telxius (NO ACCT) pero que SÍ tienen servicios de Telxius.")
        dfCliNoTelxSerSiTelx = dfAux.filter(col('PRODUCT_CODE_TOP').isin(['CLOUD_DDOS'\
                                                                         , 'CPE_DDOS'\
                                                                         , 'ITS Resale'\
                                                                         , 'ITS'\
                                                                         , 'LOCAL_PLATFORM'\
                                                                         , 'LOCAL_PLATFORM_FC'\
                                                                         , 'MANAGED_DDOS'\
                                                                         , 'Local Platform Final Customer']))
        print("Numero de Filas: ",dfCliNoTelxSerSiTelx.count())
        print("Numero de Columnas: ",len(dfCliNoTelxSerSiTelx.columns))
        write_one_to_many_in_db(dfCliNoTelxSerSiTelx, '"DQA"."ControlClientesNoTelxiusServiciosSiTelxius"', ClTipControl=67)
        print('Written in BBDD ControlClientesNoTelxiusServiciosSiTelxius')
        ###dfBusAcAll 5 columnas #dfTgs_Top 4 columnas #Clientes Compartidos
        print("Haciendo un join de clientes en General con todos los servicios.")
        dfAux = dfBusAcAll.join(dfTgs_Top, dfBusAcAll.OBJECT_ID == dfTgs_Top.CUSTOMER_ACCOUNT_TOP, "inner")#Forma de asociar un cliente con un servicio
        dfCliSerTotal = dfAux
        print("Clientes que usan algun servicio: ",dfCliSerTotal.count())
        print("Numero de Filas: ",dfCliSerTotal.count())
        print("Numero de Columnas: ",len(dfCliSerTotal.columns))
        #Clientes que tienen algun servicio de Telxius
        print("Clientes que tienen algun servicio de Telxius")
        dfCliSiTelx = dfCliSerTotal.filter(col('PRODUCT_CODE_TOP').isin(['CLOUD_DDOS'\
                                                                        , 'CPE_DDOS'\
                                                                        , 'ITS Resale'\
                                                                        , 'ITS'\
                                                                        , 'LOCAL_PLATFORM'\
                                                                        , 'LOCAL_PLATFORM_FC'\
                                                                        , 'MANAGED_DDOS'\
                                                                        , 'Local Platform Final Customer']))
        print("Numero de Filas: ",dfCliSiTelx.count())
        print("Numero de Columnas: ",len(dfCliSiTelx.columns))
        #Clientes que NO tienen algun servicio de Telxius
        print("Clientes que NO tienen algun servicio de Telxius")
        dfCliNoTelx = dfCliSerTotal.filter(~col('PRODUCT_CODE_TOP').isin(['CLOUD_DDOS'\
                                                                         , 'CPE_DDOS'\
                                                                         , 'ITS Resale'\
                                                                         , 'ITS'\
                                                                         , 'LOCAL_PLATFORM'\
                                                                         , 'LOCAL_PLATFORM_FC'\
                                                                         , 'MANAGED_DDOS'\
                                                                         , 'Local Platform Final Customer']))
        print("Numero de Filas: ",dfCliNoTelx.count())
        print("Numero de Columnas: ",len(dfCliNoTelx.columns))
        #Match por OBJECT_ID entre dfCliSiTelx(Tabla con cliente asociado a un servicio Telxius) y dfCliNoTelx(Tabla con cliente No asociado a un servicio Telxius)
        dfMatch = dfCliSiTelx.join(dfCliNoTelx, dfCliSiTelx.OBJECT_ID == dfCliNoTelx.OBJECT_ID, "leftsemi")#con leftsemi me quedo con lo clientes de un solo dataframe 
        dfAuxID = dfMatch.groupBy('OBJECT_ID').count()####
        print("Lista Final del Match con OBJECT_ID")####
        dfMatchID = dfAuxID.select('OBJECT_ID')
        print("Numero de Clientes que son compartidos: ",dfMatchID.count())
        print("Numero de Filas: ",dfMatchID.count())
        print("Numero de Columnas: ",len(dfMatchID.columns))
        #Match de clientes que usan algun servicio servicios de Telxius y NO Telxius con la Tabla General de Clientes que tienen algun servicio asociado
        print("Tabla de clientes compartidos que usan algun servicio de Telxius y NO Telxius")
        dfMatchFinal = dfCliSerTotal.join(dfMatchID, dfCliSerTotal.OBJECT_ID == dfMatchID.OBJECT_ID, "leftsemi")### 
        print("Numero de Filas: ",dfMatchFinal.count())
        print("Numero de Columnas: ",len(dfMatchFinal.columns))
        write_one_to_many_in_db(dfMatchFinal, '"DQA"."ControlClientesCompartidosTelxius"', ClTipControl=68)
        print('Written in BBDD ControlClientesCompartidosTelxius')

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Ejecución:

def readTables():
    dfBusAc = read_FAST("BUSINESS_CUST_ACC") #Este dfBusAc Dataframe no tiene  ninguna edicion
    print("BUSINESS_CUST_ACC")
    print("Tabla BUSINESS_CUST_ACC sin Editar")
    print("Numero de Filas: ",dfBusAc.count())
    print("Numero de Columnas: ",len(dfBusAc.columns))
    #Tabla de Bussiness Customer editada en el campo NAME
    print("Tabla de Bussiness Customer editada en el campo NAME")
    dfBusAcAll = dfBusAc.select('NAME','OBJECT_ID','CUST_CATEGORY','TAX_IDENT_NUMBER','CUSTOMER_ACCNUM').sort('NAME')
    dfBusAcAll = dfBusAcAll.filter(dfBusAcAll.NAME.isNotNull())
    dfBusAcAll = dfBusAcAll.filter(~col('NAME').isin([' -','-','--'])) #Funciona perfecto
    print("Resumen Tabla de Datos")
    dfBusAcAll = dfBusAcAll.withColumn('NAME', ltrim(col('NAME')))#dfBusAcAll -->> Tabla de Bussiness Customer editada en tamaño de columnas y el campo NAME
    print("Numero de Filas: ",dfBusAcAll.count())
    print("Numero de Columnas: ",len(dfBusAcAll.columns))
    ###"Tabla de Datos con clientes ACCT (Telxius)"
    print("Tabla de Datos con clientes ACCT (Telxius)")
    print("Cantidad de usuarios con ACCT (Telxius): ", dfBusAc.filter(dfBusAc.TAX_IDENT_NUMBER.startswith("ACCT"))\
        .select('NAME','OBJECT_ID','CUST_CATEGORY','TAX_IDENT_NUMBER','CUSTOMER_ACCNUM').count())
    dfBusAcTel = dfBusAc.filter(dfBusAc.TAX_IDENT_NUMBER.startswith("ACCT"))\
                    .select('NAME','OBJECT_ID','CUST_CATEGORY','TAX_IDENT_NUMBER','CUSTOMER_ACCNUM').sort('NAME')#Dataframe con clientes de ACCT (Telxius)
    ###Tabla de Datos con clientes NO Telxius y con datos null Incluidos en TAX_IDENT_NUMBER
    dfBusAcNoTel = dfBusAc.filter(dfBusAc.TAX_IDENT_NUMBER.isNull() | ~dfBusAc.TAX_IDENT_NUMBER.startswith("ACCT"))\
                .select('NAME','OBJECT_ID','CUST_CATEGORY','TAX_IDENT_NUMBER','CUSTOMER_ACCNUM').sort('NAME')
    ###Tabla de Datos con clientes NO Telxius y con datos null Incluidos en TAX_IDENT_NUMBER pero NO Null en Name
    dfBusAcNoTelName = dfBusAcNoTel.filter(dfBusAcNoTel.NAME.isNotNull())
    ###Tabla de Datos con clientes NO Telxius y con datos null Incluidos en TAX_IDENT_NUMBER pero NO Null en Name, NO ' -' '-' '--' 
    dfBusAcNoTelName = dfBusAcNoTelName.filter(~col('NAME').isin([' -','-','--'])) #Funciona perfecto
    #Tabla de Datos con clientes NO Telxius y con datos null Incluidos en TAX_IDENT_NUMBER pero NO Null en Name, NO ' -' '-' '--' ,ltrim 
    print("Tabla de Datos con clientes NO Telxius y con datos null Incluidos en TAX_IDENT_NUMBER pero NO Null en Name, NO: ' -' '-' '--' ,ltrim ")
    dfBusAcNoTelName = dfBusAcNoTelName.withColumn('NAME', ltrim(col('NAME')))
    print("Numero de Filas: ",dfBusAcNoTelName.count())
    print("Numero de Columnas: ",len(dfBusAcNoTelName.columns))
    dfBusAcNoTel = dfBusAcNoTelName
    #Tabla Serivicios Telxius y No Telxius
    print("TGS_TOP_PR_INST")
    dfTgs_Top = read_FAST("TGS_TOP_PR_INST")
    print("Resumen Tabla de Datos sin Editar")
    print("Numero de Filas: ",dfTgs_Top.count())
    print("Numero de Columnas: ",len(dfTgs_Top.columns))
    #print("DF con Edicion en campo NAME con Instance")
    dfEditName = dfTgs_Top.withColumn('NAME', F.substring_index(F.col('NAME'), 'Instance', 1))#Sirve#Tiene todas las cabeceras
    #print("DF con Edicion en campo NAME con #")
    dfEditName = dfEditName.withColumn('NAME', F.substring_index(F.col('NAME'), '#', 1))#Sirve#Tiene todas las cabeceras
    dfTgs_Top = dfEditName#Se devuelve un DF limpio a la variable original
    print("Resumen DF de TGS_TOP_PR_INST con campo NAME Limpio")
    print("Numero de Filas: ",dfTgs_Top.count())
    print("Numero de Columnas: ",len(dfTgs_Top.columns))
    dfTgs_Top = dfTgs_Top.select('NAME','OBJECT_ID','TYPE_ID','CUSTOMER_ACCOUNT','EXTERNAL_ID','INSTANCE_ID','PRODUCT_CODE','END_CUSTOMER_NAME','STATUS')\
                        .withColumnRenamed('NAME','NAME_TOP')\
                        .withColumnRenamed('OBJECT_ID','OBJECT_ID_TOP')\
                        .withColumnRenamed('TYPE_ID','TYPE_ID_TOP')\
                        .withColumnRenamed('CUSTOMER_ACCOUNT','CUSTOMER_ACCOUNT_TOP',)\
                        .withColumnRenamed('EXTERNAL_ID','EXTERNAL_ID_TOP',)\
                        .withColumnRenamed('INSTANCE_ID','INSTANCE_ID_TOP',)\
                        .withColumnRenamed('PRODUCT_CODE','PRODUCT_CODE_TOP',).sort('NAME_TOP')        
    dfTgs_Top = dfTgs_Top.filter(col('STATUS').isin(['Active']))#Este filtro es una condicion importante para cliente compartidos
    dfTgs_Top = dfTgs_Top.select('NAME_TOP'\
                                ,'OBJECT_ID_TOP'\
                                ,'TYPE_ID_TOP'\
                                ,'CUSTOMER_ACCOUNT_TOP'\
                                ,'EXTERNAL_ID_TOP'\
                                ,'INSTANCE_ID_TOP'\
                                ,'PRODUCT_CODE_TOP')#Estos son los campos que se escriben en PostgreSQL
    print("DF con agrupacion de 'NAME_TOP','CUSTOMER_ACCOUNT_TOP','PRODUCT_CODE_TOP'")
    dfGroup = dfTgs_Top.groupBy('NAME_TOP','CUSTOMER_ACCOUNT_TOP','PRODUCT_CODE_TOP').count()
    print("Numero Filas y Columnas despues del groupBy")
    print("Numero de Filas: ",dfGroup.count())
    print("Numero de Columnas: ",len(dfGroup.columns))
    dfTgs_Top = dfGroup#Se guarda un Dataframe con las columnas que necesito en la variable inicial de TOP
    dfTgs_Top = dfTgs_Top.withColumnRenamed('count','NumVecesServAsocAlCustomer_TOP')
    print("Clientes que tienen algun servicio asociado: ",dfTgs_Top.count())
    print("Numero de Filas: ",dfTgs_Top.count())
    print("Numero de Columnas: ",len(dfTgs_Top.columns))
        
    ControlClientesTelxius = Control_CalidadClientesTelxiusTGSFAST(dfBusAcTel, dfBusAcNoTel, dfTgs_Top, dfBusAcAll)
    ControlClientesTelxius.apply()
    
if __name__ == "__main__":
    readTables()
    print("Control Done")