# coding=utf8
import boto3
import time
import sys
import json
import datetime
from pyspark.sql import Row, SparkSession, Column
from pyspark import sql, SparkContext, SQLContext, SparkConf
import numpy as np
import pyspark.sql.functions as F
import config.database_config as database_config
import math
from database_utils import *
import traceback
import pandas as pd
from pyspark.sql.types import *
import control
from util import *
from datetime import *
from pyspark.sql.functions import col, when, sum, avg, min, mean, count, hour,regexp_replace, max as max_
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

spark = SparkSession \
    .builder \
    .appName("DATAWALL: Control_ConsistenciaCuentasCoincidentesFastSF") \
    .getOrCreate()

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Funciones Auxiliares:

def match_df_inner(df1,df2,col1,col2):
    print("Haciendo Match entre dataframes con inner")
    df1 = df1.join(df2, df1[col1] == df2[col2], "inner")
    result = df1
    return result

def match_df_leftouter(df1,df2,col1,col2):
    print("Haciendo Match entre dataframes con leftouter")
    df1 = df1.join(df2, df1[col1] == df2[col2], "leftouter")
    result = df1
    return result

def match_df_leftsemi(df1,df2,col1,col2):
    print("Haciendo Match entre Dataframes con leftsemi")
    df1 = df1.join(df2, df1[col1] == df2[col2], "leftsemi")
    result = df1
    return result

def match_df_leftanti (df1,df2,col1,col2):
    print("Haciendo Match entre Dataframes con leftanti")
    df1 = df1.join(df2, (df1[col1] == df2[col2]), "leftanti")
    result = df1
    return result

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Desarrollo del control:

class Control_ConsistenciaCuentasCoincidentesFastSF(control.Control):
    """ 
        Descripción: Control detectivo de consistencia de Cuentas entre Salesforce y FAST. DATAQA-718. DATAQA-805. DATAQA-833.

        Input DATA: 
            - Tabla "Account" de SalesForce
            - Tabla "BUSINESS_CUST_ACC" de FAST
            - Tabla "TGS_TOP_PR_INST" de FAST
            - Tabla "TGS_TOP_ORD" de FAST
            - Tabla "ControlCuentasHolding" de DQA

        Pasos:
        
        De forma general se estable las siguientes condiciones para los sistemas de FAST y Salesforce:
            . En FAST sólo se comprueban las cuentas que tienen estado Active o Pending Activation.
            . En SF sólo se comprueban las cuentas que tienen estado Activo.
            . En la creacion de algun DataFrame se puede aplicar una condicion adicional. Por eso es importante leer esta descripcion.
            . En las clientes de SF se excluye aquellos que tienen la jeraquia de 'Business Unit','Cost Center'.
            . A todos los clientes de FAST en cada uno de los dataframe se les agrega columnas que hacen referencia a la cantidad de PRODUCTOS/SERVICIOS asociados Y ORDENES asociadas.
            . Las columnas que hacen referencia a la cantidad de PRODUCTOS/SERVICIOS asociados tienen el estatus de Active o Planned.
            . Las columnas que hacen referencia a la cantidad de ORDENES asociadas tiene todos los tipos de estatus de la orden.
            . Las cuentas de clientes de FAST incluye el campo Holding que indica cual es la cuenta Holding de cada una de las cuentas que existen en sus diferentes jerarquias.

        1. Creación del DataFrame con cuentas de Salesforce que coinciden con cuentas de FAST y viceversa.
            · Unión entre la tabla "Account" de SF y la tabla "BUSINESS_CUST_ACC" de FAST.
            · Se implementan filtros en cada una de las tablas para no tener cuentas repetidas en el dataframe que se escribe en PostgreSQL.
            . Para este dataframe de coincidencias existen DOS REGLAS, es decir existen dos campos en cada tabla para hacer match y ver si 
              la regla se cumple en ambos sentidos de comparacion.
            . Cada regla es un dataframe que se escribe en PostgreSQL.
            . El valor del campo de Jerarquias en SF se modifica para poder comparar con el valor que se tiene en FAST.
            . Los clientes de FAST tienes las columnas respectivas que hacen referencia a la cantidad de PRODUCTOS/SERVICIOS asociados Y ORDENES asociadas.
            
        2. Creación del DataFrame con cuentas de SF que NO coinciden con FAST
            · Se aplica un leftanti entre la tabla "Account" de SF y la tabla "BUSINESS_CUST_ACC" de FAST.
            · Se implementan filtros para no tener cuentas repetidas en el dataframe que se escribe en PostgreSQL.
            . Cada dataframe que se genera se escribe en PostgreSQL. 

        3. Creación del DataFrame de cuentas de FAST que NO coinciden con SF
            · Se aplica un leftanti entre la tabla "BUSINESS_CUST_ACC" de FAST y la tabla "Account" de SF.
            · Se implementan filtros para no tener cuentas repetidas en el dataframe que se escribe en PostgreSQL.
            . Se implementa un filtro para solo considerar cuentas activas en FAST.
            . Se complementa con datos de productos/servicios asociados al cliente/cuenta. 
            . Se complementa con datos de ordenes asociadas al cliente/cuenta. 
            . Cada dataframe que se genera se escribe en PostgreSQL.
    """
    dfFast = None
    dfSFR = None
    dfTgs_Top = None
    dfTgs_Top_Ord = None
    
    def __init__(self, dfFast, dfSFR,dfTgs_Top, dfTgs_Top_Ord):
        self.dfFast = dfFast
        self.dfSFR = dfSFR
        self.dfTgs_Top = dfTgs_Top
        self.dfTgs_Top_Ord = dfTgs_Top_Ord

    def apply(self):
        dfFast = self.dfFast 
        dfSFR = self.dfSFR
        dfTgs_Top = self.dfTgs_Top
        dfTgs_Top_Ord = self.dfTgs_Top_Ord
        ###SFyFAST
        print("\n" +"DATAFRAME CON CUENTAS DE SF QUE COINCIDEN CON CUENTAS DE FAST: ")
        col1 = dfSFR.schema['BI_Id_del_cliente_BIEN__c'].name
        col2 = dfFast.schema['UNIQUE_GLOBAL_ID'].name
        result = match_df_leftsemi(dfSFR, dfFast, col1, col2)#Regla 1 por los campos que hacen Match.
        dfl = result
        print("Numero de filas y columnas respectivas del Dataframe de cuentas de SF coincidentes con FAST: ")
        print(dfl.count())
        print(len(dfl.columns))
        print("AQUI COMIENZA FILTRO DE LAS CUENTAS POR FECHA EN SF.")
        print("DATAFRAME SIN CUENTAS REPETIDAS APLICANDO FILTROS CON 'Last_Account_Activity__c'")
        dfloSF = dfl.groupby('NameSF','BI_No_Identificador_fiscal__c','ParentId','BI_Id_del_cliente_BIEN__c','BI_Identificador_Externo__c','`RecordType.Name`','BI_Activo__c').agg(max_('Last_Account_Activity__c')) # Aqui los agrupa y solo se queda con la fecha mas reciente
        print("RESUMEN DEL DATAFRAME SIN CUENTAS QUE SE REPITEN")
        print("Numero de filas y columnas respectivas del Dataframe: ")
        print(dfloSF.count())
        print(len(dfloSF.columns))
        print("DATAFRAME CON CUENTAS DE FAST QUE COINCIDEN CON CUENTAS DE SF: ")
        col1 = dfFast.schema['UNIQUE_GLOBAL_ID'].name
        col2 = dfSFR.schema['BI_Id_del_cliente_BIEN__c'].name
        result = match_df_leftsemi(dfFast, dfSFR, col1, col2)#Regla 1 por los campos que hacen Match.
        dfr = result
        print("Numero de filas y columnas respectivas del Dataframe con cuentas coincidentes con SF")
        print(dfr.count())
        print(len(dfr.columns))
        print("AQUI COMIENZA FILTRO DE ORDENES EN FAST ")
        print("DATAFRAME SIN CUENTAS REPETIDAS APLICANDO UN FILTRO POR 'MODIFIED_WHEN'")
        dfroFAST = dfr.groupby('NAME','TAX_IDENT_NUMBER','PARENT_ID','UNIQUE_GLOBAL_ID', 'STATUS','OBJECT_ID','CUST_TYPE','OBJECT_ID_BUS','NAME_NC','NAME_HOLDING', 'OBJECT_ID_HOLDING').agg(max_('MODIFIED_WHEN'))#Se queda con la ultima modificacion segun el campo 'MODIFIED_WHEN'
        print("\n" + "DATAFRAME DE FAST CON LA CANTIDAD DE PRODUCTOS/SERVICIOS ASOCIADOS")
        col1 = dfroFAST.schema['OBJECT_ID'].name
        col2 = dfTgs_Top.schema['CUSTOMER_ACCOUNT_TOP'].name
        result = match_df_leftouter(dfroFAST, dfTgs_Top, col1, col2)
        dfroFAST = result
        print("Numero de filas y columnas respectivas del Dataframe: ")
        print(dfroFAST.count())
        print(len(dfroFAST.columns))
        print("\n" + "DATAFRAME DE FAST CON LA CANTIDAD DE PRODUCTOS/SERVICIOS ASOCIADOS Y ORDENES ASOCIADAS")
        col1 = dfroFAST.schema['OBJECT_ID'].name
        col2 = dfTgs_Top_Ord.schema['PARENT_ID_ORD'].name
        result = match_df_leftouter(dfroFAST,dfTgs_Top_Ord,col1,col2)
        dfroFAST = result
        print("Numero de filas y columnas respectivas del Dataframe: ")
        print(dfroFAST.count())
        print(len(dfroFAST.columns))
        print("DATAFRAME TOTAL FILTRADO CON LOS CAMPOS QUE NECESITO DE LAS CUENTAS QUE SE ENCUENTRAN EN SF Y FAST")
        col1 = dfroFAST.schema['UNIQUE_GLOBAL_ID'].name
        col2 = dfloSF.schema['BI_Id_del_cliente_BIEN__c'].name
        result = match_df_inner(dfroFAST, dfloSF, col1, col2)
        dfuTotal = result
        print("RESUMEN DEL DATAFRAME TOTAL")
        print("Numero de filas y columnas respectivas del Dataframe: ")
        print(dfuTotal.count())
        print(len(dfuTotal.columns))
        print('Estos es lo que se escribe en la BBDD: ')
        dfuTotal.sort('NAME').show(5, False)
        write_one_to_many_in_db(dfuTotal, '"DQA"."ControlConsistenciaCuentasCoincidentesFastSF_R1"', ClTipControl=52, ListaClAtrTabSis=[11,2,357,346])#PRO--> ClTipControl=52 / PRE--> ClTipControl=49
        print('Written in BBDD ControlConsistenciaCuentasCoincidentesFastSF_R1\n')#Esta Tabla hace referencia a la Regla 1.
        ###SFNoFast
        print("\n" +"DATAFRAME CON CUENTAS DE SF QUE NO COINCIDEN CON CUENTAS FAST: ")
        col1 = dfSFR.schema['BI_Id_del_cliente_BIEN__c'].name
        col2 = dfFast.schema['UNIQUE_GLOBAL_ID'].name
        result = match_df_leftanti(dfSFR, dfFast, col1, col2)
        dfl = result
        print("Numero de filas y columnas respectivas del Dataframe de cuentas de SF NO coincidentes con FAST: ")
        print(dfl.count())
        print(len(dfl.columns))
        print("AQUI COMIENZA FILTRO DE LAS CUENTAS POR EL CAMPO: 'Last_Account_Activity__c'EN SF.")
        print("DATAFRAME SIN CUENTA REPETIDAS APLICANDO FILTROS CON EL CAMPO: 'Last_Account_Activity__c'")
        dfloSF = dfl.groupby('NameSF','BI_No_Identificador_fiscal__c','ParentId', 'BI_Id_del_cliente_BIEN__c','BI_Identificador_Externo__c','`RecordType.Name`','BI_Activo__c').agg(max_('Last_Account_Activity__c'))  # Aqui los agrupa y solo se queda con la fecha mas reciente
        print("RESUMEN DEL DATAFRAME SIN CUENTAS QUE SE REPITEN")
        print("Numero de filas y columnas respectivas del Dataframe: ")
        print(dfloSF.count())
        print(len(dfloSF.columns))
        print('Estos es lo que se escribe en la BBDD: ')
        dfloSF.sort('NameSF').show(5, False)
        write_one_to_many_in_db(dfloSF, '"DQA"."ControlConsistenciaCuentasEnSFNoFast"', ClTipControl=54, ListaClAtrTabSis=[11,2,357,346])#PRO--> ClTipControl=54 / PRE--> ClTipControl=50 
        print('Written in BBDD ControlConsistenciaCuentasEnSFNoFast\n')
        ###FastNoSF
        print("\n" + "DATAFRAME CON CUENTAS DE FAST QUE NO COINCIDEN CON CUENTAS DE SF: ")
        col1 = dfFast.schema['UNIQUE_GLOBAL_ID'].name
        col2 = dfSFR.schema['BI_Id_del_cliente_BIEN__c'].name
        result = match_df_leftanti(dfFast, dfSFR, col1, col2)
        dfr = result
        print("Numero de filas y columnas respectivas del Dataframe con cuentas NO coincidentes con SF")
        print(dfr.count())
        print(len(dfr.columns))
        print("AQUI COMIENZA FILTRO DE ORDENES EN FAST ")
        print("DATAFRAME SIN CUENTAS REPETIDAS APLICANDO UN FILTRO POR 'MODIFIED_WHEN'")
        dfroFAST = dfr.groupby('NAME','TAX_IDENT_NUMBER','PARENT_ID','UNIQUE_GLOBAL_ID','STATUS','OBJECT_ID','CUST_TYPE','OBJECT_ID_BUS','NAME_NC','NAME_HOLDING', 'OBJECT_ID_HOLDING').agg(max_('MODIFIED_WHEN'))
        print("RESUMEN DEL DATAFRAME SIN CUENTAS QUE SE REPITEN Y FILTRO DE --> STATUS=='Active' ")
        dfroFASTf = dfroFAST.filter(dfroFAST.STATUS == 'Active')
        print("\n" + "DATAFRAME DE FAST CON LA CANTIDAD DE PRODUCTOS/SERVICIOS ASOCIADOS")
        col1 = dfroFASTf.schema['OBJECT_ID'].name
        col2 = dfTgs_Top.schema['CUSTOMER_ACCOUNT_TOP'].name
        result = match_df_leftouter(dfroFASTf, dfTgs_Top, col1, col2)
        dfroFASTf = result
        print("Numero de filas y columnas respectivas del Dataframe: ")
        print(dfroFASTf.count())
        print(len(dfroFASTf.columns))
        print("\n" + "DATAFRAME DE FAST CON LA CANTIDAD DE PRODUCTOS/SERVICIOS ASOCIADOS Y ORDENES ASOCIADAS")
        col1 = dfroFASTf.schema['OBJECT_ID'].name
        col2 = dfTgs_Top_Ord.schema['PARENT_ID_ORD'].name
        result = match_df_leftouter(dfroFASTf,dfTgs_Top_Ord,col1,col2)
        dfroFASTf = result
        print("Numero de filas y columnas respectivas del Dataframe: ")
        print(dfroFASTf.count())
        print(len(dfroFASTf.columns))
        print('Estos es lo que se escribe en la BBDD: ')
        dfroFASTf.sort('NAME').show(5, False)
        write_one_to_many_in_db(dfroFASTf, '"DQA"."ControlConsistenciaCuentasEnFastNoSF"', ClTipControl=53, ListaClAtrTabSis=[11,2,357,346])#PRO--> ClTipControl=53 / PRE--> ClTipControl=51
        print('Written in BBDD ControlConsistenciaCuentasEnFastNoSF\n')
        ###SFyFAST. Nueva regla segun DATAQA-805
        print("\n" + "Nueva regla segun DATAQA-805\nDATAFRAME CON CUENTAS DE SF QUE COINCIDEN CON CUENTAS DE FAST: ")
        col1 = dfSFR.schema['BI_Identificador_Externo__c'].name
        col2 = dfFast.schema['OBJECT_ID'].name
        result = match_df_leftsemi(dfSFR, dfFast, col1, col2)#Regla 2 por los campos que hacen Match.
        dfl = result
        print("Numero de filas y columnas respectivas del Dataframe de cuentas de SF coincidentes con FAST: ")
        print(dfl.count())
        print(len(dfl.columns))
        print("AQUI COMIENZA FILTRO DE LAS CUENTAS POR FECHA EN SF.")
        print("DATAFRAME SIN CUENTAS REPETIDAS APLICANDO FILTROS CON 'Last_Account_Activity__c'")
        dfloSF = dfl.groupby('NameSF','BI_No_Identificador_fiscal__c','ParentId','BI_Id_del_cliente_BIEN__c','BI_Identificador_Externo__c','`RecordType.Name`','BI_Activo__c').agg(max_('Last_Account_Activity__c')) # Aqui los agrupa y solo se queda con la fecha mas reciente
        print("\n" + "DATAFRAME CON CUENTAS DE FAST QUE COINCIDEN CON CUENTAS DE SF: ")
        col1 = dfFast.schema['OBJECT_ID'].name
        col2 = dfSFR.schema['BI_Identificador_Externo__c'].name
        result = match_df_leftsemi(dfFast, dfSFR, col1, col2)#Regla 2 por los campos que hacen Match.
        dfr = result
        print("Numero de filas y columnas respectivas del Dataframe con cuentas coincidentes con SF")
        print(dfr.count())
        print(len(dfr.columns))
        print("AQUI COMIENZA FILTRO DE ORDENES EN FAST ")
        print("DATAFRAME SIN CUENTAS REPETIDAS APLICANDO UN FILTRO POR 'MODIFIED_WHEN'")
        dfroFAST = dfr.groupby('NAME','TAX_IDENT_NUMBER','PARENT_ID','UNIQUE_GLOBAL_ID', 'STATUS','OBJECT_ID','CUST_TYPE','OBJECT_ID_BUS','NAME_NC','NAME_HOLDING', 'OBJECT_ID_HOLDING').agg(max_('MODIFIED_WHEN'))#Se queda con la ultima modificacion segun el campo 'MODIFIED_WHEN'
        print("\n" + "DATAFRAME DE FAST CON LA CANTIDAD DE PRODUCTOS/SERVICIOS ASOCIADOS")
        col1 = dfroFAST.schema['OBJECT_ID'].name
        col2 = dfTgs_Top.schema['CUSTOMER_ACCOUNT_TOP'].name
        result = match_df_leftouter(dfroFAST, dfTgs_Top, col1, col2)
        dfroFAST = result
        print("Numero de filas y columnas respectivas del Dataframe: ")
        print(dfroFAST.count())
        print(len(dfroFAST.columns))
        print("\n" + "DATAFRAME DE FAST CON LA CANTIDAD DE PRODUCTOS/SERVICIOS ASOCIADOS Y ORDENES ASOCIADAS")
        col1 = dfroFAST.schema['OBJECT_ID'].name
        col2 = dfTgs_Top_Ord.schema['PARENT_ID_ORD'].name
        result = match_df_leftouter(dfroFAST,dfTgs_Top_Ord,col1,col2)
        dfroFAST = result
        print("Numero de filas y columnas respectivas del Dataframe: ")
        print(dfroFAST.count())
        print(len(dfroFAST.columns))
        print("\n" + "DATAFRAME TOTAL FILTRADO CON LOS CAMPOS QUE NECESITO DE LAS CUENTAS QUE SE ENCUENTRAN EN SF Y FAST")
        col1 = dfloSF.schema['BI_Identificador_Externo__c'].name
        col2 = dfroFAST.schema['OBJECT_ID'].name
        result = match_df_inner(dfloSF,dfroFAST,col1,col2)
        dfuTotal = result
        print("RESUMEN DEL DATAFRAME TOTAL")
        print("Numero de filas y columnas respectivas del Dataframe: ")
        print(dfuTotal.count())
        print(len(dfuTotal.columns))
        print('Estos es lo que se escribe en la BBDD: ')
        dfuTotal.sort('NAME').show(5, False)
        write_one_to_many_in_db(dfuTotal, '"DQA"."ControlConsistenciaCuentasCoincidentesFastSF_R2"', ClTipControl=92, ListaClAtrTabSis=[11,2,357,346])#PRO--> ClTipControl=92 / PRE--> ClTipControl=92
        print('Written in BBDD ControlConsistenciaCuentasCoincidentesFastSF_R2\n')#Esta Tabla hace referencia a la Regla 2.
        
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Lectura de Tablas:

def read_tables():
    # Lee directamente desde una base de datos FAST que esta en el servidor
    global dfFast,dfSFR,dfTgs_Top,dfTgs_Top_Ord
    print("\n" + "IDENTIFICANDO DATOS EN BUSINESS_CUST_ACC DE FAST: ")
    dfFast = read_FAST("BUSINESS_CUST_ACC")
    print("Numero de filas y columnas respectivas de la tabla BUSINESS_CUST_ACC de FAST: ")
    print(dfFast.count())
    print(len(dfFast.columns))
    print(dfFast.select('UNIQUE_GLOBAL_ID').dtypes)
    print(dfFast.select('OBJECT_ID').dtypes)
    print("Se cambia el tipo de dato en el OBJECT_ID a: ")#Es necesario tenerlo en string para la comparacion con otros dataframe
    dfFast= dfFast.withColumn("OBJECT_ID",col("OBJECT_ID").cast(StringType())) #Se cambia el tipo de la data para el match de las reglas
    print(dfFast.select('OBJECT_ID').dtypes)
    dfFast = dfFast.withColumn('CUST_TYPE', F.when(F.col('CUST_TYPE')==9145485386913639391,'Customer Country').otherwise(F.col('CUST_TYPE')))
    dfFast = dfFast.withColumn('CUST_TYPE', F.when(F.col('CUST_TYPE')==9145485386913639390,'Holding').otherwise(F.col('CUST_TYPE')))
    dfFast = dfFast.withColumn('CUST_TYPE', F.when(F.col('CUST_TYPE')==9145485386913639400,'Legal Entity').otherwise(F.col('CUST_TYPE')))
    dfAux = dfFast.withColumn('STATUS', when(dfFast.STATUS.endswith('6#9BBB59$Active'),regexp_replace(dfFast.STATUS,'[a-zA-z0-9«#$%&/()=*]*Active', 'Active'))\
                                    .when(dfFast.STATUS.endswith('6#F5DF48$Pending Activation'),regexp_replace(dfFast.STATUS,'[a-zA-z0-9«#$%&/()=*]*Pending Activation', 'Pending Activation')))
    dfAux = dfAux.filter(dfAux.STATUS.isNotNull())#Se descarta los valores null
    dfAux = dfAux.filter((dfAux.STATUS == "Active") | (dfAux.STATUS == "Pending Activation"))
    dfFast = dfAux
    query_Holding = """ SELECT "OBJECT_ID_BUS","NAME_NC","NAME_HOLDING", "OBJECT_ID_HOLDING" 
                   FROM "DQA"."ControlCuentasHolding" 
                   WHERE "ClEjecControl" = (SELECT MAX("ClEjecControl")
                                            FROM "DQA"."ControlCuentasHolding")
                    """
    dfHolding = query_DQA(query_Holding)
    print("Dataframe Holding: ")
    print("Numero de Filas: ",dfHolding.count())
    print("Numero de Columnas: ",len(dfHolding.columns))
    col1 = dfFast.schema['OBJECT_ID'].name
    col2 = dfHolding.schema['OBJECT_ID_BUS'].name
    result = match_df_leftouter(dfFast, dfHolding, col1, col2)
    dfAuxHolding = result
    print("Dataframe Fast incluido el Holding de las cuentas: ")
    print("Numero de Filas: ",dfAuxHolding.count())
    print("Numero de Columnas: ",len(dfAuxHolding.columns))
    dfFast =dfAuxHolding
    # Lee diferente tablas en formato parquet que se encuentran en S3 AWS
    print("\n" + "IDENTIFICANDO DATOS EN Account DE SF: ")
    dfSF = read_SF_parquet("Account") 
    dfSFR = dfSF.withColumnRenamed("Name", "NameSF")
    dfSFR = dfSFR.withColumn('RecordType.Name', F.substring_index(F.col('`RecordType.Name`'), '.', -1))#Se elimina ciertos caracteres para tener solo el nombre de la Jerarquia
    dfSFR = dfSFR.filter(dfSFR.BI_Activo__c == 'Sí')#Se considera solo cuentas activas
    print("Numero de filas y columnas respectivas de la tabla Acccount de SF: ")
    print(dfSFR.count())  # Total de filas
    print(len(dfSFR.columns))  # Total de columnas
    print(dfSFR.select('BI_Id_del_cliente_BIEN__c').dtypes)
    print(dfSFR.select('BI_Identificador_Externo__c').dtypes)
    dfSFR = dfSFR.filter(~col('`RecordType.Name`').isin(['Business Unit']) & ~col('`RecordType.Name`').isin(['Cost Center']))
    print("Numero de filas y columnas respectivas de la tabla Acccount de SF sin 'Business Unit' y 'Cost Center': ")
    print(dfSFR.count())  # Total de filas
    print(len(dfSFR.columns))  # Total de columnas
    # Lee  la tabla que hace referencia a productos/servicios asociados a un cliente en FAST
    print("\n" + "IDENTIFICANDO DATOS EN TGS_TOP_PR_INST")
    dfTgs_Top = read_FAST("TGS_TOP_PR_INST")
    print("Resumen Tabla de Datos sin Editar")
    print("Numero de Filas: ",dfTgs_Top.count())
    print("Numero de Columnas: ",len(dfTgs_Top.columns))
    #DF con Edicion en campo NAME con Instance
    dfEditName = dfTgs_Top.withColumn('NAME', F.substring_index(F.col('NAME'), 'Instance', 1))#Sirve#Tiene todas las cabeceras
    #DF con Edicion en campo NAME con
    dfEditName = dfEditName.withColumn('NAME', F.substring_index(F.col('NAME'), '#', 1))#Sirve#Tiene todas las cabeceras
    dfTgs_Top = dfEditName#Se devuelve un DF limpio a la variable original
    print("Se cambia el tipo de dato en el CUSTOMER_ACCOUNT a: ")
    dfTgs_Top = dfTgs_Top.withColumn("CUSTOMER_ACCOUNT",col("CUSTOMER_ACCOUNT").cast(StringType()))#El dato hay que llevarlo a string porque el dato con que se compara es string y en QS es mas practico tenerlo como string 
    print(dfTgs_Top.select('CUSTOMER_ACCOUNT').dtypes)
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
                        .withColumnRenamed('STATUS','STATUS_TOP',)\
                        .withColumnRenamed('PRODUCT_CODE','PRODUCT_CODE_TOP',).sort('NAME_TOP')        
    dfTgs_Top = dfTgs_Top.filter(~col('STATUS_TOP').isin(['Disconnected']))#Excluye la condicion de Disconnected
    print("DF con agrupacion de 'CUSTOMER_ACCOUNT_TOP'")
    dfGroup = dfTgs_Top.groupBy('CUSTOMER_ACCOUNT_TOP').count()
    print("Numero Filas y Columnas despues del groupBy")
    print("Numero de Filas: ",dfGroup.count())
    print("Numero de Columnas: ",len(dfGroup.columns))
    dfTgs_Top = dfGroup#Se escribe un Dataframe con las columnas que necesito en la variable inicial de TOP
    dfTgs_Top = dfTgs_Top.withColumnRenamed('count','Cant_Prod_Instances_active_planned')#dfTgs_Top = dfTgs_Top.withColumnRenamed('count','NumVecesServAsocAlCustomer_TOP')#old
    print("Clientes que tienen algun servicio asociado: ",dfTgs_Top.count())
    # Lee  la tabla que hace referencia a ordenes a un cliente en FAST
    print("\n" + "IDENTIFICANDO DATOS EN TGS_TOP_ORD")
    dfTgs_Top_Ord = read_FAST("TGS_TOP_ORD")
    print("Resumen Tabla de Datos sin Editar")
    print("Numero de Filas: ",dfTgs_Top_Ord.count())
    print("Numero de Columnas: ",len(dfTgs_Top_Ord.columns))
    dfTgs_Top_Ord = dfTgs_Top_Ord.withColumnRenamed('PARENT_ID','PARENT_ID_ORD')
    print("Se cambia el tipo de dato en el PARENT_ID_ORD a: ")
    dfTgs_Top_Ord = dfTgs_Top_Ord.withColumn("PARENT_ID_ORD",col("PARENT_ID_ORD").cast(StringType()))
    print(dfTgs_Top_Ord.select('PARENT_ID_ORD').dtypes)
    print("DF con agrupacion de 'PARENT_ID_ORD'")
    dfGroup = dfTgs_Top_Ord.groupBy('PARENT_ID_ORD').count()
    print("Numero Filas y Columnas despues del groupBy")
    print("Numero de Filas: ",dfGroup.count())
    print("Numero de Columnas: ",len(dfGroup.columns))
    dfTgs_Top_Ord = dfGroup#Se escribe un Dataframe con las columnas que necesito en la variable inicial 
    dfTgs_Top_Ord = dfTgs_Top_Ord.withColumnRenamed('count','Cant_Prod_Orders')#dfTgs_Top_Ord = dfTgs_Top_Ord.withColumnRenamed('count','NumOrdeAsocAlParent_Id_ORD')#old
    print("Clientes que tienen alguna orden asociada: ",dfTgs_Top_Ord.count())
    
# Ejecución:
    
if __name__ == "__main__":
    print("COMIENZA LECTURA DE TABLAS PARA LA GENERACION DE DATAFRAMES")
    read_tables()
    print("COMIENZA DESARROLLO DEL CONTROL")
    ControlConsFastSF = Control_ConsistenciaCuentasCoincidentesFastSF(dfFast,dfSFR,dfTgs_Top,dfTgs_Top_Ord)
    ControlConsFastSF.apply()
    print("CONTROL DONE")    