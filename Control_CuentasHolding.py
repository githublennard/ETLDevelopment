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
from pyspark.sql.functions import regexp_replace, when, substring_index, col, trim,ltrim,rtrim

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

spark.conf.set( "spark.sql.crossJoin.enabled" , "true" ) 
spark = SparkSession \
    .builder \
    .appName("DATAWALL: Control_CuentasHolding" ) \
    .getOrCreate()

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Funciones Auxiliares:

def match_df_inner(df1,df2,col1,col2):
    print("Haciendo Match entre Dataframes con inner")
    df1 = df1.join(df2, df1[col1] == df2[col2], "inner")
    result = df1
    return result

def match_df_leftanti_mul_con (df1,df2,col1,col2,col4, match_num ):
    print("Haciendo Multiple Match entre Dataframes con leftanti_mul_con")
    df1 = df1.join(df2, (df1[col1] == df2[col2]) & (df2[col4] == match_num), "leftanti")
    result = df1
    return result

def match_df_leftouter(df1,df2,col1,col2):#Me quedo con todos los del lado izquierdo sino hace match completo con null
    print("Haciendo Match entre Dataframes con leftouter")
    df1 = df1.join(df2, df1[col1] == df2[col2], "leftouter")
    result = df1
    return result

def match_df_leftsemi(df1,df2,col1,col2):
    print("Haciendo Match entre Dataframes con leftsemi")
    df1 = df1.join(df2, df1[col1] == df2[col2], "leftsemi")
    result = df1
    return result

def match_df_leftsemi_dif(df1,df2,col1,col2):
    print("Haciendo Match entre Dataframes con leftsemi")
    df1 = df1.join(df2, df1[col1] != df2[col2], "leftsemi")
    result = df1
    return result

def match_df_inner_dual(df1,df2,col1,col2,col3):
    print("Haciendo Multiple Match entre Dataframes con inner")
    df1 = df1.join(df2, (df1[col1] == df2[col2]) | (df1[col1] == df2[col3]), "inner")
    result = df1
    return result

# Desarrollo del control:

class Control_CuentasHolding(control.Control):
    
    """ 
        Descripción: Control Cuentas Holding - TGS en FAST. DATAQA-833.

        Input DATA: 
            - Tabla "BUSINESS_CUST_ACC" de FAST
            - Tabla "CUST_RELATION" de FAST
            - Tabla "NC_OBJECTS" de FAST
            
        Pasos:

        1. Creación del Dataframe con la relacion CUENTAS PADRE E HIJOS/NIETOS de la Tabla "CUST_RELATION" de FAST que permite identificar el Holding. 
            · A traves de un query que se tiene en la HdU se obtiene esta relacion entre las cuentas. El Padre de cada una de las cuentas
              es el Holding, cada cuenta tienen una jerarquia pero aqui lo que se quiere identificar es la cuenta Padre/Holding.
            . Se identifica a los Hijos y Nietos de cada cuenta Padre.
            · Cuando se tiene la relacion completa se guarda en un Dataframe que luego se integra con otras tablas para validar dicha informacion 
              e identificar los datos del cliente.

        2. Creación del Dataframe con datos de los clientes  con la Tabla "BUSINESS_CUST_ACC" de FAST
            · Este Dataframe contiene los datos de las diferentes cuentas se usa para hacer match con el Dataframe que tiene la inter-relacion de clientes.
            · Se implementan filtros en el status del cliente.
            · Se genera un Dataframe auxiliar que se usa al momento de hacer un match para identificar el nombre de la cuenta holding.

        3. Creación del Dataframe de Clientes con NC_OBJECTS.
            · Dataframe que traduce el tipo de cliente que se tiene en BUSINESS_CUST_ACC.

        4. Luego de tener los Dataframe de las respectivas tablas se comienza a realizar el match entre los diferentes Dataframe.
            . La primera relacion que se desarrolla es entre los de dataframes BUSINESS_CUST_ACC y NC_OBJECTS.
            . Luego la relacion entre los dataframes de (BUSINESS_CUST_ACC + NC_OBJECTS) y CUST_RELATION para identificar los Padres e Hijos/Nietos.
            . Al tener identificado a los Padres e Hijo y Nietos, se usa la tabla auxiliar de BUSINESS_CUST_ACC para asi realizar el match final entre cuentas y obtener el Holding.
        
        5. Se escribe en la BBDD el dataframe con los holding de cada cuenta.
            . Este control identifica el Holding que tiene cada cuenta. Son cuentas que se encuentran en FAST.
            
    """
    dfUnionCustRel = None
    dfBus = None
    dfBusAux = None
    dfNC_Obj = None

    def __init__(self, dfUnionCustRel,dfBus,dfBusAux,dfNC_Obj):
        self.dfUnionCustRel = dfUnionCustRel 
        self.dfBus = dfBus
        self.dfBusAux = dfBusAux
        self.dfNC_Obj = dfNC_Obj
    
    def apply(self):
        dfUnionCustRel = self.dfUnionCustRel
        dfBus = self.dfBus
        dfBusAux = self.dfBusAux
        dfNC_Obj = self.dfNC_Obj
        print("TIPO DE CUENTAS QUE SE TIENEN EN BUSINESS SEGUN NC_OBJECTS")#Este Join se hace en referencia para clasificar el tipo de jerarquia de los clientes por la tabla NC_OBJECTS, se unen dos tablas.
        col1 = dfBus.schema['CUST_TYPE_BUS'].name#Este campo me dice que tipo de cliente es en la tabla bussines account.
        col2 = dfNC_Obj.schema['OBJECT_ID_NC'].name
        result = match_df_inner(dfBus, dfNC_Obj, col1, col2)#Este match permite definir si la cuenta es CustomerCountry,Holding,LegalEntity
        dfResultBusAndNC = result #Dataframe con la relacion en comun completa entre Business y Cust_Relation
        print("Total de cuentas identificadas con NC_OBJECTS")#Puedo ver que el cliente ACCEDO tiene dos jerarquias
        print(dfResultBusAndNC.count())                       #Esto permite identificar a los clientes pero no identificar la relacion entre ellas  
        print(len(dfResultBusAndNC.columns))
        print("CUENTAS PADRE RELACION ENTRE CUENTAS SEGUN CUST_RELATION")#Esto es para hacer el match final entre 2 tablas y la tabla dfUnionCustRel,que se genera a traves de la subquery 
        col1 = dfResultBusAndNC.schema['OBJECT_ID_BUS'].name
        col2 = dfUnionCustRel.schema['PARENT_ID'].name
        result = match_df_inner(dfResultBusAndNC, dfUnionCustRel, col1, col2)
        dfPadre = result
        print(dfPadre.count())
        print(len(dfPadre.columns))
        print("CUENTAS HIJOS/NIETOS SEGUN CUST_RELATION")#Esto es para hacer el match final entre 2 tablas y la tabla dfUnionCustRel que se genera a traves de la subquery
        dfResultBusAndNC = dfResultBusAndNC.withColumn("OBJECT_ID_BUS",col("OBJECT_ID_BUS").cast(StringType()))
        col1 = dfResultBusAndNC.schema['OBJECT_ID_BUS'].name
        col2 = dfUnionCustRel.schema['REL_CUST'].name
        result = match_df_inner(dfResultBusAndNC, dfUnionCustRel, col1, col2)
        dfHijo = result
        print(dfHijo.count())
        print(len(dfHijo.columns))
        dfHijo = dfHijo.sort("NAME_BUS")
        print("UNION CUENTAS PADRE E HIJOS/NIETOS")
        dfTablaAux = dfPadre.union(dfHijo)
        print(dfTablaAux.count())
        print(len(dfTablaAux.columns)) 
        print("Identificando el Holding de cada cuenta")
        col1 = dfTablaAux.schema['PARENT_ID'].name
        col2 = dfBusAux.schema['OBJECT_ID_HOLDING'].name
        result = match_df_leftouter(dfTablaAux, dfBusAux, col1, col2)
        dfTablaFinal = result
        print(dfTablaFinal.count())
        print(len(dfTablaFinal.columns)) 
        #Reorganizando las columnas
        dfTablaFinal = dfTablaFinal.select('NAME_BUS','OBJECT_ID_BUS','CUST_TYPE_BUS','STATUS_BUS','NAME_NC','PARENT_ID','REL_CUST','NAME_HOLDING','OBJECT_ID_HOLDING','ID_CUST_TYPE').sort('NAME_BUS')
        #dfTablaFinal.printSchema()
        self.dfTablaFinal = dfTablaFinal 
        #dfTablaFinal.sort("NAME_BUS").show(100, False)
        
    def write_in_db(self):
        print('Estos es lo que se guarda en la BBDD:')
        self.dfTablaFinal.printSchema()
        print("Numero de Filas: " , self.dfTablaFinal.count())
        print("Numero de Columnas: " , len(self.dfTablaFinal.columns))
        self.dfTablaFinal.sort("NAME_BUS").show(10, False)
        write_one_to_many_in_db(self.dfTablaFinal, '"DQA"."ControlCuentasHolding"', ClTipControl=108)
        print('Written in BBDD ControlCuentasHolding') 

def read_tables():
    global dfUnionCustRel, dfBus, dfBusAux, dfNC_Obj
    print("IDENTIFICANDO PADRES-NIETOS EN CUST_RELATION: ")
    dfCustRel = read_FAST("CUST_RELATION")
    dfCustRel1 = dfCustRel
    dfCustRel2 = dfCustRel
    print("Numero de filas y columnas respectivas de la tabla CUST_RELATION de FAST: ")
    print(dfCustRel.count())
    print(len(dfCustRel.columns))
    print("Generando Rel1 y Rel2 del subquery")
    dfCustRel1 = dfCustRel1.select('NAME','OBJECT_ID','REL_TYPE','PARENT_ID','REL_CUST').sort('OBJECT_ID')
    dfCustRel1 = dfCustRel1.filter(dfCustRel.REL_TYPE == 9137799819213569275).sort('OBJECT_ID')#Esto permite filtrar por el REL_TYPE
    dfCustRel2 = dfCustRel2.select('NAME','OBJECT_ID','REL_TYPE','PARENT_ID','REL_CUST').sort('OBJECT_ID')
    dfCustRel2 = dfCustRel2.filter(dfCustRel.REL_TYPE == 9137799819213569275).sort('OBJECT_ID')#Esto permite filtrar por el REL_TYPE
    dfCustRel1 = dfCustRel1.withColumn("REL_CUST",col("REL_CUST").cast(StringType()))#Se convierte a string para que haya un mejor calculo
    dfCustRel2 = dfCustRel2.withColumn("PARENT_ID",col("PARENT_ID").cast(StringType()))#Se convierte a string para que haya un mejor calculo
    dfCustRel1 = dfCustRel1.select('NAME','OBJECT_ID','REL_TYPE','PARENT_ID','REL_CUST')\
                            .withColumnRenamed('NAME','NAME_R1')\
                            .withColumnRenamed('OBJECT_ID','OBJECT_ID_R1')\
                            .withColumnRenamed('REL_TYPE','REL_TYPE_R1')\
                            .withColumnRenamed('PARENT_ID','PARENT_ID_R1',)\
                            .withColumnRenamed('REL_CUST','REL_CUST_R1',).sort('NAME_R1')
    dfCustRel2 = dfCustRel2.select('NAME','OBJECT_ID','REL_TYPE','PARENT_ID','REL_CUST')\
                            .withColumnRenamed('NAME','NAME_R2')\
                            .withColumnRenamed('OBJECT_ID','OBJECT_ID_R2')\
                            .withColumnRenamed('REL_TYPE','REL_TYPE_R2')\
                            .withColumnRenamed('PARENT_ID','PARENT_ID_R2',)\
                            .withColumnRenamed('REL_CUST','REL_CUST_R2',).sort('NAME_R2')
    print("Rel1 y Rel2 Padre-Nieto")
    col1 = dfCustRel1.schema['REL_CUST_R1'].name
    col2 = dfCustRel2.schema['PARENT_ID_R2'].name
    result = match_df_inner(dfCustRel1, dfCustRel2, col1, col2) #Coincidencia con inner
    dfAuxNieto = result
    dfAuxNieto = dfAuxNieto.select('NAME_R1','PARENT_ID_R1','REL_CUST_R2').sort('NAME_R1')#Donde PARENT_ID_R1 es el Padre y REL_CUST_R2 es el Nieto
    print(dfAuxNieto.count())
    print(len(dfAuxNieto.columns))
    dfResultNieto = dfAuxNieto 
    print("Rel1 y Rel2 Padre-Hijo")
    col1 = dfCustRel1.schema['PARENT_ID_R1'].name
    col2 = dfCustRel2.schema['REL_CUST_R2'].name
    #col3 = dfCustRel1.schema['REL_TYPE_R1'].name
    col4 = dfCustRel2.schema['REL_TYPE_R2'].name
    match_num = 9137799819213569275  
    result = match_df_leftanti_mul_con(dfCustRel1, dfCustRel2, col1, col2, col4, match_num) #Diferencia con leftanti_mul_con#Muy importante el leftanti 
    dfResultHijo = result
    print(dfResultHijo.count())
    print(len(dfResultHijo.columns))
    dfResultHijo = dfResultHijo.select('NAME_R1','PARENT_ID_R1','REL_CUST_R1').sort('NAME_R1')#Donde PARENT_ID_R1 es el Padre y REL_CUST_R1 es el Hijo
    print("Realizando UNION de los Dataframe Cust_Relation de Padre-Hijo y Padre-Nieto")#Esta funcion Union de Pyspark no se usa el distinct() ya que no hay y no deberia 
    dfUnionCustRel = dfResultNieto.union(dfResultHijo)#Predomina el nombre de las columnas del Dataframe que recibe la union; en este caso "dfResultNieto" 
    print(dfUnionCustRel.count())
    print(len(dfUnionCustRel.columns))
    print("Cambiando Nombre de Columnas luego de la Union de los dos Dataframe Padre-Hijo Padre-Nieto")
    dfUnionCustRel = dfUnionCustRel.withColumnRenamed('NAME_R1','NAME')\
                                .withColumnRenamed('PARENT_ID_R1','PARENT_ID')\
                                .withColumnRenamed('REL_CUST_R2','REL_CUST')#El renombrado queda hasta aqui!#Aqui tengo todos los padres de las cuentas
    print("\n" + "IDENTIFICANDO DATOS EN BUSINESS_CUST_ACC DE FAST: ")
    dfBus = read_FAST("BUSINESS_CUST_ACC")
    print("Numero de filas y columnas respectivas de la tabla BUSINESS_CUST_ACC de FAST: ")
    print(dfBus.count())
    print(len(dfBus.columns))
    dfBus = dfBus.select('NAME','CUST_TYPE','OBJECT_ID','STATUS').sort('NAME')
    dfBus = dfBus.withColumn('STATUS', when(dfBus.STATUS.endswith('6#9BBB59$Active'),regexp_replace(dfBus.STATUS,'[a-zA-z0-9«#$%&/()=*]*Active', 'Active'))\
                                    .when(dfBus.STATUS.endswith('6#F5DF48$Pending Activation'),regexp_replace(dfBus.STATUS,'[a-zA-z0-9«#$%&/()=*]*Pending Activation', 'Pending Activation')))
    dfBus = dfBus.filter(dfBus.STATUS.isNotNull())#Se descarta los valores null
    dfBus = dfBus.filter(~col('NAME').isin([' -','-','--'])) #Funciona perfecto
    dfBus = dfBus.filter((dfBus.STATUS == "Active") | (dfBus.STATUS == "Pending Activation"))
    dfBus = dfBus.withColumn('NAME', ltrim(col('NAME')))
    print("Sin null y limpio de ciertos caracteres al inicio")
    print(dfBus.count())
    print(len(dfBus.columns))
    dfBus = dfBus.withColumnRenamed('NAME','NAME_BUS')
    dfBus = dfBus.withColumnRenamed('OBJECT_ID','OBJECT_ID_BUS')
    dfBus = dfBus.withColumnRenamed('CUST_TYPE','CUST_TYPE_BUS')#Esta campo me identifica que tipo de clientes|cuenta es
    dfBus = dfBus.withColumnRenamed('STATUS','STATUS_BUS')
    print("Generando Auxiliar en dfBus para futuros match")
    dfBusAux = dfBus
    dfBusAux = dfBusAux.select('NAME_BUS','OBJECT_ID_BUS','CUST_TYPE_BUS').sort('NAME_BUS')
    dfBusAux = dfBusAux.withColumnRenamed('NAME_BUS','NAME_HOLDING')\
                                .withColumnRenamed('OBJECT_ID_BUS','OBJECT_ID_HOLDING')\
                                .withColumnRenamed('CUST_TYPE_BUS','ID_CUST_TYPE')
    print("\n" + "IDENTIFICANDO CON NC_OBJECTS: ")
    query_NC = 'SELECT "OBJECT_ID", "PARENT_ID", "NAME", "DESCRIPTION" FROM "NETCRACKER"."NC_OBJECTS"'
    dfNC_Obj = query_FAST(query_NC)
    dfNC_Obj = dfNC_Obj.withColumnRenamed('NAME','NAME_NC')
    dfNC_Obj = dfNC_Obj.withColumnRenamed('OBJECT_ID','OBJECT_ID_NC')
    dfNC_Obj = dfNC_Obj.withColumnRenamed('PARENT_ID','PARENT_ID_NC')
    dfNC_Obj = dfNC_Obj.withColumnRenamed('DESCRIPTION','DESCRIPTION_NC')
    #Filter IS IN List values
    li=[9145485386913639391,9145485386913639390,9145485386913639400]#ReferenciasRespectivas: CustomerCountry,Holding,LegalEntity
    dfNC_Obj = dfNC_Obj.filter(dfNC_Obj.OBJECT_ID_NC.isin(li))
    print(dfNC_Obj.count())
    print(len(dfNC_Obj.columns))
           
if __name__ == "__main__":
    print("COMIENZA LECTURA DE TABLAS PARA LA GENERACION DE DATAFRAMES")
    read_tables()
    print("COMIENZA DESARROLLO DEL CONTROL")
    cuentas_holding = Control_CuentasHolding(dfUnionCustRel, dfBus, dfBusAux, dfNC_Obj)
    cuentas_holding.apply()
    cuentas_holding.write_in_db()
    print("FINALIZO EL CONTROL")