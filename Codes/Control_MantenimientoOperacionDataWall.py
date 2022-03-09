# -*- coding: utf-8 -*-
import boto3
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import Row, SparkSession, Column
from pyspark import sql
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.functions import col, trim, collect_set, explode
import config.database_config as database_config
import math
from database_utils import *

import control
from pyspark.sql.types import StructType,StructField, StringType,DateType
from pyspark.sql.functions import udf

from util import *
import time
import datetime
from datetime import date
import itertools

# Sin esto da error al hacer print con determinados caracteres
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

spark=SparkSession \
    .builder \
    .appName("DATAWALL: Control_MantenimientoOperacionDataWall") \
    .getOrCreate()

class Control_MOD(control.  Control):
    dfResultAllHistoricoOKNOKControl=None
    dfDBSize=None
    dfTablasSize=None
    
    """ 
    Descripcion: 
    Control para que los desarrolladores puedan visualizar en quicksight el historico de ejecucion de los controles(OK/NOK), historico de tamaño de 
    las tablas en PostgreSQL (todas la tablas en "DQA"), y el historico de tamaño de "DQA", de este modo poder controlar mas facilmente el desarrollo.

    Input Data:
    - Todas las tablas

    Pasos:
    1. Lee informacion del esquema de "DQA" y se asigna valores a "listTableName" excluyendo las tablas de warning, y las tres propias de historico que se va a crear con este control.
    2. Un bucle que va enlazando cada tabla con la tabla "EjecucionControl" para conocer la fecha de ejecucion de cada control.
    3. Si un dataframe con fecha devuelto no tiene registro, le asignamos un NOK a ese control, ya que ningun dia se ha ejecutado, por tanto, no hay registros.  
    4. Convertir la columna de fecha del paso 3 a una lista de fechas.
    5. Si la fecha hoy esta en la lista devuelta del punto 4. le asignamos un OK a ese control. En caso contrario, un NOK.
    6. Sacar el Historico DB size (DQA) y convertir los valores en GB para poder ordenar en Quicksight
    7. Sacar el Historico Tablas size (todas las tablas en DQA), y convertir valores kBytes para poder ordenar en Quicksight
    """

    def apply(self):
       
       # lee los nombres de todas las tablas de dqa 
        table= query_DQA( """ SELECT * FROM(SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema='DQA') rs
                        """)
          
        table.show(5,False)
                
        listTableName=[row.table_name for row in table.collect()]
        listTableNameSinContarWarning=[]

        # Excluir los warnings que hace el historico OK/NOK en sus propios codigos, y excluir tambien las tres tablas de historico
        for i in listTableName:
            if i.find("arning")==-1 and i.find("EquipoPreventa")== -1 and i.find("Historico_"):
                listTableNameSinContarWarning.append(i)
            else:
                continue

        schema = StructType([
        StructField('OKNOK', StringType(), True),
        StructField('ControlName', StringType(), True),
        ])

        listControlNameTotal=[]
        listOKNOKTotal=[]
        ResultAllHistoricoOKNOKControl=[]
        today = date.today().strftime("%d/%m/%Y")
        todaydate = today[6:10] + '-' + today[3:5] + '-' + today[0:2]

        tableHistorico=query_DQA( """ SELECT * from "DQA"."HistoricoEjecucionControlOKNOK" """ )
            
        
        #listTableNameSinContarWarning=['ControlNulos','KQI','ControlOportunidadesCliente','ControlVariacionKQI','ControlDuplicadosClienteSF','KQI_Controles','ReglaNegocio_VM_SLAs_Instancias','ReglaNegocio_VM_SLAs_Ordenes','ReglaNegocio_VM_Fechas','KQI_Sistema','Historico_TablasPostgreSQLSize','ReglaNegocio_Region','ReglaNegocio_NumeroCuentas','ControlDatatype','ControlCuentaOportunidad','ControlTaxIdentification','ControlDistinctNonDistinct','ControlDisponibilidad','ControlDuplicadosInstancia','ControlUnicidadContacto','ControlConsistencia','ControlRangos','ControlDuplicadosClienteFAST','EjecucionControl','KQI_Atributos','ControlDuplicadosProveedores','ControlDuplicadosServiciosFAST','ControlDuplicadosServiciosSF', 'ControlDefaults','ControlCuentaOportunidadHolding', 'Outliers','ControlOutliers','ControlVendorFAST','ControlCorruptos']
        for  i in listTableNameSinContarWarning:

            query=""" SELECT DISTINCT DATE(ejc."Fecha") AS "Fecha" FROM "DQA"."""+ "\""+i+"\" "  +i +""" JOIN "DQA"."EjecucionControl" ejc ON ejc."ClEjecControl" = """+ i + """."ClEjecControl" """
            #print(query)
            try:
                dfFECHAS=query_DQA(query).distinct()
            except Exception as e:
                #print("Tabla no tiene EjecucionContro, no hace falta comprobar OK/NOK")
                continue
            # En caso que no hay fecha registrada para ese control en tabla ejecucion control, significa que no se ha ejecutado bien, asigno NOK
            if dfFECHAS.count()==0:
                
                listControlNameTotal.append(i)
                listOKNOKTotal.append("NOK")
                
                continue

            # Convertir fechas  ejecutadas en una lista

            fechaListControl=dfFECHAS.select("Fecha").withColumn("FechaString",col("Fecha").cast(StringType())).drop("Fecha").withColumnRenamed("FechaString","Fecha")
            fechaListControl=[row.Fecha for row in fechaListControl.collect()]
            today = date.today().strftime("%d/%m/%Y")
            todaydate = today[6:10] + '-' + today[3:5] + '-' + today[0:2]
          
           # Si es la primera vez que ha ejecutado el control  y bbdd esta vacio, saca el histrico 
            if tableHistorico.count()==0:
                for i in range(0,len(fechaListControl)):
                    listControlNameTotal.append(i)
                    listOKNOKTotal.append("OK")
            else:
                if todaydate in fechaListControl:
                    listOKNOKTotal.append("OK")
                else:
                    listOKNOKTotal.append("NOK")
                listControlNameTotal.append(i)
        
        #convertir la lista en un df
        for eElem in range(len(listControlNameTotal)):
        #print((listRangeOfDatesTotal[eElem], listOKNOKTotal[eElem],listControlNameTotal[eElem]))
            ResultAllHistoricoOKNOKControl.append((listOKNOKTotal[eElem],listControlNameTotal[eElem]))

        self.dfResultAllHistoricoOKNOKControl= spark.createDataFrame(data=ResultAllHistoricoOKNOKControl, schema = schema)
        self.dfResultAllHistoricoOKNOKControl.show(5,False)
 
        # Historico DB size
        dfDBSize1=query_DQA(""" 
            SELECT schema_name, 
                pg_size_pretty(sum(table_size)::bigint)
            FROM (
            SELECT pg_catalog.pg_namespace.nspname as schema_name,
                    pg_relation_size(pg_catalog.pg_class.oid) as table_size
            FROM   pg_catalog.pg_class
                JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid
            ) t
            GROUP BY schema_name
            """).filter(col("schema_name")=="DQA").withColumnRenamed("pg_size_pretty","SizeDB").select("SizeDB")
        def convertToGB(x):
            """ Convertir tamano de DB en GB para poder ordenar en quicksight """
            if 'kB' in x:
                sizeGB=(int(x[:-2]))/1024/1024
            elif 'MB' in x:
                sizeGB=(int(x[:-2]))/1024
            elif 'GB' in x:
                sizeGB=(int(x[:-2]))
            else:
                sizeGB=int(x[:-5])/1024/1024/1024
            
            return sizeGB
        convertToGB=  F.udf(convertToGB,IntegerType())
        self.dfDBSize=dfDBSize1.withColumn("SizeDBInGB",convertToGB("SizeDB")).drop("SizeDB")
        self.dfDBSize.show(5,False)

        # Historico Tablas size
        datasetSize1=query_DQA(""" SELECT relname AS "relation", pg_size_pretty ( pg_total_relation_size (C .oid) ) AS "total_size" FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C .relnamespace) WHERE nspname NOT IN ( 'pg_catalog', 'information_schema' ) AND C .relkind <> 'i' AND nspname !~ '^pg_toast' ORDER BY "total_size" DESC """)
        """  Convertir tamano de dataset en bytes"""
        def convertToByes(x):

            """ Convertir tamano de DB en kBytes para poder ordenar en quicksight """
            if 'kB' in x:
                sizeByte=(int(x[:-2]))
            elif 'MB' in x:
                sizeByte=(int(x[:-2]))*1024
            elif 'GB' in x:
               
                sizeByte=(int(x[:-2]))*1024*1024
            else:
                sizeByte=int(x[:-5])/1024
            
            return sizeByte

        convertToByes=  F.udf(convertToByes,IntegerType()) 
       
        datasetSize=datasetSize1.withColumn("DSSizekBytes",convertToByes("total_size"))
        self.dfTablasSize=datasetSize.join(table, table.table_name==datasetSize.relation).drop("relation").withColumnRenamed("table_name","DSName").withColumnRenamed("total_size","DSSize")
        self.dfTablasSize.show(5,False)

    def write_in_db(self):  
        write_one_to_many_in_db(self.dfResultAllHistoricoOKNOKControl, '"DQA"."HistoricoEjecucionControlOKNOK"', ClTipControl=39)
        write_one_to_many_in_db(self.dfDBSize, '"DQA"."HistoricoDBSize"', ClTipControl=40)
        write_one_to_many_in_db(self.dfTablasSize, '"DQA"."HistoricoTablasPostgreSQLSize"', ClTipControl=41)
        print("Write in DB Control_MantenimientoOperacionDataWall")

def ejecutar():
    cMOD=Control_MOD()
    cMOD.apply()
    cMOD.write_in_db()

if __name__ == "__main__":
    ejecutar()