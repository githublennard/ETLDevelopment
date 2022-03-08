z#!/bin/bash

OUTPUT_FILE=/home/dq/salidas/salida_total_$(date +%Y%m%d).txt

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# CONTROLES

date > $OUTPUT_FILE

# CONTROL TRANSVERSAL
echo _______________________Control Transversal_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_Transversal.py >> $OUTPUT_FILE  

date >> $OUTPUT_FILE

# CONTROL CORRUPTOS
echo _______________________Control Corruptos_______________________ >> $OUTPUT_FILE
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_corruptos.py >> $OUTPUT_FILE

date >> $OUTPUT_FILE

# CONTROL DUPLICADOS CLIENTE - FAST
echo _______________________Control Duplicados Cliente FAST_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_DuplicadosClientesFAST.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL DUPLICADOS CLIENTE - SF
echo _______________________Control Duplicados Cliente SF_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_DuplicadosClientesSF.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL CONSISTENCIA FAST-SF
echo _______________________Consistencia FAST SF_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_ConsistenciaFASTSF.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL UNICIDAD CONTACTO
echo _______________________Unicidad contacto_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_unicidad_contacto.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL DUPLICADOS DE INSTANCIAS
echo _______________________Duplicados Instancias_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_DuplicadosInstanciasFAST.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL DUPLICADOS PROVEEDORES
echo _______________________Duplicados Proveedores_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_DuplicadosProveedoresFAST.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL DUPLICADOS SERVICIOS FAST
echo _______________________Duplicados Servicios FAST_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_DuplicadosServiciosFAST.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL DUPLICADOS SERVICIOS SF
echo _______________________Duplicados Servicios SF_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_DuplicadosServiciosSF.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL DEFAULTS
echo _______________________Control Defaults_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_Defaults.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL TAX IDENTIFICATION NUMBER
echo _______________________Control TaxIdentificationNumber_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_Tax_identification.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL DISTINCT - NONDISTINCT
echo _______________________Control Distinct-NonDistinct_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_Distinct.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL DATATYPE
echo _______________________Control Datatype_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_Datatype.py >> $OUTPUT_FILE 

# CONTROL VENDOR FAST
#echo _______________________ Control Vendor FAST _______________________ >> $OUTPUT_FILE 
#spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_VendorFAST_V3.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL DIFERENCIA EQUIPO CUENTA Y EQUIPO OPORTUNIDAD
echo _______________________ Control Diferencia Equipo Cuenta y Equipo Oportunidad _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_CuentaOportunidad.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL DIFERENCIA EQUIPO CUENTA A NIVEL HOLDING Y EQUIPO OPORTUNIDAD
echo _______________________ Control Diferencia Equipo Cuenta a nivel Holding y Equipo Oportunidad  _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_CuentaOportunidad_Holding.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL Corporate
echo _______________________ Control Corporate  _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/ControlCorporate.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL Generico Preventa 
echo _______________________ Control Generico Preventa  _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_GenericoPreventaID.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL CONSISTENCIA CUENTAS COINCIDENTES FAST SF 
echo _______________________ Control Consistencias Cuentas Coincidentes Fast SF   _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_ConsistenciaCuentasCoincidentesFastSF.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL Validacion Direct Customer 
echo _______________________ Validacion Direct Customer    _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_ValidacionDirectCustomer.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL Nombre Cliente Provedor 
echo _______________________ Control Nombre Cliente Provedor   _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_NombreClienteProvedor.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL Digitalizacion Facturas
echo _______________________ Control Digitalizacion Facturas    _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_DigitalizacionFacturas.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL CALIDAD CLIENTES TELXIUS 
echo _______________________ Calidad Clientes Telxius    _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_CalidadClientesTelxiusTGSFAST.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL CCC
echo _______________________ CONTROL CCC    _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_CustomerControlCenter.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROLES SERVICE CIRCUIT
echo _______________________ CONTROLES SERVICE CIRCUIT    _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_ServiceCircuit.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROLES TICKETS SALESFORCE_ROD
echo _______________________ CONTROL TICKETS SF ROD    _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_Tickets_SF_RoD.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROLES SERVICE_ID NGN FAST
echo _______________________ CONTROL SERVICE_ID NGN FAST    _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_ServiceID_NGN_FAST.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROLES AGREGADOS FAST
echo _______________________ CONTROL AGREGADOS FAST   _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_Agregados_Fast.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# CONTROL CUENTAS HOLDING DATAQA-833 
echo _______________________ CUENTAS HOLDING _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_CuentasHolding.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# REGLAS DE NEGOCIO

# VM - SLAs ORDENES
echo _______________________Regla de Negocio VM - SLAs Ordenes______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Regla_VM_SLAs_Ordenes.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# VM - SLAs INSTANCIAS
echo _______________________Regla de Negocio VM - SLAs Instancias_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Regla_VM_SLAs_Instancias.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

# VM - FECHAS
echo _______________________Regla de Negocio VM - Fechas_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Regla_VM_Fechas.py >> $OUTPUT_FILE 


# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# CALCULO DE LOS KQIs

date >> $OUTPUT_FILE

echo _______________________ Control KQIs _______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_KQIs.py >> $OUTPUT_FILE 

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# WARNINGS

date >> $OUTPUT_FILE

echo _______________________Warning KQI_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Warning_SobrepasoUmbral.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

echo _______________________Warning mnc_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/warning_mnc.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

echo _______________________Warning Nulos Mandatories_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/warning_nulos_mandatories.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

echo _______________________Warning Wariabilidad_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/warning_wariabilidad_diaria.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

echo _______________________Warning Sufijo Correct_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Warning_Correct.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

echo _______________________Warning Variabilidad Atributo___________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Warning_VariabilidadDiariaATR.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

echo _______________________Warning Sobrepaso Umbral Atributo___________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/warning_sobrepaso_umbral_atributo.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE

echo _______________________Warning Equipo Preventa___________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/warning_equipoPreventa.py >> $OUTPUT_FILE 

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Control Mantenimiento y Operacion Data Wall

date >> $OUTPUT_FILE

echo _______________________Control Mantenimiento y Operacion Data Wall_______________________ >> $OUTPUT_FILE 
spark-submit --deploy-mode client --queue dq --master yarn --num-executors 4 --executor-memory 4GB /home/dq/Datawall_Master/dq/Control_MantenimientoOperacionDataWall.py >> $OUTPUT_FILE 

date >> $OUTPUT_FILE
