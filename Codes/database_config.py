import boto3
import base64
from botocore.exceptions import ClientError
from .config import ENTORNO

if ENTORNO == "PRE":
    # OLD
    BUCKET_SF = "s3://s3-bucket-salesforce-muestra-traful/"
    BUCKET_ROD = "s3://s3-bucket-rod-pro-traful-24h/"
    STDNUM_ZIP = "s3://datawall-pro/Controles/TaxIdentificationNumber/stdnum.zip"
    CUENTAS = "s3://datawall-pro/Controles/TaxIdentificationNumber/Cuentas.csv"
    CAMPOS_PRIORITARIOS = 's3://datawall-pro/Warnings/warning_kqis/DQ_Campos_prioritarios_ownership.csv'
    CONTROL_CONSISTENCIA_FASTSF_MAPEO = "s3://test-dqa/Controles/Consistencia_FASTSF/MapeoConsistenciaFASTSF.csv"
    CONTROL_DUPLICADOS_CLIENTE_MAPEO = "s3://test-dqa/Controles/Duplicados/MapeoNombreCliente.csv"
    VW_SUPPLIER_ORDER = "s3://s3-bucket-fast-hist-pro/VW_SUPPLIER_ORDER_FULL_HIST_20200622.csv"
    RUTA_WARNING_CORRECT = "s3://test-dqa/Warnings/Warning_Correct"
    RUTA_WARNING_VARIABILIDAD_DIARIA = "s3://test-dqa/Warnings/Warning_VariabilidadDiaria_Atributo/Variabilidad_KQI"
    RUTA_WARNING_VARIABILIDAD_DIARIA_GENERAL = "s3://test-dqa/Warnings/Warning_VariabilidadDiariaGeneral/Variabilidad_KQI"
    DATA_GENERICOPADREPREVENTA = "s3://test-dqa/Documentacion/Oportunidad_GenericoPreventaPadre.csv"
    DATA_ID_ATRIBUTOS_FAST = "s3://test-dqa/Documentacion/AtributosFAST_ID.csv"
    RUTA_CONTROL_VENDOR_FAST = "s3://test-dqa/Controles/Control_Vendor_Fast"
    RUTA_WARNING_SOBREPASO_UMBRAL_ATRIBUTO="s3://test-dqa/Warnings/warning_sobrepaso_atributos/atribs_sobrepaso"
    RUTA_WARNING_EQUIPOPREVENTA_FICHERO_ENTRADA="s3://test-dqa/Warnings/Warning_EquipoPreventa/Presales_MNCs_TEAM-20200825.csv"
    RUTA_WARNING_SOBREPASO_UMBRAL_ATRIBUTO_FICHERO_COMP_MIN="s3://test-dqa/Warnings/warning_kqis/DQ_Campos_prioritarios_ownership.csv"
    RUTA_WARNING_KQI = "s3://test-dqa/Warnings/warning_kqis/WarningKQI"
    RUTA_SALESFORCE= "s3://pre-dataquality-it/00-Staging-Area/"

    # NEW 
    BUCKET_SF = "s3://s3-bucket-salesforce-muestra-traful/"                                                                                                                                                     # Se usa en en ControlTransversal linea 118
    BUCKET_ROD = "s3://s3-bucket-rod-pro-traful-24h/"                                                                                                                                                           # Se puede eliminar
    STDNUM_ZIP = "s3://pro-dataquality-it/02-Presentation-Area/Datawall-Pro/Controles/TaxIdentificationNumber/stdnum.zip"                                                                                       # Actualizado
    CUENTAS = "s3://pro-dataquality-it/02-Presentation-Area/Datawall-Pro/Controles/TaxIdentificationNumber/Cuentas.csv"                                                                                         # Actualizado
    CAMPOS_PRIORITARIOS = 's3://pro-dataquality-it/02-Presentation-Area/Datawall-Pro/Warnings/warning_kqis/DQ_Campos_prioritarios_ownership.csv'                                                                # Actualizado, pero esta vacio
    CONTROL_CONSISTENCIA_FASTSF_MAPEO = "s3://pre-dataquality-it/02-Presentation-Area/DataQuality/Consistencia_FASTSF/MapeoConsistenciaFASTSF.csv"                                                              # Actualizado
    CONTROL_DUPLICADOS_CLIENTE_MAPEO = "s3://pre-dataquality-it/02-Presentation-Area/DataQuality/Duplicados/MapeoNombreCliente.csv"                                                                             # Actualizado
    #VW_SUPPLIER_ORDER = "s3://pro-dataquality-it/02-Presentation-Area/fast-hist/VW_SUPPLIER_ORDER_FULL_HIST_20200622.csv"                                                                                       # Actualizado
    RUTA_WARNING_CORRECT = "s3://pre-dataquality-it/02-Presentation-Area/Warnings/Warning_Correct/"                                                                                                             # Actualizado
    RUTA_WARNING_VARIABILIDAD_DIARIA = "s3://pre-dataquality-it/02-Presentation-Area/Warnings/Warning_VariabilidadDiaria_Atributo/Variabilidad_KQI/"                                                            # Actualizado
    RUTA_WARNING_VARIABILIDAD_DIARIA_GENERAL = "s3://pre-dataquality-it/02-Presentation-Area/Warnings/Warning_VariabilidadDiariaGeneral/Variabilidad_KQI/"                                                      # Actualizado, antes no existia
    DATA_GENERICOPADREPREVENTA = "s3://pre-dataquality-it/02-Presentation-Area/DataQuality/Documentacion/Oportunidad_GenericoPreventaPadre.csv"                                                                 # Actualizado
    DATA_ID_ATRIBUTOS_FAST = "s3://pre-dataquality-it/02-Presentation-Area/DataQuality/Documentacion/AtributosFAST_ID.csv"                                                                                      # Actualizado
    #RUTA_CONTROL_VENDOR_FAST = "s3://pre-dataquality-it/02-Presentation-Area/DataQuality/Control_Vendor_Fast/"                                                                                                  # Actualizado
    RUTA_WARNING_SOBREPASO_UMBRAL_ATRIBUTO="s3://pre-dataquality-it/02-Presentation-Area/DataQuality/Warnings/warning_sobrepaso_atributos/atribs_sobrepaso/"                                                    # Actualizado
    RUTA_WARNING_EQUIPOPREVENTA_FICHERO_ENTRADA="s3://pre-dataquality-it/02-Presentation-Area/DataQuality/Warnings/Warning_EquipoPreventa/Presales_MNCs_TEAM-20200825.csv"     # Actualizado
    RUTA_WARNING_EQUIPOPREVENTA= "02-Presentation-Area/DataQuality/Warnings/Warning_EquipoPreventa/Warning_EquipoPreventa.xlsx"
    RUTA_WARNING_SOBREPASO_UMBRAL_ATRIBUTO_FICHERO_COMP_MIN="s3://pre-dataquality-it/02-Presentation-Area/DataQuality/Warnings/warning_kqis/DQ_Campos_prioritarios_ownership.csv"                               # Actualizado
    RUTA_WARNING_KQI = "s3://pre-dataquality-it/02-Presentation-Area/DataQuality/Warnings/warning_kqis/WarningKQI/"                                                                                             # Actualizado
    RUTA_SALESFORCE= "s3://pre-dataquality-it/00-Staging-Area/SalesForce/"

    #### Extraccion Salesforce ####
    BUCKET_SF_PQ = "s3://pre-dataquality-it/"
    BUCKET_DQ = "pre-dataquality-it"
    PREFIX_SF = "00-Staging-Area/SalesForce"

    RUTA_LIST_DATASETS_QS = "s3://pre-dataquality-it/02-Presentation-Area/DataQuality/ListDatasetsQS/ListDatasetsQuickSight.csv"
else:
    # OLD
    BUCKET_SF = "s3://s3-bucket-salesforce-muestra-traful/"
    BUCKET_ROD = "s3://s3-bucket-rod-pro-traful-24h/"
    STDNUM_ZIP = "s3://datawall-pro/Controles/TaxIdentificationNumber/stdnum.zip"
    CUENTAS = "s3://datawall-pro/Controles/TaxIdentificationNumber/Cuentas.csv"
    CAMPOS_PRIORITARIOS = 's3://datawall-pro/Warnings/warning_kqis/DQ_Campos_prioritarios_ownership.csv'
    CONTROL_CONSISTENCIA_FASTSF_MAPEO = "s3://datawall-pro/Controles/ConsistenciaFASTSF/MapeoConsistenciaFASTSF.csv"
    CONTROL_DUPLICADOS_CLIENTE_MAPEO = "s3://datawall-pro/Controles/DuplicadosCliente/MapeoNombreCliente.csv"
    VW_SUPPLIER_ORDER = "s3://s3-bucket-fast-hist-pro/VW_SUPPLIER_ORDER_FULL_HIST_20200622.csv"
    RUTA_WARNING_CORRECT = "s3://datawall-pro/Warnings/Warning_Correct"
    RUTA_WARNING_VARIABILIDAD_DIARIA = "s3://datawall-pro/Warnings/Warning_VariabilidadDiaria_Atributo/Variabilidad_KQI"
    RUTA_WARNING_VARIABILIDAD_DIARIA_GENERAL = "s3://datawall-pro/Warnings/Warning_VariabilidadDiariaGeneral/Variabilidad_KQI"
    DATA_ID_ATRIBUTOS_FAST = "s3://datawall-pro/Documentacion/AtributosFAST_ID.csv"
    RUTA_CONTROL_VENDOR_FAST = "s3://datawall-pro/Controles/Control_Vendor_Fast"
    RUTA_WARNING_SOBREPASO_UMBRAL_ATRIBUTO_FICHERO_COMP_MIN="s3://datawall-pro/Warnings/warning_kqis/DQ_Campos_prioritarios_ownership.csv"
    RUTA_WARNING_SOBREPASO_UMBRAL_ATRIBUTO = "s3://datawall-pro/Warnings/warning_sobrepaso_atributos/atribs_sobrepaso"
    RUTA_WARNING_EQUIPOPREVENTA_FICHERO_ENTRADA="s3://datawall-pro/Warnings/Warning_EquipoPreventa/Presales_MNCs_TEAM-20200825.csv"
    RUTA_WARNING_EQUIPOPREVENTA= "02-Presentation-Area/Warnings/Warning_EquipoPreventa/Warning_EquipoPreventa.xlsx"
    DATA_GENERICOPADREPREVENTA = "s3://datawall-pro/Documentacion/Oportunidad_GenericoPreventaPadre.csv"
    RUTA_WARNING_KQI = "s3://datawall-pro/Warnings/WarningKQI"
    RUTA_WARNING_NULOS_MANDATORIES = "s3://datawall-pro/Warnings/Warning_NulosMandatories"
    RUTA_WARNING_MNC = "s3://datawall-pro/Warnings/Warning_MNC"

    RUTA_SALESFORCE= "s3://pro-dataquality-it/00-Staging-Area/SalesForce/"

    # NEW
    BUCKET_SF = "s3://s3-bucket-salesforce-muestra-traful/"                                                                                                                                                     # Se usa en en ControlTransversal linea 118
    BUCKET_ROD = "s3://s3-bucket-rod-pro-traful-24h/"                                                                                                                                                           # Se puede eliminar
    STDNUM_ZIP = "s3://pro-dataquality-it/02-Presentation-Area/Datawall-Pro/Controles/TaxIdentificationNumber/stdnum.zip"                                                                                       # Actualizado
    CUENTAS = "s3://pro-dataquality-it/02-Presentation-Area/Datawall-Pro/Controles/TaxIdentificationNumber/Cuentas.csv"                                                                                         # Actualizado
    CAMPOS_PRIORITARIOS = 's3://pro-dataquality-it/02-Presentation-Area/Datawall-Pro/Warnings/warning_kqis/DQ_Campos_prioritarios_ownership.csv'                                                                # Actualizado, pero esta vacio
    CONTROL_CONSISTENCIA_FASTSF_MAPEO = "s3://pro-dataquality-it/02-Presentation-Area/Datawall-Pro/Controles/ConsistenciaFASTSF/MapeoConsistenciaFASTSF.csv"                                                    # Actualizado
    CONTROL_DUPLICADOS_CLIENTE_MAPEO = "s3://pro-dataquality-it/02-Presentation-Area/Datawall-Pro/Controles/DuplicadosCliente/MapeoNombreCliente.csv"                                                           # Actualizado
    VW_SUPPLIER_ORDER = "s3://pro-dataquality-it/02-Presentation-Area/fast-hist/VW_SUPPLIER_ORDER_FULL_HIST_20200622.csv"                                                                                       # Actualizado
    DATA_GENERICOPADREPREVENTA = "s3://pro-dataquality-it/02-Presentation-Area/DataQuality/Documentacion/Oportunidad_GenericoPreventaPadre.csv"                                                                 # Actualizado
    DATA_ID_ATRIBUTOS_FAST = "s3://pro-dataquality-it/02-Presentation-Area/DataQuality/Documentacion/AtributosFAST_ID.csv"                                                                                      # Actualizado
    RUTA_CONTROL_VENDOR_FAST = "s3://datawall-pro/Controles/Control_Vendor_Fast"                                                                                                                                                                             # No Actualizado
    RUTA_WARNING_SOBREPASO_UMBRAL_ATRIBUTO="s3://pro-dataquality-it/02-Presentation-Area/Warnings/warning_sobrepaso_atributos/atribs_sobrepaso/"                                                                # Actualizado
    RUTA_WARNING_EQUIPOPREVENTA_FICHERO_ENTRADA="s3://pro-dataquality-it/02-Presentation-Area/Warnings/Warning_EquipoPreventa/Presales_MNCs_TEAM-20200825.csv"                 # Actualizado
    RUTA_WARNING_SOBREPASO_UMBRAL_ATRIBUTO_FICHERO_COMP_MIN="s3://pro-dataquality-it/02-Presentation-Area/Warnings/warning_kqis/DQ_Campos_prioritarios_ownership.csv"                                           # Actualizado
    RUTA_WARNING_KQI = "s3://pro-dataquality-it/02-Presentation-Area/Warnings/warning_kqis/WarningKQI/"                                                                                                         # Actualizado
    RUTA_WARNING_VARIABILIDAD_DIARIA = "s3://pro-dataquality-it/02-Presentation-Area/Warnings/Warning_VariabilidadDiaria_Atributo/Variabilidad_KQI/"                                                            # Actualizado
    RUTA_WARNING_VARIABILIDAD_DIARIA_GENERAL = "s3://pro-dataquality-it/02-Presentation-Area/Warnings/Warning_VariabilidadDiariaGeneral/Variabilidad_KQI/"                                                      # Actualizado, antes no existia
    RUTA_WARNING_CORRECT = "s3://pro-dataquality-it/02-Presentation-Area/Warnings/Warning_Correct/"                                                                                                             # Actualizado


    RUTA_SALESFORCE= "s3://pro-dataquality-it/00-Staging-Area/SalesForce/"

    RUTA_LIST_DATASETS_QS = "s3://pro-dataquality-it/02-Presentation-Area/DataQuality/ListDatasetsQS/ListDatasetsQuickSight.csv"

    #### Extraccion Salesforce ####
    BUCKET_SF_PQ = "s3://pro-dataquality-it/"
    BUCKET_DQ = "pro-dataquality-it"
    PREFIX_SF = "00-Staging-Area/SalesForce"

#### Get Credentials ####
def get_secret():

    secret_name = "pre-dq-rdb-fast"
    region_name = "eu-central-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            
    # Your code goes here. 
