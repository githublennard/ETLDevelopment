--DELETE
DELETE FROM "DQA"."ControlClientesTelxiusServiciosNoTelxius"
WHERE "ClEjecControl" = 1209562;

--ALTER TABLE
ALTER TABLE "DQA"."ControlConsistenciaCuentasEnFastNoSF"
ADD COLUMN "CUSTOMER_ACCOUNT_TOP" text,
ADD COLUMN "NumVecesServAsocAlCustomer_TOP" bigint;

--INSERT DATA
INSERT INTO "DQA"."ControlConsistenciaCuentasCoincidentesFastSF_R2" (
"ClEjecControl","NameSF" ,"BI_No_Identificador_fiscal__c", "ParentId","BI_Id_del_cliente_BIEN__c" ,"BI_Identificador_Externo__c",
"RecordType.Name", "BI_Activo__c" , "max(Last_Account_Activity__c)", "NAME" , "TAX_IDENT_NUMBER", "PARENT_ID" ,"UNIQUE_GLOBAL_ID",
"STATUS", "OBJECT_ID","CUST_TYPE","OBJECT_ID_BUS","NAME_NC","NAME_HOLDING","OBJECT_ID_HOLDING","max(MODIFIED_WHEN)",
"CUSTOMER_ACCOUNT_TOP","Cant_Prod_Instances_active_planned","PARENT_ID_ORD" ,"Cant_Prod_Orders" )
SELECT
*
FROM "DQA"."ControlConsistenciaCuentasCoincidentesFastSF_R2_OLD";

--CREATE TABLE FROM TWO TABLES
CREATE TABLE "ControlConsistenciaCuentasCoincidentesFastSF_R2_new" AS
SELECT
"ClEjecControl" ,"NameSF" ,"BI_No_Identificador_fiscal__c", "ParentId","BI_Id_del_cliente_BIEN__c" ,"BI_Identificador_Externo__c",
 "RecordType.Name", "BI_Activo__c" , "max(Last_Account_Activity__c)","NAME", "TAX_IDENT_NUMBER" , "PARENT_ID" ,"UNIQUE_GLOBAL_ID",
 "STATUS","OBJECT_ID","CUST_TYPE","OBJECT_ID_BUS","NAME_NC","NAME_HOLDING","OBJECT_ID_HOLDING", "max(MODIFIED_WHEN)",
"CUSTOMER_ACCOUNT_TOP","NumVecesServAsocAlCustomer_TOP" , "PARENT_ID_ORD", "NumOrdeAsocAlParent_Id_ORD"
FROM "DQA"."ControlConsistenciaCuentasCoincidentesFastSF_R2"
INNER JOIN "DQA"."ControlCuentasHolding_LAST"
ON "ControlConsistenciaCuentasCoincidentesFastSF_R2"."OBJECT_ID" = "DQA"."ControlCuentasHolding_LAST"."OBJECT_ID_BUS";

--CREATE TABLE
CREATE TABLE "DQA"."ControlConsistenciaCuentasCoincidentesFastSF_R2"
(
    "ClEjecControl" bigint NOT NULL,
    "NameSF" text COLLATE pg_catalog."default",
    "BI_No_Identificador_fiscal__c" text COLLATE pg_catalog."default",
    "ParentId" text COLLATE pg_catalog."default",
    "BI_Id_del_cliente_BIEN__c" text COLLATE pg_catalog."default",
    "BI_Identificador_Externo__c" text COLLATE pg_catalog."default",
    "RecordType.Name" text COLLATE pg_catalog."default",
    "BI_Activo__c" text COLLATE pg_catalog."default",
    "max(Last_Account_Activity__c)" text COLLATE pg_catalog."default",
    "NAME" text COLLATE pg_catalog."default",
    "TAX_IDENT_NUMBER" text COLLATE pg_catalog."default",
    "PARENT_ID" bigint,
    "UNIQUE_GLOBAL_ID" text COLLATE pg_catalog."default",
    "STATUS" text COLLATE pg_catalog."default",
    "OBJECT_ID" text COLLATE pg_catalog."default",
    "CUST_TYPE" text COLLATE pg_catalog."default",
    "max(MODIFIED_WHEN)" timestamp with time zone,
    "CUSTOMER_ACCOUNT_TOP" text COLLATE pg_catalog."default",
    "NumVecesServAsocAlCustomer_TOP" bigint,
    "PARENT_ID_ORD" text COLLATE pg_catalog."default",
    "NumOrdeAsocAlParent_Id_ORD" bigint,
    CONSTRAINT "FK_ControlConsistenciaCuentasCoincidentesFastSF_R2" FOREIGN KEY ("ClEjecControl")
        REFERENCES "DQA"."EjecucionControl" ("ClEjecControl") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
