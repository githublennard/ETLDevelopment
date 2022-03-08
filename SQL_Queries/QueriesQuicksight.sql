--HistoricoTablasPostgreSQL_PRO
SELECT hts.*,DATE(ejc."Fecha") as "Fecha" 
FROM "DQA"."HistoricoTablasPostgreSQLSize" hts JOIN "DQA"."EjecucionControl" ejc 
ON ejc."ClEjecControl"= hts."ClEjecControl"
AND ejc."Fecha" >= (CURRENT_DATE - 180)  
ORDER BY "Fecha" asc

--HistoricoDBSize_PRO
SELECT DISTINCT CAST(hdb."SizeDBInGB" AS integer), DATE(ejc."Fecha") AS "Fecha" 
FROM "DQA"."HistoricoDBSize" hdb JOIN "DQA"."EjecucionControl" ejc 
ON ejc."ClEjecControl" = hdb."ClEjecControl" AND ejc."Fecha" >= (CURRENT_DATE - 180)  
ORDER BY "Fecha" asc

--ControlConsistenciaCuentasCoincidentesFastSF_R1_PRO
SELECT cccc.*, date(ejc."Fecha") as "Fecha",
case 
when cccc."BI_Identificador_Externo__c" != cccc."OBJECT_ID" OR cccc."BI_Identificador_Externo__c" IS NULL OR cccc."OBJECT_ID" IS NULL  then 'SiR1_1'--No Coincidencia en sentido contrario
else 'NoR1_1' 
end as "R1_1",
case 
when cccc."BI_Identificador_Externo__c" = cccc."OBJECT_ID" then 'SiR1_2'--Coincidencia en sentido contrario
else 'NoR1_2' 
end as "R1_2",
case 
when cccc."NameSF" != cccc."NAME" then 'SiR1_3'--No Coinciden en Nombre
else 'NoR1_3' 
end as "R1_3",
case
when cccc."RecordType.Name" != cccc."CUST_TYPE" then 'SiR1_4'--No Coinciden en Jerarquia
else 'NoR1_4' 
end as "R1_4",
case
when cccc."TAX_IDENT_NUMBER" LIKE 'ACCT%' then 'SiTelxius'--No Coinciden en Jerarquia
else 'NoTelxius' 
end as "EsTelxius"
FROM "DQA"."ControlConsistenciaCuentasCoincidentesFastSF_R1" cccc
JOIN "DQA"."EjecucionControl" ejc
ON cccc."ClEjecControl" = ejc."ClEjecControl"
AND cccc."ClEjecControl" = (select MAX(cccc."ClEjecControl") from "DQA"."ControlConsistenciaCuentasCoincidentesFastSF_R1" cccc)
ORDER BY cccc."NAME" asc

--ControlConsistenciaCuentasCoincidentesFastSF_R1_Historico_PRO
SELECT cccc.*,date(ejc."Fecha") as "Fecha",
case 
when cccc."BI_Identificador_Externo__c" != cccc."OBJECT_ID" OR cccc."BI_Identificador_Externo__c" IS NULL OR cccc."OBJECT_ID" IS NULL  then 'SiR1_1'--No Coincidencia en sentido contrario
else 'NoR1_1' 
end as "R1_1",
case 
when cccc."BI_Identificador_Externo__c" = cccc."OBJECT_ID" then 'SiR1_2'--Coincidencia en sentido contrario
else 'NoR1_2' 
end as "R1_2",
case 
when cccc."NameSF" != cccc."NAME" then 'SiR1_3'--No Coinciden en Nombre
else 'NoR1_3' 
end as "R1_3",
case
when cccc."RecordType.Name" != cccc."CUST_TYPE" then 'SiR1_4'--No Coinciden en Jerarquia
else 'NoR1_4' 
end as "R1_4",
case
when cccc."TAX_IDENT_NUMBER" LIKE 'ACCT%' then 'SiTelxius'--No Coinciden en Jerarquia
else 'NoTelxius' 
end as "EsTelxius"
FROM "DQA"."ControlConsistenciaCuentasCoincidentesFastSF_R1" cccc
JOIN "DQA"."EjecucionControl" ejc
ON cccc."ClEjecControl" = ejc."ClEjecControl"
ORDER BY cccc."NAME" asc

--ControlClientesCompartidosTelxius_PRO
select ccct.*, date(ejc."Fecha") as "Fecha" from "DQA"."ControlClientesCompartidosTelxius" ccct JOIN "DQA"."EjecucionControl" ejc
on ccct."ClEjecControl" = ejc."ClEjecControl"
where ccct."ClEjecControl" = (select MAX(ccct."ClEjecControl") from "DQA"."ControlClientesCompartidosTelxius" ccct)
order by ccct."NAME" asc

--ControlClientesCompartidosTelxiusHistorico_PRO
select ccct.*, date(ejc."Fecha") as "Fecha" from "DQA"."ControlClientesCompartidosTelxius" ccct JOIN "DQA"."EjecucionControl" ejc
on ccct."ClEjecControl" = ejc."ClEjecControl"
AND ejc."Fecha" >= (CURRENT_DATE - 180)  
ORDER BY "Fecha" asc