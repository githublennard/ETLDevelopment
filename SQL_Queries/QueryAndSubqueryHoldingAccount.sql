SELECT      
         PADRE.OBJECT_ID SC_HOLDING_OBJECT_ID,
         PADRE.NAME DS_HOLDING_NAME,
         PADRE.STATUS DS_HOLDING_STATUS,
         PADRE.IS_MNC DS_HOLDING_IS_MNC,
         PADRE.IS_OTT DS_HOLDING_IS_OTT,
         PADRE.ACRONYM DS_HOLDING_ACRONYM,
         TIPO_PADRE.NAME DS_HOLDING_CUST_TYPE,
         HIJO.OBJECT_ID SC_LE_OBJECT_ID,
         HIJO.NAME DS_LE_NAME,
         HIJO.STATUS DS_LE_STATUS,
         HIJO.IS_MNC DS_LE_IS_MNC,
         HIJO.IS_OTT DS_LE_IS_OTT,
         HIJO.ACRONYM DS_LE_ACRONYM,
         TIPO_HIJO.NAME DS_LE_CUST_TYPE

FROM BUSINESS_CUST_ACC PADRE,

     NC_OBJECTS TIPO_PADRE,

          (SELECT R.PARENT_ID PADRE, R.REL_CUST HIJO    -- RELACIÓN PADRE-HIJOS

           FROM CUST_RELATION R

           WHERE R.REL_TYPE = 9137799819213569275     -- PARENT_OF       

             AND NOT EXISTS(SELECT 1 FROM CUST_RELATION R2                 --  El "No existe", Es verdadero SI NO Existe un "cliente|cuenta" con 

                                        WHERE R2.REL_CUST = R.PARENT_ID       -- estos dos campos iguales

                                       AND R2.REL_TYPE = 9137799819213569275)   -- y este campo igual al digito; por lo tanto si existe es Falso.
		   																		                                      -- Al ser Falso se descarta la fila y se avanza a la siguiente fila
																				                                        -- Es decir descarto todos los que son iguales en esos dos campos y tengan ese REL_TYPE	
           UNION

           SELECT R1.PARENT_ID PADRE, R2.REL_CUST HIJO       -- RELACIÓN PADRE-NIETOS

             FROM CUST_RELATION R1, CUST_RELATION R2

            WHERE     R1.REL_TYPE = 9137799819213569275  -- PARENT_OF  -- Esto ya viene PRE_FILTRADO desde el inicio de la lectura de la tabla CUST_RELATION
		   																                                 -- por lo tanto NO es necesario hacer un filtro por REL_TYPE
                  AND R2.REL_TYPE = 9137799819213569275  -- PARENT_OF

                  AND R1.REL_CUST = R2.PARENT_ID) ARBOL,

     BUSINESS_CUST_ACC HIJO,

     NC_OBJECTS TIPO_HIJO

WHERE PADRE.OBJECT_ID = ARBOL.PADRE  --OBJECT_ID de Buss y PARENT_ID de Arbol

      AND ARBOL.HIJO = HIJO.OBJECT_ID    --REL_CUST de Arbol y OBJECT_ID de Buss 

      AND PADRE.CUST_TYPE = TIPO_PADRE.OBJECT_ID(+)  --CUST_TYPE de Buss y OBJECT_ID de NC 

      AND HIJO.CUST_TYPE = TIPO_HIJO.OBJECT_ID(+)    --CUST_TYPE de Buss y OBJECT_ID de NC