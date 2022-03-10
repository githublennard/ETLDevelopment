# Ejecucion todos los controles de dq
03 3 * * * /home/dq/Datawall_Master/dq/control_total.sh > /home/dq/crontab_logs/datawall/salida_cadena-`date +\%Y\%m\%d` 2>&1

# Ejecucion controles Vendor
32 * * * * /home/dq/Datawall_Master/dq/run_in_dq.sh /home/dq/Datawall_Master/dq/Control_VendorFAST_V2.py salida_Vendor.txt >> /home/dq/crontab_logs/vendor/salida_vendor-`date +\%Y\%m\%d` 2>&1
#51 * * * * /home/dq/Datawall_Master/dq/run_in_dq.sh /home/dq/Datawall_Master/dq/Control_VendorFAST_V2.py salida_Vendor.txt >> /home/dq/crontab_logs/vendor/salida_vendor-`date +\%Y\%m\%d` 2>&1
25 * * * * /home/dq/Datawall_Master/dq/run_in_dq.sh /home/dq/Datawall_Master/dq/Control_Telefonica.py salida_Vendor1.txt >> /home/dq/crontab_logs/vendor/salida_vendor-`date +\%Y\%m\%d` 2>&1


# Ejecucion extraccion de Salesforce
00 2 * * * /usr/bin/python36 /home/dq/Datawall_Master/dq/SF_extraction.py > /home/dq/crontab_logs/ExtraccionSF/salida_ExtractionSF`date +\%Y\%m\%d`.txt 2>&1

# Remove data from ControlTicketsRemedySF every 7 days.
# 30 18 * * * python /home/dq/Datawall_Master/dq/removeDataFromDBXDays.py ControlTicketsRemedySF 7 >> /home/dq/crontab_logs/datawall/salida_cadena-`date +\%Y\%m\%d` 2>&1 

# Programar el borrado automatico de la base de datos.
0 19 16 * * /home/dq/Datawall_Master/dq/run_in_dq.sh /home/dq/Datawall_Master/dq/purgadoBaseDatos.py /home/dq/crontab_logs/datawall/purgado-`date +\%Y\%m\%d`.txt 