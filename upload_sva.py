from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Crea una sesión de Spark
# spark = SparkSession.builder \
#     .appName("CSV to SQL Server ETL") \
#     .config("spark.driver.extraClassPath", "C:\\spark\\spark-3.3.2-bin-hadoop2\\jars\\*") \
#     .config("spark.executor.extraLibraryPath", "C:\\spark\\spark-3.3.2-bin-hadoop2\\auth\\x64\\sqljdbc_auth.dll") \
#     .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:\\spark\\spark-3.3.2-bin-hadoop2\\auth\\x64") \
#     .getOrCreate()


os.environ["java.library.path"] = r"C:\\spark\\spark-3.3.2-bin-hadoop2\\auth\\x64\\sqljdbc_auth.dll"

print("java.library.path:", os.environ.get('java.library.path'))

# Configuración de SparkSession
spark = SparkSession.builder \
    .appName("PySparkSQLServer") \
    .config("spark.driver.extraClassPath", "C:\\spark\\spark-3.3.2-bin-hadoop2\\jars\\mssql-jdbc-12.8.1.jre8") \
    .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:\\spark\\spark-3.3.2-bin-hadoop2\\auth\\x64") \
    .config("spark.driver.extraLibraryPath", os.environ["java.library.path"]) \
    .getOrCreate()

# Configuración
csv_path = "c:/data/upload_pyspark/DATA_SVA_TV_APP_MAX.txt"  # Cambia a la ruta de tu archivo CSV
table_name = "sva_max_fuente"  # Cambia al nombre de la tabla en SQL Server
#jdbc_url = "jdbc:sqlserver://2xx.121.226.15x;encrypt=false;databaseName=BACKUP_PRUEBAS_WR"  # Cambia a tu configuración
jdbc_url = "jdbc:sqlserver://WRIVERA-LP1;encrypt=false;databaseName=sva"
#jdbc_url = "jdbc:sqlserver://WRIVERA-LP1;instanceName=user;encrypt=false;databaseName=sva;integratedSecurity=true;loginTimeout=30"

jdbc_properties = {
    "user": "walter",
    "password": "walter23",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Paso 1: Extracción - Leer el CSV

# URL JDBC para SQL Server en localhost con autenticación de Windows (integrada)

# Propiedades JDBC (sin usuario y contraseña para autenticación automática)
# jdbc_properties = {
#     "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
#     #"authenticationScheme": "JavaKerberos"
# }



# Leer el archivo usando Spark y asignar los nombres de las columnas
column_names = [
    'ASSIGNED_PRODUCT_KEY', 'ORDER_ACTION_KEY', 'CUSTOMER_KEY', 'ASSIGNED_PRODUCT_ID', 'SUBSCRIBER_KEY', 
    'ETL_CRD_DT', 'ETL_UPD_DT', 'SUB_MAIN_OFFER_KEY', 'PRODUCT_KEY', 'VIRT_MOD_VALUE', 'MAIN_COMPONENT_KEY', 
    'SERVICE_ID', 'SVA_FECHA_ALTA', 'SVA_CODIGO', 'SVA_DESC', 'SVA_PRECIO', 'DOC_TIPO', 'DOC_NUMERO', 'NOMBRES', 
    'APELLIDOS', 'ORDER_KEY', 'ORDER_START_DATE', 'ORDER_STATUS_DATE', 'SVA_FECHA_VENTA', 'SVA_MES_VENTA', 
    'SVA_MES_ALTA', 'SVA_MES_ACTIVACION', 'ORDER_ACTION_TYPE_KEY', 'ORDER_ACTION_TYPE_DESC', 'SALES_CHANNEL_KEY', 
    'SALES_CHANNEL_NAME', 'ORDER_CREATOR', 'AGENT_KEY', 'AGENTE_DOCUMENTO', 'PUNTO_VENTA', 'SOCIO', 'CANAL_VENTA', 
    'NRO_ORDEN', 'FLAG_REPETIDO', 'FLAG_INT', 'FLAG_TV', 'PRODUCTO_GRUPO', 'PRODUCTO_FECHA_ALTA', 
    'PRODUCTO_STATUS_DESC', 'CICLO', 'FSEG', 'INT_SUBSCRIBER_KEY', 'INT_SERVICE_ID', 'INT_TECNOLOGIA', 
    'TV_SUBSCRIBER_KEY', 'TV_SERVICE_ID', 'TV_SERVICE_TECHNOLOGY', 'PRODUCTO_RENTA_TOTAL', 'TELEFONO', 
    'DEPARTAMENTO', 'PROVINCIA', 'DISTRITO', 'TROBA', 'TV_FECHA_ALTA', 'INT_FECHA_ALTA', 'MIBID', 'USERUNIQUEID', 
    'SVA_FECHA_ACTIVACION', 'STATUS_GVP', 'CORREOGVP', 'SUBSCRIPTIONS', 'CNT_DIAS_ACTIVACION', 'FLAG_DIAS', 
    'FLAG_PRUEBA', 'PERFIL_RIESGO', 'FLAG_FRAUDE', 'TELEFONO_MOVISTAR_1', 'TELEFONO_MOVISTAR_2', 
    'TELEFONO_MOVISTAR_3', 'TELEFONO_MOVISTAR_4', 'TELEFONO_COMPETENCIA_1', 'TELEFONO_COMPETENCIA_2', 
    'TELEFONO_COMPETENCIA_3', 'TELEFONO_COMPETENCIA_4', 'FLAG_COMPRA', 'FLAG_ACTIVACION', 'GRUPO', 'CANAL_AGRUPADO', 
    'INTERVALO_DIAS', 'HERRAMIENTA_VENTA', 'Correo_Contacto', 'BILLING_ARRANGEMENT_KEY', 'FINANCIAL_ACCOUNT_KEY', 
    'TIPO_RENTA', 'PRODUCT_KEY_ABO', 'ASGN_BLNG_OFR_STATUS_KEY', 'ASGN_BLNG_OFR_STATE_KEY', 
    'ASSIGNED_BILLING_VERSION_ID', 'FLAG_ABO', 'CASCADA_FINAL', 'FECHA_CARGA', 'CLUSTER_', 'COBERTURA_PANGEA', 
    'FLAG_PANGEA', 'FLAG_HBO', 'flag_fecha', 'TENENCIA_PLANTA', 'SEGMENTO_FINAL', 'SEGMENTO_MARGEN', 'FLAG_APP_MM', 
    'FLAG_FECHA_ALTA'
]

# Convertir todos los nombres de columna a minúsculas
column_names = [col_name.lower() for col_name in column_names]

# Leer el archivo en un DataFrame de PySpark
df = spark.read.option("header", True).option("delimiter", "|").csv(csv_path)
#df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Asignar nombres de columnas en minúsculas al DataFrame
# for i, col_name in enumerate(column_names):
#     df = df.withColumnRenamed(f"_c{i}", col_name)

# Seleccionar solo las columnas necesarias y renombrarlas a minúsculas
selected_columns = ["assigned_product_key","order_action_key","customer_key", "assigned_product_id","subscriber_key","etl_crd_dt","etl_upd_dt","sub_main_offer_key",
                    "product_key","virt_mod_value","main_component_key","service_id","sva_codigo","sva_desc","sva_precio",
                   "order_start_date","order_status_date","sva_fecha_venta","sva_fecha_alta","sva_mes_venta","sva_mes_alta","sva_mes_activacion",
                   	"doc_tipo","doc_numero","nombres","apellidos","order_key","order_action_type_key","order_action_type_desc",
                   	"sales_channel_key","sales_channel_name","order_creator","agent_key","agente_documento","punto_venta","socio",                           
                   	"canal_venta","producto_grupo","producto_fecha_alta","producto_status_desc","ciclo","fseg","int_subscriber_key","int_service_id","int_tecnologia",
                   	"tv_subscriber_key","tv_service_id","tv_service_technology","producto_renta_total","telefono",
                   	"departamento","provincia","distrito","troba","tv_fecha_alta","int_fecha_alta","mibid","useruniqueid",
                   	"sva_fecha_activacion","status_gvp","correogvp","subscriptions","cnt_dias_activacion","telefono_movistar_1","telefono_movistar_2",
                   	"telefono_movistar_3","telefono_movistar_4","telefono_competencia_1","telefono_competencia_2",
                   	"telefono_competencia_3","telefono_competencia_4","correo_contacto",                    
                    "billing_arrangement_key","financial_account_key","product_key_abo","asgn_blng_ofr_status_key",
					"asgn_blng_ofr_state_key","assigned_billing_version_id","nro_orden","flag_abo","flag_compra","flag_dias","flag_fraude","flag_prueba","flag_repetido",
                    "flag_int","flag_tv","flag_activacion","perfil_riesgo","intervalo_dias","herramienta_venta","cascada_final","canal_agrupado","tipo_renta","grupo","fecha_carga"
                   ] 
               

df_selected = df.select([col(column) for column in selected_columns])

# # Mostrar el resultado o guardarlo en otro archivo
df_selected.show()

# Paso 2: Transformación - Aplica transformaciones necesarias
# Ejemplo de transformación: Filtrar filas con valores nulos en una columna específica
#df = df.filter(col("nombre_columna").isNotNull())
# df = df.toDF(*[c.lower() for c in df.columns])
# print(df.columns)

# Paso 3: Carga - Escribe en SQL Server
df_selected.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=jdbc_properties)

# print("Carga completada.")
spark.stop()
