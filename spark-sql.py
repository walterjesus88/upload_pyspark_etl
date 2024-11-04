from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("CSV to SQL Server ETL") \
    .getOrCreate()
    
    #.config("spark.driver.extraClassPath", "C:\\spark\\spark-3.3.2-bin-hadoop2\\jars\\*") \
    #.config("spark.executor.extraLibraryPath", "C:\\spark\\spark-3.3.2-bin-hadoop2\\auth\\x64\\sqljdbc_auth.dll") \
    #.config("spark.driver.extraJavaOptions", "-Djava.library.path=C:\\spark\\spark-3.3.2-bin-hadoop2\\auth\\x64") \

df =spark.read.json("employee.json")

df.show()

df_selected = df.select("name", "age")
df_selected.show()

# Registrar el DataFrame como una tabla SQL temporal
df.createOrReplaceTempView("employees")

# Ejecutar una consulta SQL
result = spark.sql("SELECT name, age FROM employees")
result.show()

query = """
    SELECT department, COUNT(*) as num_employees
    FROM employees
    GROUP BY department
"""
result = spark.sql(query)
result.show()