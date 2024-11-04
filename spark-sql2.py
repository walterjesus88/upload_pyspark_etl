from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, explode, when
import os

spark = SparkSession.builder \
    .appName("CSV to SQL Server ETL") \
    .getOrCreate()  

file_path = "employees_complex.json"
if os.path.exists(file_path):
    print("El archivo existe. Ahora verificaremos su contenido:")
    with open(file_path, 'r', encoding='utf-8') as f:
        print(f.read(300))  # Imprime los primeros 300 caracteres para verificar el contenido
else:
    print("El archivo no existe en la ruta especificada.")

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("experience", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
    StructField("projects", ArrayType(
        StructType([
            StructField("project_name", StringType(), True),
            StructField("hours_week", IntegerType(), True)
        ])
    ), True),
    StructField("skills", ArrayType(StringType()), True)
])

# Cargar el archivo JSON con el esquema definido

#df =spark.read.json("employees_complex.json")
#df = spark.read.json("C:/data/upload_pyspark/data.json",mode="PERMISSIVE")
df = spark.read.schema(schema).json("C:/data/upload_pyspark/data.json", mode="PERMISSIVE")
# Mostrar los datos cargados
df.show()
df.printSchema()



# Filtrar registros corruptos (por ejemplo, donde 'projects' es nulo o no contiene estructuras adecuadas)
#cleaned_df = df.filter(col("projects").isNotNull() & (col("projects").getItem(0).isNotNull()))

# Explode para separar las filas de proyectos si existen
#exploded_df = cleaned_df.withColumn("project", explode("projects"))

# Seleccionar las columnas relevantes
# result_df = exploded_df.select(
#     "name",
#     "age",
#     "department",
#     "salary",
#     "project.hours_week",
#     "project.project_name"
# )

# Mostrar el DataFrame limpio y procesado
# result_df.show(truncate=False)



df.createOrReplaceTempView("employees")
query = """
    SELECT  name, size(skills) as num_skills
    FROM employees
    where size(skills) > 2
"""
result = spark.sql(query)
result.show()


spark.sql("""
    SELECT country, state, AVG(salary) as avg_salary
    FROM employees
    GROUP BY country, state
""").show()


from pyspark.sql.functions import explode

# Explotar el array "projects" para tener una fila por proyecto
# name, project, horas en el proyecto
projects_df = df.select("name", explode("projects").alias("project"))
projects_df.select("name", "project.project_name", "project.hours_week").show()

from pyspark.sql.functions import array_contains
#filtrar por skills 
df.filter(array_contains(df["skills"], "Python")).select("name", "skills").show()


# Obtener empleados que trabajen más de 20 horas por semana en al menos un proyecto:
projects_df.createOrReplaceTempView("employee_projects")
spark.sql("""
    SELECT DISTINCT name
    FROM employee_projects
    WHERE project.hours_week > 20
""").show()


#Consulta combinada con funciones de agregación y filtros:
spark.sql("""
    SELECT department, country, COUNT(name) as num_employees
    FROM employees
    WHERE salary > 50000
    GROUP BY department, country
""").show()
