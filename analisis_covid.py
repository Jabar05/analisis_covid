# Importar librerías necesarias
from pyspark.sql import SparkSession, functions as F
import time

# Inicializar sesión de Spark
spark = SparkSession.builder.appName("Analisis_COVID_Colombia").getOrCreate()

# Definir la ruta del archivo en HDFS
file_path = 'hdfs://localhost:9000/ruta/del/archivo/casos_covid.csv'

# Leer el archivo CSV desde HDFS
df = spark.read.format('csv') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .load(file_path)

# Mostrar esquema
df.printSchema()

# 1. Conteo por sexo
print("\nConteo por sexo:")
df.groupBy("sexo").count().show()

# 2. Conteo por departamento
print("\nConteo por departamento:")
df.groupBy("departamento").count().orderBy(F.col("count").desc()).show(10)

# 3. Fallecidos por sexo
print("\nFallecidos por sexo:")
df.filter(F.col("estado") == "Fallecido") \
  .groupBy("sexo").count().show()

# 4. Fallecidos por departamento
print("\nFallecidos por departamento:")
df.filter(F.col("estado") == "Fallecido") \
  .groupBy("departamento").count().orderBy(F.col("count").desc()).show(10)

# 5. Casos recuperados por departamento
print("\nRecuperados por departamento:")
df.filter(F.col("recuperado") == "Recuperado") \
  .groupBy("departamento").count().orderBy(F.col("count").desc()).show(10)

# Mantener sesión abierta 3 minutos
print("")
time.sleep(180)

spark.stop()