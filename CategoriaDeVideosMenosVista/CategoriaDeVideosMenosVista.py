from pyspark.sql import SparkSession
import sys
input_file = sys.argv[1]
output_file =  sys.argv[2]# Crear una SparkSession
spark = SparkSession.builder.appName("TextFileRDDExample").getOrCreate()

# Obtener el SparkContext desde la SparkSession
datosentrada = spark.sparkContext.textFile(input_file)

# Leer el archivo de texto y crear un RDD
filtered_rdd = datosentrada.map(lambda line: line.split("\t")) \
                           .filter(lambda parts: len(parts) >= 6) \
                           .map(lambda parts: (parts[3], int(parts[5]))) \
                           .reduceByKey(lambda a, b: a + b)

# Encontrar la categoría con el menor número de visualizaciones
menor_categoria = filtered_rdd.reduce(lambda a, b: a if a[1] < b[1] else b)

# Crear un nuevo RDD con el resultado formateado
resultado_rdd = spark.sparkContext.parallelize([menor_categoria]) \
                                  .map(lambda pair: f"{pair[0]};{pair[1]}")

# Guardar el RDD resultante en un archivo de salida
resultado_rdd.saveAsTextFile(output_file)

