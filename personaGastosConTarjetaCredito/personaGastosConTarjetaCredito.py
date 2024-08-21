from pyspark.sql import SparkSession
import sys 
input_file = sys.argv[1]
output_file =  sys.argv[2]
# Crear una SparkSession
spark = SparkSession.builder.appName("TextFileRDDExample").getOrCreate()

# Obtener el SparkContext desde la SparkSession
datosentrada = spark.sparkContext.textFile(input_file)
mapped_rdd = datosentrada.map(lambda line: line.split(";")) \
                    .map(lambda parts: (parts[0], int(parts[2]) if parts[1] == "Tarjeta de crédito" else 0)) \
                    .reduceByKey(lambda a, b: a + b) \
                    .map(lambda pair: f"{pair[0]};{pair[1]}")


mapped_rdd.saveAsTextFile(output_file)
