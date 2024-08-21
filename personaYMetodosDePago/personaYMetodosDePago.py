from pyspark.sql import SparkSession
import sys
input_file = sys.argv[1]
salida1 =  sys.argv[2]# Crear una SparkSession
salida2 =  sys.argv[3]# Crear una SparkSession

# Crear una SparkSession
spark = SparkSession.builder.appName("TextFileRDDExample").getOrCreate()

# Obtener el SparkContext desde la SparkSession
datosentrada =spark.sparkContext.textFile(input_file)



#.filter(lambda line: "Tarjeta de crédito" in line) \
# Paso 1: Filtrar transacciones con tarjeta de crédito y mapear para obtener pares (persona, dinero_gastado)
mapped_rdd = datosentrada.filter(lambda line: "Tarjeta de crédito" not in line) \
               .map(lambda line: line.split(";")) \
                .map(lambda parts: (parts[0], int(parts[2])))

# Mostrar los resultados


# Define el umbral
umbral = 1500
# Modificar los valores basado en el umbral
result_rdd_1 =  mapped_rdd.map(lambda x: (x[0], 1 if x[1] > umbral else 0)).reduceByKey(lambda a, b: a + b).map(lambda pair: f"{pair[0]};{pair[1]}")

result_rdd_1.saveAsTextFile(salida1)

result_rdd_2 =  mapped_rdd.map(lambda x: (x[0], 1 if x[1] <= umbral else 0)).reduceByKey(lambda a, b: a + b).map(lambda pair: f"{pair[0]};{pair[1]}")


result_rdd_2.saveAsTextFile(salida2)
