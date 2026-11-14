from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
print("LIGNES_LOGS =", spark.read.text("hdfs://namenode:8020/data/raw/logs/flows/*/*/flows.*").count())