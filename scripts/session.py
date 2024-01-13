from pyspark.sql import SparkSession

def create_spark_session(num_executors):
   
   spark = SparkSession.builder \
        .appName("ADV DATABASES") \
        .config("spark.executor.instances", str(num_executors)) \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.cleaner.periodicGC.interval", "1min") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "1g") \
        .config("spark.default.parallelism", "4") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "2") \
        .config("spark.dynamicAllocation.maxExecutors", "4") \
        .getOrCreate()
      
   return spark


