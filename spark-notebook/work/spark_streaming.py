from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName('Streaming data for word counting') \
    .master('spark://spark-master:7077') \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config('spark.sql.warehouse.dir', f's3a://lakehouse/') \
    .enableHiveSupport() \
    .getOrCreate()
    
lines = spark.readStream.format("socket")\
            .option("host", "160.191.244.13")\
            .option("port", "9999")\
            .load()
            
words_df = lines.select(split(col("value"), "\\s").alias("word"))
counts_df = words_df.groupBy("word").count()


checkpoint_path = "s3a://lakehouse/streaming/test/checkpoint"


streamingQuery = counts_df.writeStream\
                        .format("console")\
                        .outputMode("complete")\
                        .trigger(processingTime="1 second")\
                        .option("checkpointlocation", checkpoint_path)\
                        .start()

streamingQuery.awaitTermination()