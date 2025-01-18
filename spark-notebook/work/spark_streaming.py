from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName('Streaming data for word counting') \
    .master('spark://spark-master:7077') \
    .config("spark.executor.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config('spark.sql.warehouse.dir', f's3a://lakehouse/') \
    .enableHiveSupport() \
    .getOrCreate()

## TODO: create the source for reading stream
lines = spark.readStream.format("socket")\
            .option("host", "160.191.244.13")\
            .option("port", "9999")\
            .load()

## TODO: DataFrane operations
words_df = lines.select(split(col("value"), "\\s").alias("word"))
counts_df = words_df.groupBy("word").count()

## TODO: create checkpoint folder to save metadata of each batch (stream) -> fault tolerance - consistency (read once)
checkpoint_path = "s3a://lakehouse/streaming/test/checkpoint"

## TODO: create the sink for writing to delta
streamingQuery = counts_df.writeStream\
                        .format("delta")\
                        .outputMode("complete")\
                        .trigger(processingTime="1 second")\
                        .option("path", "s3a://lakehouse/streaming/test/wordcount")\
                        .option("checkpointlocation", checkpoint_path)\
                        .start()

# ## TODO: create the sink for writing to console
# streamingConsole = counts_df.writeStream\
#                             .format("console")\
#                             .outputMode("complete")\
#                             .option("checkpointlocation", checkpoint_path)\
#                             .trigger(processingTime="1 second")\
#                             .start()

streamingQuery.awaitTermination()
# streamingConsole.awaitTermination()