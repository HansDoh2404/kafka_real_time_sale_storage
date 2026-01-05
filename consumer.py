from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType
from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC, S3_URL, S3_ACCESS_KEY, S3_SECRET_KEY


conf = SparkConf()
conf.set('spark.hadoop.fs.s3a.endpoint', f"http://{S3_URL}")
conf.set('spark.hadoop.fs.s3a.access.key', S3_ACCESS_KEY)
conf.set('spark.hadoop.fs.s3a.secret.key', S3_SECRET_KEY)
conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
conf.set('spark.hadoop.fs.s3a.path.style.access', 'true')
conf.set("spark.hadoop.fs.s3a.connection.timeout", "60000")
conf.set("spark.hadoop.fs.s3a.socket.timeout", "60000")
conf.set("spark.hadoop.fs.s3a.attempts.maximum", "5")
# conf.set('spark.jars', './jars/*') # --> au cas où les jars ont été télchargés
conf.set(
    "spark.jars.packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "org.apache.kafka:kafka-clients:3.6.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


spark = (
    SparkSession.builder
    .master("local[6]")
    .appName("SaleStream")
    .config(conf=conf)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


schema = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("payment_method", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("file_name", StringType(), True)
    ]
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", f"http://{KAFKA_BOOTSTRAP_SERVERS}")
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 5000)
    .load()
)

parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")
    

query = (
    parsed_df.writeStream
    .format("parquet")
    .option("path", "s3a://warehouse/sales")
    .option("checkpointLocation", "s3a://warehouse/checkpoints")
    .outputMode("append")
    .trigger(processingTime="10 second")
    .start()
)

query.awaitTermination()

