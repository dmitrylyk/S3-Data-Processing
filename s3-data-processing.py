from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = ""

S3_PATH = f's3a://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{AWS_BUCKET_NAME}/'

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("Streaming App") \
    .getOrCreate()

sc = spark.sparkContext

ssc = StreamingContext(sc, 30)

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", ACCESS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", SECRET_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

jsonSchema = StructType([StructField("user", StringType(), True),
                         StructField("timestamp", TimestampType(), True),
                         StructField("spend", DoubleType(), True),
                         StructField("evtname", StringType(), True)])

revenueSchema = StructType([StructField("user", StringType(), True),
                            StructField("total_spend", DoubleType(), True)])

revenueCache = spark.createDataFrame(spark.sparkContext.emptyRDD(), revenueSchema)


def processing(time, rdd):
    df = spark.read \
        .schema(jsonSchema) \
        .json(rdd)

    if len(df.head(1)) == 0:
        print(str(time), "(Empty Batch)")
    else:
        print(str(time))
        df.show()

        events = df.drop("spend")

        totalSpend = df.na.fill(0, ["spend"]) \
            .groupby("user") \
            .sum("spend") \
            .withColumnRenamed("sum(spend)", "total_spend") \
            .withColumn("total_spend", round(col("total_spend"), 2))

        global revenueCache

        revenue = revenueCache.union(totalSpend) \
            .groupby("user") \
            .sum("total_spend") \
            .withColumnRenamed("sum(total_spend)", "total_spend") \
            .withColumn("total_spend", round(col("total_spend"), 2))

        revenueCache = revenue

        events.show()
        revenue.show()

        events.write \
            .format("jdbc") \
            .mode("append") \
            .option("driver", 'org.postgresql.Driver') \
            .option("url", "") \
            .option("dbtable", "users") \
            .option("user", "") \
            .option("password", "") \
            .save()

        revenue.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("driver", 'org.postgresql.Driver') \
            .option("url", "") \
            .option("dbtable", "revenue") \
            .option("user", "") \
            .option("password", "") \
            .save()


stream = ssc.textFileStream(S3_PATH)

stream.foreachRDD(processing)

ssc.start()
ssc.awaitTermination()
