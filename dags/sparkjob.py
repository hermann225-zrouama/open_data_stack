from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession \
        .builder \
        .appName("ETL") \
        .config("spark.jars.packages",
            "org.apache.spark:spark-hadoop-cloud_2.12:3.3.2,org.postgresql:postgresql:42.2.20") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000") \
        .config('spark.hadoop.fs.s3a.access.key', 'EkshoGX3tVjmQt5z') \
        .config('spark.hadoop.fs.s3a.secret.key', 'V6iOTv6LBamTyyLXszMKhJfnaGpfxbWj') \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.com.amazonaws.services.s3.enableV4", "true") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+PrintGCDetails -XX:+PrintGCDateStamps") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:+PrintGCDetails -XX:+PrintGCDateStamps") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.maxExecutors", "4") \
        .config("spark.dynamicAllocation.minExecutors", "1") \
        .getOrCreate()


df = spark.read.parquet('s3a://dev-bucket/storage/green_taxi/', inferSchema=True)

df_agg = (
        df.filter(
                F.col('tolls_amount') > 0
        ).groupBy(
                F.col('DOLocationID')
        ).agg(
                F.avg('trip_distance').alias('avg_distance')
        ).orderBy('DOLocationID')
)

mode = "overwrite"
url = "jdbc:postgresql://warehouse:5432/admin"
properties = {"user": "admin","password": "pg_admin","driver": "org.postgresql.Driver"}
df_agg.write.option('truncate', 'true').jdbc(url=url, table="aggregations.tolls_avg_distance", mode='overwrite', properties=properties)