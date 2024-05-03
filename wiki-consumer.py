from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, unix_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, BooleanType, LongType, IntegerType, StringType

# Spark session & context
spark = (SparkSession
         .builder
         .master('local')
         .appName('wiki-changes-event-consumer')
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
         .getOrCreate())
sc = spark.sparkContext

# Create stream dataframe setting kafka server, topic and offset option
df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")  # Make sure this is correct
      .option("subscribe", "wiki-changes")
      .load())


# Convert binary to string key and value
df1 = (df
    .withColumn("key", df["key"].cast(StringType()))
    .withColumn("value", df["value"].cast(StringType())))

# Event data schema
schema_wiki = StructType(
    [StructField("$schema",StringType(),True),
     StructField("bot",BooleanType(),True),
     StructField("comment",StringType(),True),
     StructField("id",StringType(),True),
     StructField("length",
                 StructType(
                     [StructField("new",IntegerType(),True),
                      StructField("old",IntegerType(),True)]),True),
     StructField("meta",
                 StructType(
                     [StructField("domain",StringType(),True),
                      StructField("dt",StringType(),True),
                      StructField("id",StringType(),True),
                      StructField("offset",LongType(),True),
                      StructField("partition",LongType(),True),
                      StructField("request_id",StringType(),True),
                      StructField("stream",StringType(),True),
                      StructField("topic",StringType(),True),
                      StructField("uri",StringType(),True)]),True),
     StructField("minor",BooleanType(),True),
     StructField("namespace",IntegerType(),True),
     StructField("parsedcomment",StringType(),True),
     StructField("patrolled",BooleanType(),True),
     StructField("revision",
                 StructType(
                     [StructField("new",IntegerType(),True),
                      StructField("old",IntegerType(),True)]),True),
     StructField("server_name",StringType(),True),
     StructField("server_script_path",StringType(),True),
     StructField("server_url",StringType(),True),
     StructField("timestamp",StringType(),True),
     StructField("title",StringType(),True),
     StructField("type",StringType(),True),
     StructField("user",StringType(),True),
     StructField("wiki",StringType(),True)])

# Create dataframe setting schema for event data
df_wiki = (df1
           # Sets schema for event data
           .withColumn("value", from_json("value", schema_wiki))
          )

from pyspark.sql.functions import col, from_unixtime, to_date, to_timestamp

# Transform into tabular 
# Convert unix timestamp to timestamp
# Create partition column (change_timestamp_date)
df_wiki_formatted = (df_wiki.select(
    col("key").alias("event_key")
    ,col("topic").alias("event_topic")
    ,col("timestamp").alias("event_timestamp")
    ,col("value.$schema").alias("schema")
    ,"value.bot"
    ,"value.comment"
    ,"value.id"
    ,col("value.length.new").alias("length_new")
    ,col("value.length.old").alias("length_old")
    ,"value.minor"
    ,"value.namespace"
    ,"value.parsedcomment"
    ,"value.patrolled"
    ,col("value.revision.new").alias("revision_new")
    ,col("value.revision.old").alias("revision_old")
    ,"value.server_name"
    ,"value.server_script_path"
    ,"value.server_url"
    ,to_timestamp(from_unixtime(col("value.timestamp"))).alias("change_timestamp")
    ,to_date(from_unixtime(col("value.timestamp"))).alias("change_timestamp_date")
    ,"value.title"
    ,"value.type"
    ,"value.user"
    ,"value.wiki"
    ,col("value.meta.domain").alias("meta_domain")
    ,col("value.meta.dt").alias("meta_dt")
    ,col("value.meta.id").alias("meta_id")
    ,col("value.meta.offset").alias("meta_offset")
    ,col("value.meta.partition").alias("meta_partition")
    ,col("value.meta.request_id").alias("meta_request_id")
    ,col("value.meta.stream").alias("meta_stream")
    ,col("value.meta.topic").alias("meta_topic")
    ,col("value.meta.uri").alias("meta_uri")
))

df_wiki_formatted = df_wiki_formatted.withWatermark("change_timestamp", "5 minutes")
current_time = unix_timestamp(current_timestamp())
five_minutes_ago = expr("current_timestamp() - interval 5 minutes")

filtered_titles = df_wiki_formatted.filter(col("change_timestamp") >= five_minutes_ago)

# Aggregate data to find trending titles
# trending_titles = (filtered_titles
#                    .groupBy("title")
#                    .count()
#                    .withColumnRenamed("count", "title_count")
#                    .orderBy(col("title_count").desc()))


# Write the sorted and limited stream to a console sink
# query = (trending_titles
#          .writeStream
#          .outputMode("complete")
#          .format("parquet")
#          .option("path", "output")
#          .option("checkpointLocation", "checkpoint")
#          .start())

query =(
    filtered_titles
    .writeStream
    .format("parquet")
    .queryName("wiki_changes")
    .option("checkpointLocation", "checkpoint")
    .option("path", "output")
    .outputMode("append")
    .partitionBy("change_timestamp_date")
    .start())

query.awaitTermination()

