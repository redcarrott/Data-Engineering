#!/usr/bin/env python
"""Extract events from kafka, filter, transform and write to hdfs
"""
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@udf('boolean')
def is_purchase_ingredient(event_as_json):
    """udf for filtering purchase_an_ingredient events
    """
    event = json.loads(event_as_json)
    if event['event_type'] == 'purchase_ingredient':
        return True
    return False

@udf('boolean')
def is_join_restaurant(event_as_json):
    """udf for filtering join_a_restaurant events
    """
    event = json.loads(event_as_json)
    if event['event_type'] == 'join_restaurant':
        return True
    return False

@udf('boolean')
def is_enter_contest(event_as_json):
    """udf for filtering enter_contest events
    """
    event = json.loads(event_as_json)
    if event['event_type'] == 'enter_contest':
        return True
    return False

def post_schema():
    """define schema for post events
    """
    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("Content-Length", StringType(), True),
        StructField("Content-Type", StringType(), True),
        StructField("attributes", StringType(), True)
    ])

def contest_attr_schema():
    """define schema for attributes of enter_a_contest events
    """
    return StructType([
        StructField("contest", StringType(), True),
        StructField("outcome", StringType(), True)
    ])

def restaurant_attr_schema():
    """define schema for attributes of join_a_restaurant events
    """
    return StructType([
        StructField("restaurant_name", StringType(), True),
        StructField("special_ingredient", StringType(), True)
    ])

def ingredient_attr_schema():
    """define schema for attributes of purchase_an_ingredient events
    """
    return StructType([
        StructField("ingredient_type", StringType(), True),
        StructField("gold_cost", IntegerType(), True)
    ])
        
def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .getOrCreate()

    raw_events = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .load()

    purchase_ingredient = raw_events \
        .filter(is_purchase_ingredient(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          post_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')
    
    spark.sql("drop table if exists default.events")
    sql_string = """
        create external table if not exists default.events (
            raw_event string,
            timestamp string,
            Accept string,
            Host string,
            `User-Agent` string,
            event_type string
            )
            stored as parquet
            location '/tmp/events'
            tblproperties ("parquet.compress"="SNAPPY")
            """
    spark.sql(sql_string)
    
    sink_purchase_ingredient = purchase_ingredient \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_purchase_ingredient") \
        .option("path", "/tmp/purchase_ingredient") \
        .trigger(processingTime="10 seconds") \
        .start()
        
    join_restaurant = raw_events \
        .filter(is_join_restaurant(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          post_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')
    
    spark.sql("drop table if exists default.events")
    sql_string = """
        create external table if not exists default.events (
            raw_event string,
            timestamp string,
            Accept string,
            Host string,
            `User-Agent` string,
            event_type string
            )
            stored as parquet
            location '/tmp/events'
            tblproperties ("parquet.compress"="SNAPPY")
            """
    spark.sql(sql_string)
    
    sink_join_restaurant = join_restaurant \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_join_restaurant") \
        .option("path", "/tmp/join_restaurant") \
        .trigger(processingTime="10 seconds") \
        .start()
         
    enter_contest = raw_events \
        .filter(is_enter_contest(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          post_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')
    
    spark.sql("drop table if exists default.events")
    sql_string = """
        create external table if not exists default.events (
            raw_event string,
            timestamp string,
            Accept string,
            Host string,
            `User-Agent` string,
            event_type string
            )
            stored as parquet
            location '/tmp/events'
            tblproperties ("parquet.compress"="SNAPPY")
            """
    spark.sql(sql_string)
    
    sink_enter_contest = enter_contest \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_enter_contest") \
        .option("path", "/tmp/enter_contest") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    sink_enter_contest.awaitTermination()
    sink_join_restaurant.awaitTermination()
    sink_purchase_ingredient.awaitTermination()

if __name__ == "__main__":
    main()