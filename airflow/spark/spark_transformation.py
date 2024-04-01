#!/usr/bin/env python
# coding: utf-8

import argparse
import os

import pyspark

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col


parser = argparse.ArgumentParser()

parser.add_argument('--input_table', required=True)
parser.add_argument('--output')
args = parser.parse_args()
input_table = 'gs://ucl-premier-417919-bucket/par/part-00000-4b684bf2-1fdf-4fc9-a102-62b090bd6502-c000.snappy.parquet'
output = args.output


credentials_location = "/home/bryan/.google/credentials/google_credentials.json"

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "../lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()



spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-west1-245998250410-jigg8mxm')


df_player_par = spark.read.parquet('gs://ucl-premier-417919-bucket/raw/player_stats.parquet')


df_player_par.show()


df_player_par.printSchema()


df_player_par = df_player_par.withColumn("player", col("player").cast("string")) \
       .withColumn("club", col("club").cast("string"))



df_player_par = df_player_par.withColumn("value", col("value").cast("float"))



df_player_par.printSchema()



output_path = 'gs://ucl-premier-417919-bucket/par/'
df_player_par.coalesce(1).write.parquet(output_path,mode='overwrite')
# df_green.write.parquet('gs://ucl-premier-417919-bucket/pq/')


table_ref = 'premier-predict-417919.ucl_data.updated_types'
df_player_table = spark.read.format('bigquery') \
    .option("table", table_ref) \
    .load()
df_player_table.createOrReplaceTempView('df_player_table')





