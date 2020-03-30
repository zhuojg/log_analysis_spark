from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
import pandas as pd
from pyspark.sql.functions import to_date, to_timestamp
import pyhdfs
import os
import time


# Configuration
spark = SparkSession.builder.master('local').appName("WordCount").getOrCreate()

es_nodes = "localhost:9200" # where elasticsearch service is, default is localhost:9200
es_index = 'waf/logs'       # where to save data on elasticsearch


data_path = '/logs/waf-dev/%s' % time.strftime('%Y%m%d%H', time.localtime(time.time()))     
# absolute path of data on the hadoop, generate dir name according to local machine time.
# e.g. 2020033014 -> 2020.3.30 14h

hadoop_http_ip = 'localhost:5070'   # where the hadoop http is, default is localhost:50070
hadoop_fs_ip = 'localhost:9000' # where SparkContext can access hadoop file, default is localhost:9000
# ---


def write_2_es(file_path):
    sc=spark.sparkContext
    lines = sc.textFile('hdfs://%s%s' % (hadoop_fs_ip, file_path))

    waf_parts = lines.map(lambda l: l.split("#WAFSPLIT#"))
    
    try:
        waf_5=waf_parts.filter(lambda x:len(x)==15)
        waf_55=waf_5.map(lambda p: Row(time=p[2],src_ip=p[3],src_port=p[4],dst_ip=p[5],dst_port=p[6],attack=p[7],action=p[8],vul_level=p[9],risk=p[10],advice=p[11],request=p[12],method=p[13],url=p[14]))

        schemawaf_55=spark.createDataFrame(waf_55)
        schemawaf_55.createOrReplaceTempView("waf_55")
        # schemawaf_55.show(5)
    except ValueError:
        # When there is no data with length 15, create a empty DataFrame
        schema = StructType([
            StructField("time", DateType(), False),
            StructField("src_ip", StringType(), False),
            StructField("src_port", StringType(), False),
            StructField("dst_ip", StringType(), False),
            StructField("dst_port", StringType(), False),
            StructField("attack", StringType(), False),
            StructField("action", StringType(), False),
            StructField("vul_level", StringType(), False),
            StructField("risk", StringType(), False),
            StructField("advice", StringType(), False),
            StructField("request", StringType(), False),
            StructField("method", StringType(), False),
            StructField("url", StringType(), False),
        ])
        schemawaf_55 = spark.createDataFrame(sc.emptyRDD(), schema)
    
    try:
        waf_1=waf_parts.filter(lambda x:len(x)==11)
        waf_11=waf_1.map(lambda p: Row(time=p[2],src_ip=p[3],src_port=p[4],dst_ip=p[5],dst_port=p[6],attack=p[7],action=p[8],vul_level=p[9],risk=p[10]))
        schemawaf_11=spark.createDataFrame(waf_11)
        schemawaf_11.createOrReplaceTempView("schemawaf_11")
        # schemawaf_11.show(5)
    except ValueError:
        # When there is no data with length 11, create a empty DataFrame
        schema = StructType([
            StructField("time", DateType(), False),
            StructField("src_ip", StringType(), False),
            StructField("src_port", StringType(), False),
            StructField("dst_ip", StringType(), False),
            StructField("dst_port", StringType(), False),
            StructField("attack", StringType(), False),
            StructField("action", StringType(), False),
            StructField("vul_level", StringType(), False),
            StructField("risk", StringType(), False),
        ])
        schemawaf_11 = spark.createDataFrame(sc.emptyRDD(), schema)

    try:
        waf_2=waf_parts.filter(lambda x:len(x)==12)
        waf_12=waf_2.map(lambda p: Row(time=p[2],src_ip=p[3],src_port=p[4],dst_ip=p[5],dst_port=p[6],attack=p[7],action=p[8],vul_level=p[9],risk=p[10],advice=p[11]))
        schemawaf_12=spark.createDataFrame(waf_12)
        schemawaf_12.createOrReplaceTempView("waf_12")
        # schemawaf_12.show(5)
    except Exception as e:
        # When there is no data with length 12, create a empty DataFrame
        schema = StructType([
            StructField("time", DateType(), False),
            StructField("src_ip", StringType(), False),
            StructField("src_port", StringType(), False),
            StructField("dst_ip", StringType(), False),
            StructField("dst_port", StringType(), False),
            StructField("attack", StringType(), False),
            StructField("action", StringType(), False),
            StructField("vul_level", StringType(), False),
            StructField("risk", StringType(), False),
            StructField("advice", StringType(), False),
        ])
        schemawaf_12 = spark.createDataFrame(sc.emptyRDD(), schema)

    schemawaf1=schemawaf_11.join(schemawaf_12,["time","src_ip","src_port","dst_ip","dst_port","attack","action","vul_level","risk"],"outer").join(schemawaf_55,["time","src_ip","src_port","dst_ip","dst_port","attack","action","vul_level","risk"],"outer")

    df_waf_1 = schemawaf1.select('time','src_ip','src_port','dst_ip','dst_port','attack','action','vul_level','risk','method','request','url')
    df_waf=df_waf_1.withColumn("Time",df_waf_1["time"].cast("timestamp"))
    # df_waf.printSchema()
    # print('count',df_waf.count())
    # df_waf.show()
    df_waf.write.format("org.elasticsearch.spark.sql").option("es.nodes", es_nodes).option("es.mapping.rich.date", "true").mode("append").save(es_index)


def main():
    hdfs_client = pyhdfs.HdfsClient(hadoop_http_ip)

    for item in list(hdfs_client.listdir(data_path)):
        if item.split('.')[-1] == 'tmp':
            continue
        
        print(os.path.join(data_path, item))
        write_2_es(os.path.join(data_path, item))


if __name__ == '__main__':
    main()
