from elasticsearch import Elasticsearch
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import to_date, to_timestamp
import re
import json
import hashlib
from datetime import datetime, timezone, timedelta
import time
import dateutil.parser as dp
from pyspark.sql.functions import date_format


log_file_path = 'vpn.txt'
sc = SparkContext.getOrCreate()
rdd = sc.textFile(log_file_path)

rdd_regex = r'^.*(\w{3,}\s\d{2}\s\d+)\s(\d{2}:\d{2}:\d{2})\s\w+\sid=(\w+)\stime=\"(.*?)\"\stimezone=\w+\(([\+,\-]\d+)\)\sfw=(\w+)\spri=(\w+)\svpn=(\w+)\ssrc=([\d\.]+)\ssport=(\w+)\stype=(\w+)\smsg=\"(.*)\"'
parse_regex = re.compile(rdd_regex)


def parse(input_str):
    s = parse_regex.match(input_str)
    d = {}
    try:
        d['id'] = s.group(3)
        # d['date'] = s.group(1)
        # d['time'] = s.group(2)
        # d['fulltime'] = s.group(4)
        # d['timezone'] = s.group(5)

        full_time = dp.parse(s.group(4))

        # convert time to utc according to time_zone
        time_zone = s.group(5)
        full_time = full_time.replace(tzinfo=timezone(timedelta(hours=int(time_zone[0] + time_zone[1:3]), minutes=int(time_zone[0] + time_zone[3:5]))))
        
        d['time'] = full_time
        d['fw'] = s.group(6)
        d['pri'] = int(s.group(7))
        d['vpn'] = s.group(8)
        d['src'] = s.group(9)
        d['sport'] = s.group(10)
        d['type'] = s.group(11)
        d['msg'] = s.group(12)
    except Exception as e:
        # print(e)
        pass

    return d

def add_id(data):
    j = json.dumps(data).encode('utf-8')
    generated_id = hashlib.sha224(j).hexdigest()
    return (generated_id, json.dumps(data))

rdd_parsed = rdd.map(parse)
# rdd_with_id = rdd_parsed.map(add_id)

# print(rdd_with_id.take(1))

# es_write_conf = {
#         "es.nodes" : "localhost",
#         "es.port" : "9200",
#         "es.resource" : 'vpn/logs',
#         "es.input.json": "yes"
# }

# rdd_with_id.saveAsNewAPIHadoopFile(
#     path='-',
#     outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
#     keyClass="org.apache.hadoop.io.NullWritable",
#     valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
#     conf=es_write_conf
#     )



sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(rdd_parsed)
df.show()
df.printSchema()


df.write.format(
    'org.elasticsearch.spark.sql'
    ).option(
        'es.resource', 'vpn/logs'
        ).option(
            'es.nodes', 'localhost'
            ).option('es.port', '9200').save()
