import os
import sys
import pyspark
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import split, regexp_extract


log_file_path = 'vpn_test.txt'
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
base_df = sqlContext.read.text(log_file_path)
base_df.show()

split_df = base_df.select(
                        regexp_extract('value', r'^.*(\w{3,}\s\d{2}\s\d+)', 1).alias('date'),
                        regexp_extract('value', r'^.*(\d{2}:\d{2}:\d{2})', 1).alias('time'),
                        regexp_extract('value', r'^.*[\s]id=(\w+)', 1).alias('id'),
                        regexp_extract('value', r'^.*time=\"(.*)\"', 1).alias('time'),
                        regexp_extract('value', r'^.*timezone=(\w+\(\+\w+\))', 1).alias('timezone'),
                        regexp_extract('value', r'^.*fw=(\w+)', 1).alias('fw'),
                        regexp_extract('value', r'^.*pri=(\w+)', 1).alias('pri'),
                        regexp_extract('value', r'^.*vpn=(\w+)', 1).alias('vpn'),
                        regexp_extract('value', r'^.*src=([\d\.]+)', 1).alias('src'),
                        regexp_extract('value', r'^.*sport=(\w+)', 1).alias('sport'),
                        regexp_extract('value', r'^.*type=(\w+)', 1).alias('type'),
                        regexp_extract('value', r'^.*msg=\"(.*)\"', 1).alias('msg'),
                        )
split_df.show()
