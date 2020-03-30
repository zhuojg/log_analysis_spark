import os
import sys
import pyspark
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import split, regexp_extract

log_file_path = 'dns.txt'
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
base_df = sqlContext.read.text(log_file_path)
base_df.show()

split_df = base_df.select(
                        regexp_extract('value', r'^.*(\d{2}-\w+-\d{4})', 1).alias('date'),
                        regexp_extract('value', r'^.*(\d{2}:\d{2}:[\d\.]{2,})', 1).alias('time'),
                        regexp_extract('value', r'^.*client\s([\d\.]{7,})\s\d+:', 1).alias('address'),
                        regexp_extract('value', r'^.*client\s[\d\.]{7,}\s(\d+):', 1).alias('port'),
                        regexp_extract('value', r'^.*view\s(\w+):', 1).alias('view'),
                        regexp_extract('value', r'^.*view\s\w+:\s([\w\.]+)\s', 1).alias('domain'),
                        regexp_extract('value', r'^.*view\s\w+:\s[\w\.]+\s(\w+)\s', 1).alias('classcategory'),
                        regexp_extract('value', r'^.*view\s\w+:\s[\w\.]+\s\w+\s(\w+)\s', 1).alias('0'),
                        regexp_extract('value', r'^.*view\s\w+:\s[\w\.]+\s\w+\s\w+\s(\w+)\s', 1).alias('1'),
                        regexp_extract('value', r'^.*view\s\w+:\s[\w\.]+\s\w+\s\w+\s\w+\s(.{1})\s', 1).alias('2'),
                        regexp_extract('value', r'^.*view\s\w+:\s[\w\.]+\s\w+\s\w+\s\w+\s.{1}\s(\w{1,2})\s', 1).alias('3'),
                        regexp_extract('value', r'^.*view\s\w+:\s[\w\.]+\s\w+\s\w+\s\w+\s.{1}\s\w{1,2}\s(\w{1,2})\s', 1).alias('4'),
                        regexp_extract('value', r'^.*view\s\w+:\s[\w\.]+\s\w+\s\w+\s\w+\s.{1}\s\w{1,2}\s\w{1,2}\s(\w{1,2})\s', 1).alias('5'),
                        regexp_extract('value', r'^.*view\s\w+:\s[\w\.]+\s\w+\s\w+\s\w+\s.{1}\s\w{1,2}\s\w{1,2}\s\w{1,2}\s(\w{1,2})\s', 1).alias('6'),
                        regexp_extract('value', r'^.*view\s\w+:\s[\w\.]+\s\w+\s\w+\s\w+\s.{1}\s\w{1,2}\s\w{1,2}\s\w{1,2}\s\w{1,2}\s(\w{1,2})\s', 1).alias('7'),
                        regexp_extract('value', r'^.*view\s\w+:\s[\w\.]+\s\w+\s\w+\s\w+\s.{1}\s\w{1,2}\s\w{1,2}\s\w{1,2}\s\w{1,2}\s\w{1,2}\s(\w{1,2})', 1).alias('8'),
                        )
split_df.show()
