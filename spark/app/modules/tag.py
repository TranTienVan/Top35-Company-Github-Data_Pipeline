import json
import psycopg2
import pyspark
import requests
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType

class Tag():
    TAG_SCHEMA = StructType([
        StructField('commit_sha', StringType(), True),
        StructField('name', StringType(), True),
        StructField('zipball_url', StringType(), True),
    ])
    TAG_URL = "https://api.github.com/repos/{}/{}/tags"    
    
    def __init__(self, tag: dict):
        
        data = [tag["commit"]["sha"], tag["name"], tag["zipball_url"]]
        
        self.data = data

    def get_sha(self):
        return self.data[0]
    

