import json
import psycopg2
import pyspark
import requests
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType
from datetime import datetime

class ACTIVITY_REPOSITORY():
    ACTIVITY_REPOSITORY_SCHEMA = StructType([
        StructField('repository_id', LongType(), True),
        StructField('datetime_updated', TimestampType(), True)
    ])
    ACTIVITY_REPOSITORY_URL = "https://github.com/orgs/{}/repositories"
    
    def __init__(self, repository_id, datetime_updated):
        
        data = [
            repository_id,
            datetime.strptime(datetime_updated, "%Y-%m-%d %H:%M:%S"),    
        ]
        
        self.data = data

    def get_repo_id(self):
        return self.data[0]
    

