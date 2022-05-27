import json
from re import S
import requests
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType
import psycopg2


class License():
    ALL_LICENSE_URL = 'https://api.github.com/licenses'
    LICENSE_URL = 'https://api.github.com/licenses/{}'
    LICENSE_SCHEMA = StructType([
        StructField('key', StringType(), True),
        StructField('name', StringType(), True),
        StructField('spdx_id', StringType(), True),
        StructField('html_url', StringType(), True),
        StructField('description', StringType(), True),
        StructField('implementation', StringType(), True)
    ])
    
    def __init__(self, key: str, auth, cursor = None):
        self.key = key
        self.url = License.LICENSE_URL.format(self.key)
        if cursor != None:
            cursor.execute("""
                select * from "LICENSE"
                where key = %s
            """, (key, ))            
            self.data = list(cursor.fetchall()[0])
            
        else:
            self.read_data(auth)

    def read_data(self, auth):

        DataFromGithub = requests.get(self.url, auth=auth).json()
        
        data = [
            DataFromGithub['key'],
            DataFromGithub['name'],
            DataFromGithub['spdx_id'],
            DataFromGithub['html_url'],
            DataFromGithub['description'],
            DataFromGithub['implementation'],
        ]
        
        self.data = data

    def get_key(self):
        return self.data[0]

    def get_data(self) -> list:
        
        return self.data
        
    
    
        
