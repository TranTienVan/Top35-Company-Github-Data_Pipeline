import json
import psycopg2
import pyspark
import requests
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType
from modules.repository import Repository
from modules.user import User


class Language():
    LANGUAGE_SCHEMA = StructType([
        StructField('repository_id', LongType(), True),
        StructField('language', StringType(), True),
        StructField('used_count', IntegerType(), True)
    ])
    LANGUAGE_URL = "https://api.github.com/repos/{}/{}/languages"
    
    
    def __init__(self, company: str, repository: str, repository_id, auth):
        self.url = Language.LANGUAGE_URL.format(company, repository)
        self.repository_id = repository_id
        
        self.read_data(auth)
        
    
    def read_data(self, auth):
        DataFromGithub = requests.get(self.url, auth=auth).json()
        
        data = [
            [self.repository_id, l, used_count] 
            for (l, used_count) in DataFromGithub.items()
        ]
        
        self.data = data
        

    
    

