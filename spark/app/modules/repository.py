import json
from h11 import Data
import pyspark
import requests
from sqlalchemy import Integer
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType
from modules.license import License
from modules.user import User
import psycopg2
from datetime import datetime

class Repository():
    ALL_REPOSITORY_URL = "https://api.github.com/orgs/{}/repos"
    REPOSITORY_URL = 'https://api.github.com/repos/{}/{}'
    REPOSITORY_SCHEMA = StructType([   
        StructField('id', LongType(), True),
        StructField('name', StringType(), True),
        StructField('full_name', StringType(), True),
        StructField('html_url', StringType(), True),
        StructField('description', StringType(), True),
        StructField('created_at', TimestampType(), True),
        StructField('updated_at', TimestampType(), True),
        StructField('pushed_at', TimestampType(), True),
        StructField('homepage', StringType(), True),
        StructField('size', IntegerType(), True),
        StructField('stargazers_count', IntegerType(), True),
        StructField('watchers_count', IntegerType(), True),
        StructField('forks_count', IntegerType(), True),
        StructField('open_issues_count', IntegerType(), True),
        StructField('license_key', StringType(), True),
        StructField('organization_id', LongType(), True)
    ])
    
    
    def __init__(self, user_id, DataFromGithub, auth = None):
        if type(DataFromGithub) == dict and not auth:
            self.user_id = user_id
        else:
            self.company = user_id
            self.repository = DataFromGithub
            self.url = Repository.REPOSITORY_URL.format(user_id, DataFromGithub)
            
            DataFromGithub = requests.get(self.url, auth=auth).json()
            
        
        data = [
            DataFromGithub['id'],
            DataFromGithub['name'],
            DataFromGithub['full_name'],
            DataFromGithub['html_url'],
            DataFromGithub['description'],
            datetime.strptime(DataFromGithub['created_at'].replace("T", " ")[:-1], "%Y-%m-%d %H:%M:%S"),
            datetime.strptime(DataFromGithub['updated_at'].replace("T", " ")[:-1], "%Y-%m-%d %H:%M:%S"),
            datetime.strptime(DataFromGithub['pushed_at'].replace("T", " ")[:-1], "%Y-%m-%d %H:%M:%S"),
            DataFromGithub['homepage'],
            DataFromGithub['size'],
            DataFromGithub['stargazers_count'],
            DataFromGithub['watchers_count'],
            DataFromGithub['forks_count'],
            DataFromGithub['open_issues_count'],
            DataFromGithub["license"]["key"] if DataFromGithub["license"] and DataFromGithub["license"]["key"] != 'other' else None,
            self.user_id
        ]
        self.data = data
    
    def to_dict(self):
        
        return {
            'id': self.data[0],
            'name': self.data[1],
            'full_name': self.data[2],
            'html_url': self.data[3],
            'description': self.data[4],
            'created_at': self.data[5],
            'updated_at': self.data[6],
            'pushed_at': self.data[7],
            'homepage': self.data[8],
            'size': self.data[9],
            'stargazers_count': self.data[10],
            'watchers_count': self.data[11],
            'forks_count': self.data[12],
            'open_issues_count': self.data[13],
            'license_key': self.data[14],
            'organization_id': self.data[15],
        }
        
    def get_id(self):
        return self.data[0]
        
        
    def get_data(self):
        return self.data




