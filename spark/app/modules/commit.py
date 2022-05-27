import json
import psycopg2
import pyspark
import requests
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType
from modules.repository import Repository
from modules.user import User
from datetime import datetime

class Commit():
    COMMIT_SCHEMA = StructType([
        StructField('sha', StringType(), True),
        StructField('date', TimestampType(), True),
        StructField('message', StringType(), True),
        StructField('comment_count', IntegerType(), True),
        StructField('html_url', StringType(), True),
        StructField('author_id', LongType(), True),
        # StructField('additions', StringType(), True),
        # StructField('deletions', StringType(), True),
        StructField('repository_id', LongType(), True),
    ])
    
    COMMIT_URL = "https://api.github.com/repos/{}/{}/commits"
    def __init__(self, repository_id, commit: dict):
        data = [
            commit["sha"],
            datetime.strptime(commit["commit"]["author"]["date"].replace("T", " ")[:-1], "%Y-%m-%d %H:%M:%S"),
            commit["commit"]["message"],
            commit["commit"]["comment_count"],
            commit["html_url"],
            commit["author"]["id"],
            repository_id
        ]
        
        self.data = data

    def to_dict(self):
        return {
            'sha': self.data[0],
            'date': self.data[1],
            'message': self.data[2],
            'comment_count': self.data[3],
            'html_url': self.data[4],
            'author_id': self.data[5],
            'repository_id': self.data[6],
        }
        
        
    def get_sha(self):
        return self.data[0]
    
    def get_author_id(self):
        return self.data[0] 



        
        
        
        
        