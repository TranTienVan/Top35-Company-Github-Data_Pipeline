import requests
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType
import psycopg2
from datetime import datetime

class User():
    
    USER_URL = 'https://api.github.com/users/{}'
    USER_SCHEMA = StructType([   
        StructField('id', LongType(), True),
        StructField('login', StringType(), True),
        StructField('html_url', StringType(), True),
        StructField('type', StringType(), True),
        StructField('name', StringType(), True),
        StructField('blog', StringType(), True),
        StructField('location', StringType(), True),
        StructField('email', StringType(), True),
        StructField('public_repos', IntegerType(), True),
        StructField('followers', IntegerType(), True),
        StructField('following', IntegerType(), True),
        StructField('created_at', TimestampType(), True),
        StructField('updated_at', TimestampType(), True)
    ])
    
    def __init__(self, username: str, auth, cursor = None):
        if cursor != None:
            cursor.execute("""
                select * from "USER"
                where name = %s
            """, (username, ))
            self.data = list(cursor.fetchall()[0])
        else:
            self.username = username
            self.url = User.USER_URL.format(self.username)
            self.read_data(auth)
    
    def read_data(self, auth):

        DataFromGithub = requests.get(self.url, auth=auth).json()
        if "id" not in DataFromGithub or "login" not in DataFromGithub:
            self.data = []
        else:
            self.data = [
                DataFromGithub['id'],
                DataFromGithub["login"],
                DataFromGithub['html_url'],
                DataFromGithub['type'],
                DataFromGithub['name'],
                DataFromGithub['blog'],
                DataFromGithub['location'],
                DataFromGithub['email'],
                DataFromGithub['public_repos'],
                DataFromGithub['followers'],
                DataFromGithub['following'],
                datetime.strptime(DataFromGithub['created_at'].replace("T", " ")[:-1],"%Y-%m-%d %H:%M:%S"),
                datetime.strptime(DataFromGithub['updated_at'].replace("T", " ")[:-1], "%Y-%m-%d %H:%M:%S")
            ]
    
    def to_dict(self):
        return {
            'id': self.data[0],
            'login': self.data[1],
            'html_url': self.data[2],
            'type': self.data[3],
            'name': self.data[4],
            'blog': self.data[5],
            'location': self.data[6],
            'email': self.data[7],
            'public_repos': self.data[8],
            'followers': self.data[9],
            'following': self.data[10],
            'created_at': self.data[11],
            'updated_at'    : self.data[12]
        }
    def get_id(self):
        return self.data[0]
    
    def get_data(self) -> list:    
        return self.data
    
    
        
