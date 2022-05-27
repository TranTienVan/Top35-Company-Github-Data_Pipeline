import json
import psycopg2
import pyspark
import requests
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType
from modules.repository import Repository
from modules.user import User


class Contribution():
    CONTRIBUTION_SCHEMA = StructType([
        StructField('repository_id', LongType(), True),
        StructField('contributor_id', LongType(), True),
        StructField('contributions', IntegerType(), True),
    ])
    CONTRIBUTION_URL = "https://api.github.com/repos/{}/{}/contributors"
    
    
    def __init__(self, repository_id, contribution: dict):
        
        data = [repository_id, contribution["id"], contribution["contributions"]]
        
        self.data = data

    def get_id_pair(self):
        return [self.data[0], self.data[1]]
    

