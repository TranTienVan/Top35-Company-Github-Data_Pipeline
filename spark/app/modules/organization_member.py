import json
import psycopg2
import pyspark
import requests
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType

class Organization_Member():
    ORGANIZATION_MEMBER_SCHEMA = StructType([
        StructField('organization_id', LongType(), True),
        StructField('user_id', LongType(), True)
    ])
    ORGANIZATION_MEMBER_URL = "https://api.github.com/orgs/{}/members"
    
    def __init__(self, organization_id, user_id):
        self.data = [organization_id, user_id]
        
    
        