from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
from modules import Repository
from modules import User
from modules import Commit
from pyspark.conf import SparkConf
import sys
from modules import ACTIVITY_REPOSITORY
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType
# Create spark session
from modules import Tag
spark = (SparkSession
    .builder
    .getOrCreate()
)

conf = SparkConf().setAppName("SparkByExamples.com") \
        .set("spark.shuffle.service.enabled", "false") \
        .set("spark.dynamicAllocation.enabled", "false")

####################################
# Parameters
####################################
POSTGRES_URL = sys.argv[1]
POSTGRES_USERNAME = sys.argv[2]
POSTGRES_PASSWORD = sys.argv[3]
POSTGRES_HOST = sys.argv[4]
POSTGRES_DATABASE_NAME = sys.argv[5]

EMAIL_GITHUB = sys.argv[6]
KEY_GITHUB = sys.argv[7]

params = {
    "per_page": str(100),
    "page": str(1)
}

headers = {
    "Accept":"application/vnd.github.v3+json"
}

auth = (
    EMAIL_GITHUB,
    KEY_GITHUB
)


def update_repo_activity():    
    df_companies = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."USER"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .load() \
        .filter("type == 'Organization'") \
        .select("login")
    
    df_repos = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."REPOSITORY"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .load()
    
    activity = []
    new_repos = []
    activity_repos = []
    
    old_repos = [row[1] for row in df_repos.rdd.collect()]
    
    for [company] in df_companies.rdd.collect():

        r = requests.get(ACTIVITY_REPOSITORY.ACTIVITY_REPOSITORY_URL.format(company))
        soup = BeautifulSoup(r.text, "html.parser")


        repos = soup.find_all("li", {"class": "Box-row"})

        for repo in repos:
            name = repo.find("a", {"class": "d-inline-block"}).getText(strip = True)
            time = repo.find("relative-time", {"class": "no-wrap"})["datetime"].replace("T", " ")[:-1]
            
            if datetime.strptime(time["datetime"].replace("T", " ")[:-1], "%Y-%m-%d %H:%M:%S") + timedelta(hours = 7)> datetime.now() - timedelta(hours = 1):
                
                repo_infor = Repository(company, name, auth)
                activity.append(company, repo_infor.get_id(), name, time)
                activity_repo = ACTIVITY_REPOSITORY(repo_infor.get_id(), time)
                activity_repos.append(activity_repo)
                
                if name not in old_repos:
                    new_repos.append(repo_infor.data)
                    
    df_new_repo_data = spark.createDataFrame(data=new_repos,schema=Repository.REPOSITORY_SCHEMA)
    df_new_repo_data.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."REPOSITORY"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()
    print("######################################")
    print("Loaded df_new_repo_data")
    print("######################################")
    
    
    df_activity_repo = spark.createDataFrame(data=activity_repos,schema=ACTIVITY_REPOSITORY.ACTIVITY_REPOSITORY_SCHEMA)
    df_activity_repo.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."REPOSITORY_ACTIVITY"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()
    print("######################################")
    print("Loaded df_activity_repo")
    print("######################################")
    
    
    df = spark.createDataFrame(data = activity, schema=StructType([
        StructField("company", StringType(), True),
        StructField("repository_id", LongType(), True),
        StructField("repository", StringType(), True),
        StructField("datetime_updated", TimestampType, True)
    ]))
    
    df.write.format("csv").option("header", True).mode("overwrite").save("/opt/spark/resources/csv/activity.csv")
    
update_repo_activity()

# update repository
# Check and add commit
# CHeck and add tag
# Check and add contributor and repo_contributor
# update language count
    
    
     
    
    