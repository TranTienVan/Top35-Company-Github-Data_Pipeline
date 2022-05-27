import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType
from modules import License
from modules import Repository
from modules import User
from modules import companies
from modules import Language
from modules import Tag
import requests

from modules import Organization_Member


# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)

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

def load_tag_repository():
    df_repositories = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."REPOSITORY"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .load() \
        .select("id", "name", "organization_id")
        
    df_companies = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."USER"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .load() \
        .filter("type == 'Organization'") \
        .select("id", "login")

    df_repositories = df_repositories.join(df_companies, df_repositories.organization_id == df_companies.id, "inner").select(df_repositories.name, df_companies.name)




    for [repository_name, company_name] in df_repositories.rdd.collect():
        tags_data = []    
        i = 1
        length = 100    
        while length == 100:
            params["page"] = str(i)
            
            tags = requests.get(Tag.TAG_URL.format(company_name, repository_name), params=params, auth = auth).json()
            
            for tag in tags:
                tag_obj = Tag(tag)
                tags_data.append(tag_obj.data)
                print(tag_obj.data)
            
            
            length = len(tags)
            i += 1
        
        print("Collected {} {}".format(repository_name, company_name))
        
        df_old_tags = spark.read \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", 'public."REPOSITORY_TAG"') \
            .option("user", POSTGRES_USERNAME) \
            .option("password", POSTGRES_PASSWORD) \
            .load()
        
        old_tags = [row[0] for row in df_old_tags.rdd.collect()]
        
        
        df_tags_data = spark.createDataFrame(data=tags_data,schema=Tag.TAG_SCHEMA)

        df_tags_data \
            .filter(df_tags_data.commit_sha.isin(old_tags) == False) \
            .write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", 'public."REPOSITORY_TAG"') \
            .option("user", POSTGRES_USERNAME) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()
        print("######################################")
        print("Loaded {} {} df_tags_data".format(company_name, repository_name))
        print("######################################")

def update_tag_repository():
    df_activity = spark.read.format("csv").option("header", True).load("/opt/spark/resources/csv/activity.csv")
    tags_data = []    
    for [company_name, repository_id, repository_name, time] in df_activity.rdd.collect():
        
        i = 1
        length = 100    
        while length == 100:
            params["page"] = str(i)
            
            tags = requests.get(Tag.TAG_URL.format(company_name, repository_name), params=params, auth = auth).json()
            
            for tag in tags:
                tag_obj = Tag(tag)
                tags_data.append(tag_obj.data)
                print(tag_obj.data)
            
            
            length = len(tags)
            i += 1
        
        print("Collected {} {}".format(repository_name, company_name))

    
    df_old_tags = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."REPOSITORY_TAG"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .load()
    
    old_tags = [row[0] for row in df_old_tags.rdd.collect()]
    
    
    df_tags_data = spark.createDataFrame(data=tags_data,schema=Tag.TAG_SCHEMA)

    df_tags_data \
        .filter(df_tags_data.commit_sha.isin(old_tags) == False) \
        .write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."REPOSITORY_TAG"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()
    print("######################################")
    print("Loaded df_tags_data")
    print("######################################")
    
if sys.argv[8] == "load":
    load_tag_repository()

elif sys.argv[8] == "update":
    update_tag_repository()