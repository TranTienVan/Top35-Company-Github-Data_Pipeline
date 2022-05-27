from pickle import APPEND
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType
from modules import License
from modules import Repository
from modules import User
from modules import companies
from modules import Language
import requests
from pyspark.conf import SparkConf

# Create spark session
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
    
df_companies = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", 'public."USER"') \
    .option("user", POSTGRES_USERNAME) \
    .option("password", POSTGRES_PASSWORD) \
    .load() \
    .filter("type == 'Organization'") \
    .select("id", "login")
    
df_old_repositories = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", 'public."REPOSITORY_LANGUAGE"') \
    .option("user", POSTGRES_USERNAME) \
    .option("password", POSTGRES_PASSWORD) \
    .load()

old_id_repositories = list(set([row[0] for row in df_old_repositories.rdd.collect()]))

df_repositories = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", 'public."REPOSITORY"') \
    .option("user", POSTGRES_USERNAME) \
    .option("password", POSTGRES_PASSWORD) \
    .load() 

df_repositories = df_repositories \
    .filter(df_repositories.id.isin(old_id_repositories) == False) \
    .join(df_companies, df_repositories.organization_id == df_companies.id, "inner") \
    .select(df_repositories.id, df_repositories.name, df_companies.login)


for [repository_id, repository_name, company_name] in df_repositories.rdd.collect():
    
    
    repo_language = Language(company_name, repository_name, repository_id, auth)
    
    repos_language_data = repo_language.data
    
    print(company_name, repository_name)
    
    df_repo_language_data = spark.createDataFrame(data=repos_language_data,schema=Language.LANGUAGE_SCHEMA)

    df_repo_language_data.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."REPOSITORY_LANGUAGE"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()
        

    print("######################################")
    print("Loaded {} df_repo_language_data".format("All"))
    print("######################################")
