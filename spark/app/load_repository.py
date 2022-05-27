import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.conf import SparkConf
from pyspark.sql.types import DoubleType
from modules import License
from modules import Repository
from modules import User
from modules import companies
import requests
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

df_old_repo = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", 'public."REPOSITORY"') \
    .option("user", POSTGRES_USERNAME) \
    .option("password", POSTGRES_PASSWORD) \
    .load()
    
old_companies =list(set([row[15] for row in df_old_repo.rdd.collect()]))

df_companies = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", 'public."USER"') \
    .option("user", POSTGRES_USERNAME) \
    .option("password", POSTGRES_PASSWORD) \
    .load()

df_companies = df_companies \
    .filter("type == 'Organization'") \
    .filter(df_companies.id.isin(old_companies) == False) \
    .select("id", "login")

for [company_id, company] in df_companies.rdd.collect():
    license = []
    length = 100    
    repos_data = []
    i = 1
    while length == 100:
        params["page"] = str(int(i))
        
        repos = requests.get(Repository.ALL_REPOSITORY_URL.format(company), params=params, auth=auth).json()
        
        for repo in repos:
            repo_detail = Repository(company_id, repo)
             
            if repo["license"] and repo["license"]["key"] != "other":
                license.append(repo["license"]["key"])
            repos_data.append(repo_detail.data)
        
        print(company, company_id, i)
        i += 1
        length = len(repos)
        
    df_old_license = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."LICENSE"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .load() \
        .select("key")
    
    old_license = [row[0] for row in df_old_license.rdd.collect()]
    
    license_data = []
    new_license = list(set(license) - set(old_license))
    for key in new_license:
        license = License(key, auth)
        print(license.data)
        license_data.append(license.data)
    
    df_license_data = spark.createDataFrame(data=license_data,schema=License.LICENSE_SCHEMA)

    df_license_data.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."LICENSE"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()
        
    print("######################################")
    print("Loaded df_license_data")
    print("######################################")
        
        
    df_repo_data = spark.createDataFrame(data=repos_data,schema=Repository.REPOSITORY_SCHEMA)
    df_old_repo_data = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."REPOSITORY"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .load()
        
    old_repo_id = [row[0] for row in df_old_repo_data.rdd.collect()]
    
    df_repo_data = df_repo_data.filter(df_repo_data.id.isin(old_repo_id) == False)

    df_repo_data.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."REPOSITORY"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()
        
    print("######################################")
    print("Loaded {} df_repo_data".format(company))
    print("######################################")
