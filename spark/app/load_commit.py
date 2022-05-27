import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import DoubleType
from soupsieve import select
from modules import License
from modules import Repository
from modules import User
from modules import companies
from modules import Language
from modules import Commit
import requests
from datetime import datetime
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType
from modules import Organization_Member
import time

conf = SparkConf().setAppName("SparkByExamples.com") \
        .set("spark.shuffle.service.enabled", "false") \
        .set("spark.dynamicAllocation.enabled", "false") 
        
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

def load_commit():
    df_all_repositories = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."REPOSITORY"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .load()
    df_all_repositories.registerTempTable("df_all_repositories")
    df_old_repositories = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."COMMIT"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .load()
    df_old_repositories.registerTempTable("df_old_repositories")
    df_companies = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."USER"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .load()
    df_companies.registerTempTable("df_companies")
    
    df_repositories = spark.sql("""
        with temp as (
            select id from df_all_repositories
            except
            select distinct repository_id as id from df_old_repositories
        )
        select a.id, a.name, c.login
        from df_all_repositories a
        inner join df_companies c
            on a.organization_id = c.id
        inner join temp t
            on t.id = a.id
        where c.type = 'Organization';      
    """)
    print("Loaded successfully")
    old_users = [row[1] for row in df_companies.rdd.collect()]
    
    
    
    for [repository_id, repository_name, company_name] in df_repositories.rdd.collect():
        new_users = []
        commits_data = []    
        
        i = 1
        length = 100
        
        while length == 100:
            params["page"] = str(i)
            
            commits = requests.get(Commit.COMMIT_URL.format(company_name, repository_name), params=params, auth = auth).json()
            
            for commit in commits:
                if "author" in commit and commit["author"]:
                    commits_data.append([
                        commit["sha"],
                        datetime.strptime(commit["commit"]["author"]["date"].replace("T", " ")[:-1], "%Y-%m-%d %H:%M:%S"),
                        commit["commit"]["message"],
                        commit["commit"]["comment_count"],
                        commit["html_url"],
                        commit["author"]["id"],
                        repository_id
                    ])
                    
                    new_users.append(commit["author"]["login"])
            
            print(company_name, repository_name, i)
            length = len(commits)
            i += 1
    
        new_users = list(set(new_users) - set(old_users))
        new_users_data = []
        for new_user in new_users:
            new_user_obj = User(new_user, auth)
            if new_user_obj.data != []:
                time.sleep(0.5)
                print(new_user)
                new_users_data.append(new_user_obj.data)
                
        # df_new_user_data = spark.createDataFrame(data=new_users_data,schema=User.USER_SCHEMA)

        df_new_user_data = pd.DataFrame(data=new_users_data, columns=User.USER_SCHEMA.fieldNames())
        
        # df_new_user_data \
        #     .write \
        #     .format("jdbc") \
        #     .option("url", POSTGRES_URL) \
        #     .option("dbtable", 'public."USER"') \
        #     .option("user", POSTGRES_USERNAME) \
        #     .option("password", POSTGRES_PASSWORD) \
        #     .mode("append") \
        #     .save()
        df_new_user_data.to_csv("/opt/spark/resources/csv/USER.csv", header = False, index=False, mode="a")
        
        print("######################################")
        print("Loaded df_new_user_data")
        print("######################################")
        
        # df_commits_data = spark.createDataFrame(data=commits_data,schema=Commit.COMMIT_SCHEMA)

        df_commits_data = pd.DataFrame(data=commits_data, columns= Commit.COMMIT_SCHEMA.fieldNames())
        
        # df_commits_data \
        #     .write \
        #     .format("jdbc") \
        #     .option("url", POSTGRES_URL) \
        #     .option("dbtable", 'public."COMMIT"') \
        #     .option("user", POSTGRES_USERNAME) \
        #     .option("password", POSTGRES_PASSWORD) \
        #     .mode("append") \
        #     .save()
        df_commits_data.to_csv("/opt/spark/resources/csv/COMMIT.csv", header = False, index=False, mode="a")
        
        print("######################################")
        print("Loaded {} {} df_commits_data".format(company_name, repository_name))
        print("######################################")
        
    
    
def update_commit():
    df_activity = spark.read.format("csv").option("header", True).load("/opt/spark/resources/csv/activity.csv")
    
    commits_data = []
    new_users = []
    
    for [company_name, repository_id, repository_name, time] in df_activity.rdd.collect():
        
        commits = requests.get(Commit.COMMIT_URL.format(company_name, repository_name), params=params, auth = auth).json()

        
        for commit in commits:
            if commit["author"]:
            
                commit_obj = Commit(repository_id, commit)
                commits_data.append(commit_obj.to_dict())
                
                new_users.append([commit["author"]["id"], commit["author"]["login"]])
                
    df_users = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."USER"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .load()
    
    old_user = [row[0] for row in df_users.rdd.collect()]
        
    df_new_users = spark.createDataFrame(data=new_users, schema=StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True)
    ]))
    df_new_users = df_new_users.dropDuplicates(subset=["id"]).filter(df_new_users.id.isin(old_user) == False)
    
    new_users_data = []

    fail_user = []
    for [user_id, new_user] in df_new_users.rdd.collect():
        new_user_obj = User(new_user, auth)
        if new_user_obj.data != []:
            # print(new_user_obj.data)
            new_users_data.append(new_user_obj.data)
        else:
            fail_user.append(user_id)

    df_new_user_data = spark.createDataFrame(data=new_users_data,schema=User.USER_SCHEMA)

    df_new_user_data \
        .write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."USER"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()
        
    print("######################################")
    print("Loaded df_new_user_data")
    print("######################################")
    
    df_commits_data = spark.createDataFrame(data=commits_data,schema=Commit.COMMIT_SCHEMA)

    df_commits_data \
        .filter(df_commits_data.author_id.isin(fail_user) == False) \
        .write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."COMMIT"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()
        
    print("######################################")
    print("Loaded df_commits_data")
    print("######################################")

if sys.argv[8] == "load":
    load_commit()
elif sys.argv[8] == "update":
    update_commit()