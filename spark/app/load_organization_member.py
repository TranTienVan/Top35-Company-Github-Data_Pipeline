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
from modules import Organization_Member

import time
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
df_companies_member = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", 'public."ORGANIZATION_MEMBER"') \
    .option("user", POSTGRES_USERNAME) \
    .option("password", POSTGRES_PASSWORD) \
    .load()

df_companies = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", 'public."USER"') \
    .option("user", POSTGRES_USERNAME) \
    .option("password", POSTGRES_PASSWORD) \
    .load()

df_companies = df_companies \
    .join(df_companies_member, df_companies.id == df_companies_member.organization_id, "inner") \
    .select(df_companies.login)

old_companies = list(set([row[0].lower() for row in df_companies.rdd.collect()]))

companies = list(set(companies) - set(old_companies))

for company in companies:
    company_obj = User(company, auth)
    
    organization_members = []
    relational_organization_members = []
    length = 100    
    i = 1
    while length == 100:
        params["page"] = str(i)
        
        members = requests.get(Organization_Member.ORGANIZATION_MEMBER_URL.format(company), params=params, auth=auth).json()
        
        for member in members:
            if "login" in member and "id" in member:
                member_obj = User(member["login"], auth)
                if member_obj.data == []:
                    continue
                
                relational_member = Organization_Member(company_obj.get_id(), member_obj.get_id())
                
                
                organization_members.append(member_obj.data)
                relational_organization_members.append(relational_member.data)
        print(company, i)
        i += 1
        length = len(members)
        
    df_organization_members = spark.createDataFrame(data=organization_members,schema=User.USER_SCHEMA)
    
    df_users = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."USER"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .load() 
        
    # df_organization_members = df_organization_members.union(df_users)
    # df_organization_members = df_organization_members.drop_duplicates(subset=["id"])
    
    old_id = [row[0] for row in df_users.rdd.collect()]
    df_organization_members = df_organization_members.filter(df_organization_members.id.isin(old_id) == False)
    
    df_organization_members.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."USER"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()
        
        
    print("######################################")
    print("Loaded {} df_organization_members".format(company))
    print("######################################")
    
    
    df_relational_organization_members = spark.createDataFrame(data=relational_organization_members, schema=Organization_Member.ORGANIZATION_MEMBER_SCHEMA)
    df_old_relational_organization_members = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."ORGANIZATION_MEMBER"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .load() 
    df_relational_organization_members = df_relational_organization_members.exceptAll(df_old_relational_organization_members)
    
    df_relational_organization_members.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", 'public."ORGANIZATION_MEMBER"') \
        .option("user", POSTGRES_USERNAME) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()
    print("######################################")
    print("Loaded {} df_relational_organization_members".format(company))
    print("######################################")
    
    time.sleep(60)
