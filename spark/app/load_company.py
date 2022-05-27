import sys
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType
from modules import companies
from modules import User

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
    .filter("type == 'Organization'")

old_companies = [row[1].lower() for row in df_companies.rdd.collect()]

companies = list(set(companies) - set(old_companies))

companies_data = []

for company in companies:
    company_obj = User(company, auth)
    print(company_obj.data)
    companies_data.append(company_obj.data)

df_company_data = spark.createDataFrame(data=companies_data,schema=User.USER_SCHEMA)

df_company_data.write \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", 'public."USER"') \
    .option("user", POSTGRES_USERNAME) \
    .option("password", POSTGRES_PASSWORD) \
    .mode("append") \
    .save()
    
print("######################################")
print("Loaded df_company_data")
print("######################################")


