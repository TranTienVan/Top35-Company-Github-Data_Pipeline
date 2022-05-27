import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType
from modules import License
from modules import Repository
from modules import User
from modules import companies
from modules import Language
from modules import Commit
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


df_commits = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", 'public."COMMIT"') \
    .option("user", POSTGRES_USERNAME) \
    .option("password", POSTGRES_PASSWORD) \
    .load() \
        
df_commits.registerTempTable("df_commits")

df_repository_contribution = spark.sql("""
    select 
        repository_id, 
        author_id as contributor_id,
        count(*) as contributions
    from df_commits
    group by repository_id, author_id
""")
df_repository_contribution.write \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", 'public."REPOSITORY_CONTRIBUTOR"') \
    .option("user", POSTGRES_USERNAME) \
    .option("password", POSTGRES_PASSWORD) \
    .mode("overwrite") \
    .save()
print("######################################")
print("Loaded {} df_repository_contribution".format("all"))
print("######################################")



