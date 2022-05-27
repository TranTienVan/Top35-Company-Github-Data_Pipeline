import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType
from modules import License

import requests
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

keys = [v["key"] for v in requests.get(License.ALL_LICENSE_URL, params=params, headers=headers, auth=auth).json()]

license_data = []

for key in keys:
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


