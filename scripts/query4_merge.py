from session import create_spark_session
from pyspark.sql.functions import udf, year, month, regexp_replace, col, desc, dense_rank, to_date
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from queries import query4_hne
import io
import sys, time


spark = create_spark_session(4)
print("spark session created")

schema1 = "`DR_NO` STRING, \
          `Date Rptd` STRING, \
          `DATE OCC` STRING, \
          `TIME OCC` INTEGER, \
          `AREA` INTEGER, \
          `AREA NAME` STRING, \
          `Rpt Dist No` INTEGER, \
          `Part 1-2` INTEGER, \
          `Crm Cd` INTEGER, \
          `Crm Cd Desc` STRING, \
          `Mocodes` STRING, \
          `Vict Age` INTEGER, \
          `Vict Sex` STRING, \
          `Vict Descent` STRING, \
          `Premis Cd` INTEGER, \
          `Premis Desc` STRING, \
          `Weapon Used Cd` INTEGER, \
          `Weapon Desc` STRING, \
          `Status` STRING, \
          `Status Desc` STRING, \
          `Crm Cd 1` INTEGER, \
          `Crm Cd 2` INTEGER, \
          `Crm Cd 3` INTEGER, \
          `Crm Cd 4` INTEGER, \
          `LOCATION` STRING, \
          `Cross Street` STRING, \
          `LAT` DOUBLE, \
          `LON` DOUBLE"

descent_mapping = {
    'A': 'Other Asian',
    'B': 'Black',
    'C': 'Chinese',
    'D': 'Cambodian',
    'F': 'Filipino',
    'G': 'Guamanian',
    'H': 'Hispanic/Latin/Mexican',
    'I': 'American Indian/Alaskan Native',
    'J': 'Japanese',
    'K': 'Korean',
    'L': 'Laotian',
    'O': 'Other',
    'P': 'Pacific Islander',
    'S': 'Samoan',
    'U': 'Hawaiian',
    'V': 'Vietnamese',
    'W': 'White',
    'X': 'Unknown',
    'Z': 'Asian Indian'
}

schema2 = "`Zip Code` INTEGER, \
          `Community` STRING, \
          `Estimated Median Income` STRING"

schema3 = "`LAT` DOUBLE, \
          `LON` DOUBLE, \
          `ZIPcode` INTEGER"

schema4 = "`X` DOUBLE, \
          `Y` DOUBLE, \
          `FID` INTEGER, \
          `DIVISION` STRING, \
          `LOCATION` STRING, \
          `PREC` INTEGER"

data1 = spark.read.csv("/user/ubuntu/ta/advanced-db/data/crime_data_2010.csv", header=True, schema=schema1)
data2 = spark.read.csv("/user/ubuntu/ta/advanced-db/data/crime_data_2020.csv", header=True, schema=schema1)
data3 = spark.read.csv("/user/ubuntu/ta/advanced-db/data/LA_income_2015.csv", header=True, schema=schema2)
data4 = spark.read.csv("/user/ubuntu/ta/advanced-db/data/revgecoding.csv", header=True, schema=schema3)
data5 = spark.read.csv("/user/ubuntu/ta/advanced-db/data/LAPD_Police_Stations.csv", header=True, schema=schema4)

df = data1.union(data2).distinct()

df = df.withColumn("Date Rptd", to_date(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a")) \
       .withColumn("DATE OCC", to_date(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))

df = df.withColumn("Year", year("DATE OCC"))

data3 = data3.withColumn("Estimated Median Income", regexp_replace(col("Estimated Median Income"), "\$", ""))
data3 = data3.withColumn("Estimated Median Income", regexp_replace(col("Estimated Median Income"), ",", "").cast("float"))

crime_year = df.withColumn("Year", year("DATE OCC"))

crime_2015 = crime_year.filter(
   (col("Year") == 2015) &
   (col("Vict Descent").isNotNull()))

def map_descent(code):
   return descent_mapping.get(code, "Unknown")  # Default to "Unknown" if code not found

map_descent_udf = udf(map_descent, StringType())

crime_2015 = crime_2015.withColumn("Vict Descent", map_descent_udf(crime_2015["Vict Descent"]))

revgecoding = data4.dropDuplicates(['LAT', 'LON'])

old_stdout8 = sys.stdout
new_stdout8 = io.StringIO()
sys.stdout = new_stdout8

join_strategies = ["merge"] 

for strategy in join_strategies:
    print(f"Executing query with {strategy} join strategy")
    query4_hne.query4(df, data5, strategy)

captured_output8 = new_stdout8.getvalue()

sys.stdout = old_stdout8

output_path8 = f"/home/user/opt/output/output8.csv"
with open(output_path8, 'w') as file:
    file.write(captured_output8)

spark.stop()
