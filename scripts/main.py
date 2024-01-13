from session import create_spark_session
from pyspark.sql.functions import udf, year, month, regexp_replace, col, desc, dense_rank, to_date
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from queries import query1, query2, query3, query4
import io
import sys, time

# Redirect the standard output
old_stdout1 = sys.stdout
new_stdout1 = io.StringIO()
sys.stdout = new_stdout1

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

data3 = data3.withColumn("Estimated Median Income", regexp_replace(col("Estimated Median Income"), "\$", ""))
data3 = data3.withColumn("Estimated Median Income", regexp_replace(col("Estimated Median Income"), ",", "").cast("float"))

df.count()
print(f"Total number of rows: {df.count()}")

df.printSchema()

# Get the string from the StringIO object
captured_output1 = new_stdout1.getvalue()

# Reset the standard output
sys.stdout = old_stdout1

output_path1 = "/home/user/opt/output/output1.csv"

# Save the captured output to a file
with open(output_path1, 'w') as file:
    file.write(captured_output1)

old_stdout2 = sys.stdout
new_stdout2 = io.StringIO()
sys.stdout = new_stdout2

q1df_start = time.time()
q1df_results = query1.query1_df(df)
q1df_end = time.time()

q1df_results.show()

print(f'Q1 Dataframe time: {q1df_end-q1df_start} seconds.')

q1sql_start = time.time()
q1sql_results = query1.query1_sql(df)
q1sql_end = time.time()

q1sql_results.show()

print(f'Q1 SQL time: {q1sql_end-q1sql_start} seconds.')

captured_output2 = new_stdout2.getvalue()

sys.stdout = old_stdout2

output_path2 = "/home/user/opt/output/output2.csv"
with open(output_path2, 'w') as file:
    file.write(captured_output2)

old_stdout3 = sys.stdout
new_stdout3 = io.StringIO()
sys.stdout = new_stdout3

q2df_start = time.time()
q2df_results = query2.query2_df(df)
q2df_end = time.time()

q2df_results.show()

print(f'Q2 Dataframe time: {q2df_end-q2df_start} seconds.')

q2rdd_start = time.time()
q2rdd_results = query2.query2_rdd(df)
q2rdd_end = time.time()

print(q2rdd_results.take(4))

print(f'Q2 RDD time: {q2rdd_end-q2rdd_start} seconds.')

captured_output3 = new_stdout3.getvalue()

sys.stdout = old_stdout3

output_path3 = "/home/user/opt/output/output3.csv"
with open(output_path3, 'w') as file:
    file.write(captured_output3)

spark.stop()

for num_executors in [2, 3, 4]:
    

    spark = create_spark_session(num_executors)
    
    old_stdout4 = sys.stdout
    new_stdout4 = io.StringIO()
    sys.stdout = new_stdout4
    
    data1 = spark.read.csv("/user/ubuntu/ta/advanced-db/data/crime_data_2010.csv", header=True, schema=schema1)
    data2 = spark.read.csv("/user/ubuntu/ta/advanced-db/data/crime_data_2020.csv", header=True, schema=schema1)
    data3 = spark.read.csv("/user/ubuntu/ta/advanced-db/data/LA_income_2015.csv", header=True, schema=schema2)
    data4 = spark.read.csv("/user/ubuntu/ta/advanced-db/data/revgecoding.csv", header=True, schema=schema3)

    
    df = data1.union(data2).distinct()

    df = df.withColumn("Date Rptd", to_date(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a")) \
       .withColumn("DATE OCC", to_date(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))

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
    
    q3_start = time.time()
    q3_results = query3.query3(crime_2015, data3, revgecoding)
    q3_end = time.time()

    q3_results.show()
    print(f'Number of Executors: {num_executors}')
    print(f'Q3 time: {q3_end-q3_start} seconds.')

    captured_output4 = new_stdout4.getvalue()

    sys.stdout = old_stdout4

    output_path4 = f"/home/user/opt/output/output4_executors_{num_executors}.csv"
    with open(output_path4, 'w') as file:
       file.write(captured_output4)
  
    spark.stop()


spark = create_spark_session(4)
   
old_stdout5 = sys.stdout
new_stdout5 = io.StringIO()
sys.stdout = new_stdout5

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

query4.query4(df,data5)

captured_output5 = new_stdout5.getvalue()

sys.stdout = old_stdout5

output_path5 = f"/home/user/opt/output/output5.csv"
with open(output_path5, 'w') as file:
    file.write(captured_output5)

spark.stop()
