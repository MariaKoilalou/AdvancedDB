from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


def query2_df(df):

    def day_part(hour):
        if 500 <= hour < 1200:
            return "Πρωί"
        elif 1200 <= hour < 1700:
            return "Απόγευμα"
        elif 1700 <= hour < 2100:
            return "Βράδυ"
        else:
            return "Νύχτα"

    day_part_udf = udf(day_part, StringType())

    df_day_part = df.withColumn("DayPart", day_part_udf(col("TIME OCC")))

    df_street_crimes = df_day_part.filter(col("Premis Desc") == "STREET").groupBy("DayPart").count().orderBy(col("count").desc())

    return df_street_crimes


def query2_rdd(df):

    def day_part(hour):
        if 500 <= hour < 1200:
            return "Πρωί"
        elif 1200 <= hour < 1700:
            return "Απόγευμα"
        elif 1700 <= hour < 2100:
            return "Βράδυ"
        else:
            return "Νύχτα"

    rdd = df.rdd.filter(lambda row: row['Premis Desc'] == 'STREET')

    def map_day_part(record):
        hour = int(record["TIME OCC"])
        part = day_part(hour)
        return (part, 1)

    rdd_mapped = rdd.map(map_day_part)
    rdd_reduced = rdd_mapped.reduceByKey(lambda a, b: a + b)

    rdd_street_crimes = rdd_reduced.sortBy(lambda x: x[1], ascending=False)

    return rdd_street_crimes 
