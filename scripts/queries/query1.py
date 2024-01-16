from pyspark.sql.functions import year, month, col, desc, dense_rank, to_date
from pyspark.sql.window import Window


def query1_df(df):
    crime_date = df.withColumn("year", year("DATE OCC")).withColumn("month", month("DATE OCC"))

    count = crime_date.groupBy("year", "month").count().withColumnRenamed("count", "crime_total")

    window_spec = Window.partitionBy("year").orderBy(desc("crime_total"))
    top_months = count.withColumn("#", dense_rank().over(window_spec)).filter(col("#") <= 3)

    top_months = top_months.orderBy("year", "#")

    return top_months

def query1_sql(df):
    crime_date = df.withColumn("year", year("DATE OCC")).withColumn("month", month("DATE OCC"))

    # Δημιουργία προσωρινής προβολής
    crime_date.createOrReplaceTempView("crimes")

    # SQL ερώτημα για την εύρεση των τριών μηνών με τον υψηλότερο αριθμό εγκλημάτων ανά έτος
    query1 = """
    SELECT year, month, crime_total, `#` 
    FROM (
        SELECT year, month, count(*) AS crime_total, 
               DENSE_RANK() OVER (PARTITION BY year ORDER BY count(*) DESC) AS `#`
        FROM crimes
        GROUP BY year, month
    ) 
    WHERE `#` <= 3
    ORDER BY year, `#`
    """

    top_months = crime_date.sparkSession.sql(query1)

    return top_months
