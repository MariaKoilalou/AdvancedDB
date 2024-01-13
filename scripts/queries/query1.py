from pyspark.sql.functions import year, month, col, desc, dense_rank, to_date
from pyspark.sql.window import Window


def query1_df(df):
    crime_date = df.withColumn("Year", year("DATE OCC")).withColumn("Month", month("DATE OCC"))

    count = crime_date.groupBy("Year", "Month").count()

    window_spec = Window.partitionBy("Year").orderBy(desc("count"))
    top_months = count.withColumn("rank", dense_rank().over(window_spec)).filter(col("rank") <= 3)

    top_months = top_months.orderBy("Year", "rank")

    return top_months

def query1_sql(df):
    crime_date = df.withColumn("Year", year("DATE OCC")).withColumn("Month", month("DATE OCC"))

    # Δημιουργία προσωρινής προβολής
    crime_date.createOrReplaceTempView("crimes")

    # SQL ερώτημα για την εύρεση των τριών μηνών με τον υψηλότερο αριθμό εγκλημάτων ανά έτος
    query1 = """
    SELECT Year, Month, count, rank 
    FROM (
        SELECT Year, Month, count(*) AS count, 
               DENSE_RANK() OVER (PARTITION BY Year ORDER BY count(*) DESC) AS rank
        FROM crimes
        GROUP BY Year, Month
    ) 
    WHERE rank <= 3
    ORDER BY Year, rank
    """

    top_months = crime_date.sparkSession.sql(query1)

    return top_months
