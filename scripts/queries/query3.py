from pyspark.sql.functions import count,col, udf
from pyspark.sql.types import StringType

def query3 (crime_2015, data3, revgecoding):

    crime_zip = crime_2015.join(revgecoding, ["LAT", "LON"], "left")

    best3_zip = data3.orderBy("Estimated Median Income", ascending=False).limit(3)
    worst3_zip = data3.orderBy("Estimated Median Income", ascending=True).limit(3)
    
    best3_zip_list = [row['Zip Code'] for row in best3_zip.collect()] 
    worst3_zip_list = [row['Zip Code'] for row in worst3_zip.collect()]

    crimes = crime_zip.filter(
        (col("ZIPcode").isin(best3_zip_list)) | 
        (col("ZIPcode").isin(worst3_zip_list))
    )
    
    vict_descent_count = crimes.groupBy("Vict Descent").count().orderBy("count", ascending=False)


    return vict_descent_count
	
	
