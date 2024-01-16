from pyspark.sql.functions import row_number, count, min, udf, year, month, regexp_replace, col, desc, dense_rank, to_date
from pyspark.sql.window import Window
import math
from pyspark.sql import functions as F


def query4(df, data5, join_strategy):

    def haversine(lat1, lon1, lat2, lon2):
        # Radius of the Earth in kilometers
        R = 6371.0

        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)

        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        distance = R * c
        return distance

    def get_distance(lat1, long1, lat2, long2):

        def is_valid_coordinate(lat, lon):
            return -90 <= lat <= 90 and -180 <= lon <= 180


        if not is_valid_coordinate(lat1, long1) or not is_valid_coordinate(lat2, long2):
            # Print the invalid rows
            print(f"Invalid row: lat1={lat1}, long1={long1}, lat2={lat2}, long2={long2}")
            return -1

        try:
            return haversine(lat1, long1, lat2, long2)
        except ValueError:
            return -1

    df = df.filter(
        (df["AREA NAME"] != "Null Island") &
        (df["Weapon Used Cd"].substr(1, 1) == "1")
    )

    joined_df = df.join(data5.hint(join_strategy), df["AREA"] == data5["PREC"])

    joined_df.explain()

    distance_udf = udf(get_distance)

    distance_df = joined_df.withColumn(
        "DISTANCE",
        distance_udf(
            F.col("LAT"), F.col("LON"),
            F.col("Y"), F.col("X")
        ).cast("double")
    )

    query_4_1a = distance_df.groupBy("Year").agg(
        F.avg("DISTANCE").alias("average_distance"),
        F.count("*").alias("#")
    ).orderBy("Year").withColumnRenamed("Year", "year")

    query_4_1b = distance_df.groupBy("DIVISION").agg(
        F.avg("DISTANCE").alias("average_distance"),
        F.count("*").alias("#")
    ).orderBy(F.desc("#")).withColumnRenamed("DIVISION", "division")

    print("Απόσταση από το αστυνομικό τμήμα που ανέλαβε την έρευνα για το περιστατικό:")
    print("(a)")
    query_4_1a.show()
    print("(b)")
    query_4_1b.show()

    cross_joined_df = df.crossJoin(data5.withColumnRenamed("LAT", "Y").withColumnRenamed("LON", "X"))


    cross_joined_df = cross_joined_df.withColumn(
        "DISTANCE",
        distance_udf(col("LAT"), col("LON"), col("Y"), col("X")).cast("double")
    )

    windowSpec = Window.partitionBy("DR_NO").orderBy("DISTANCE")

    nearest_station_df = cross_joined_df.withColumn(
        "row_num",
        F.row_number().over(windowSpec)
    ).filter(col("row_num") == 1).drop("row_num")
    
    query_4_2a = nearest_station_df.groupBy("Year").agg(
        F.avg("DISTANCE").alias("average_distance"),
        F.count("*").alias("#")
    ).orderBy("Year").withColumnRenamed("Year", "year")

    query_4_2b = nearest_station_df.groupBy("DIVISION").agg(
        F.avg("DISTANCE").alias("average_distance"),
        F.count("*").alias("#")
    ).orderBy(F.desc("#")).withColumnRenamed("DIVISION", "division")

    print("Απόσταση από το πλησιέστερο αστυνομικό τμήμα:")
    print("(a)")
    query_4_2a.show()
    print("(b)")
    query_4_2b.show()

        
