import numpy as np
import pandas as pd

def haversine_distance(lon1: pd.Series, lat1: pd.Series, lon2: pd.Series, lat2: pd.Series):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    All args must be of equal length.
    
    Taken from: https://stackoverflow.com/questions/29545704/fast-haversine-approximation-python-pandas
    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km

def preprocess(data):
    data["pickup_datetime"] = pd.to_datetime(data["pickup_datetime"])
    data["hav_distance_km"] = haversine_distance(
        data["pickup_longitude"],
        data["pickup_latitude"],
        data["dropoff_longitude"],
        data["dropoff_latitude"]
    )
    data["day_of_week"] = data["pickup_datetime"].dt.dayofweek
    data["hour"] = data["pickup_datetime"].dt.hour
    return data

def spark_preprocess(df):
    from pyspark.sql.functions import col, pandas_udf, dayofweek, hour, to_timestamp, when
    from pyspark.sql.types import LongType

    df = df.withColumn("pickup_datetime", to_timestamp(df.pickup_datetime))

    haversine_distance_udf = pandas_udf(haversine_distance, returnType=LongType())
    df = df.withColumn(
        "hav_distance_km", 
        haversine_distance_udf(
            df.pickup_longitude, df.pickup_latitude, df.dropoff_longitude, df.dropoff_latitude
        )
    )

    # Here we need to adjust dayofweek to keep the same Pandas pattern (0-Mon, 1-Tue ... 6-Sun)
    df = (
        df.withColumn("day_of_week", dayofweek(df.pickup_datetime) - 2)
          .withColumn("day_of_week", when(col("day_of_week") == -1, 6)
                                     .otherwise(col("day_of_week"))
          )
    )
    df = df.withColumn("hour", hour(df.pickup_datetime))

    return df