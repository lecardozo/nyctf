import cloudpickle
import pandas as pd
from smart_open import open
from marshmallow import Schema, fields, validate, post_load

from ..training.features import preprocess

class FeatureSchema(Schema):
    pickup_latitude = fields.Float(validate=validate.Range(min=-90, max=90), required=True)
    pickup_longitude = fields.Float(validate=validate.Range(min=-180, max=180), required=True)
    dropoff_latitude = fields.Float(validate=validate.Range(min=-90, max=90), required=True)
    dropoff_longitude = fields.Float(validate=validate.Range(min=-180, max=180), required=True)
    pickup_datetime = fields.DateTime(required=True)

    @post_load
    def to_pandas(self, data, **kwargs):
        data_dict = {k:[v] for k,v in data.items()}
        return pd.DataFrame(data_dict)


class Predictor:

    @classmethod
    def from_serialized(cls, path: str):
        with open(path, "rb") as f:
            model_instance = cloudpickle.load(f)
        return cls(model_instance)
    
    def __init__(self, model_instance):
        self.model = model_instance


class OnlinePredictor(Predictor):

    def predict(self, features: FeatureSchema):
        preprocessed = preprocess(features)
        output = self.model.predict(
            preprocessed[["hav_distance_km", "day_of_week", "hour"]]
        )
        return {
            "fare_amount": float(output[0])
        }


class BatchPredictor(Predictor):

    def _spark_predict(self, hav_distance_km: pd.Series, day_of_week: pd.Series, hour: pd.Series) -> pd.Series:
        data = pd.DataFrame({
            "hav_distance_km": hav_distance_km,
            "day_of_week": day_of_week,
            "hour": hour
        })

        return pd.Series(self.model.predict(data))
    
    def spark_predict(self, features_df):
        from pyspark.sql.functions import col, pandas_udf
        from pyspark.sql.types import DoubleType
        predict_udf = pandas_udf(self._spark_predict, returnType=DoubleType())
        df = features_df.withColumn(
            "predictions", 
            predict_udf(col("hav_distance_km"), col("day_of_week"), col("hour"))
        )
        return df

    def predict(self, features_df: pd.DataFrame):
        preprocessed = preprocess(features_df)
        output = self.model.predict(
            preprocessed[["hav_distance_km", "day_of_week", "hour"]]
        )
        return output