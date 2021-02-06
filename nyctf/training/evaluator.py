import pandas as pd
from sklearn.metrics import mean_squared_error
from .features import preprocess
from .trainer import clean_dataset

def run(batch_predictor, test_df):
    test_df = pd.read_parquet(test_df)
    test_df = clean_dataset(test_df)
    y_pred = batch_predictor.predict(test_df)
    return mean_squared_error(test_df.fare_amount, y_pred, squared=False)