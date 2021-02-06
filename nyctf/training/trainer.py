import cloudpickle
import pandas as pd
from pathlib import Path
from sklearn.model_selection import train_test_split, GridSearchCV, PredefinedSplit
from xgboost import XGBRegressor
from .features import preprocess

RANDOM_SEED = 1234
SAMPLE_SIZE = 5542385 # roughly 10% of the data

def split_dataset(dataset_path: str):
    dirname = Path(dataset_path).parent
    if (Path(dirname, "training-set.parquet").exists() and
        Path(dirname, "validation-set.parquet").exists() and
        Path(dirname, "test-set.parquet").exists()):
        print("Reading pre-split datasets")
        training = pd.read_parquet("data/training-set.parquet")
        validation = pd.read_parquet("data/validation-set.parquet")
    else:
        print("Splitting dataset")
        data = pd.read_csv(dataset_path, nrows=SAMPLE_SIZE, parse_dates=["pickup_datetime"])
        training, test = train_test_split(data, random_state=RANDOM_SEED)
        training, validation = train_test_split(training, random_state=RANDOM_SEED)
        training.to_parquet("data/training-set.parquet")
        validation.to_parquet("data/validation-set.parquet")
        test.to_parquet("data/test-set.parquet")
    
    return training, validation


def clean_dataset(df):
    invalid_fare = (df.fare_amount <= 2.5)
    invalid_coordinates = (
        df.filter(regex="lon|lat").eq(0).any(axis=1) |
        df.filter(regex="lon|lat").isnull().any(axis=1) |
        df.filter(regex="lon").abs().gt(180).any(axis=1) |
        df.filter(regex="lat").abs().gt(90).any(axis=1)
    )
    invalid_passenger_count = df.passenger_count > 6
    df = df[~(
        invalid_coordinates | invalid_fare | invalid_passenger_count
    )]
    return df


def run(training_data: pd.DataFrame, validation_data: pd.DataFrame,
        artifact_path: str):
    training = training_data.assign(split=-1)
    validation = validation_data.assign(split=0)
    full_data = pd.concat([training, validation])
    full_data = clean_dataset(full_data)
    splits = PredefinedSplit(test_fold=full_data.split)
    param_space = {
        "max_depth": [3, 4],
        "n_estimators": [50, 100]
    }
    estimator = XGBRegressor()
    model = GridSearchCV(estimator, cv=splits, param_grid=param_space, refit=True)
    print("Generating features")
    processed_data = preprocess(full_data)
    print("Training model")
    model.fit(
        X=processed_data[["hav_distance_km", "day_of_week", "hour"]], 
        y=processed_data[["fare_amount"]]
    )

    with open(artifact_path, "wb") as out:
        cloudpickle.dump(model, out)
