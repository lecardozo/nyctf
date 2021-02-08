import pyspark
from nyctf.inference.predictor import BatchPredictor
from nyctf.training.features import spark_preprocess

def run(input_path: str, output_path: str, model_path: str):
    """Run batch inference given input data and trained model.

    Args:
        input_path (str): S3 Path to input data for inference
        output_path (str): S3 path for the output predictions
        model_path (str): S3 path to trained model
    """
    sc = pyspark.SparkContext.getOrCreate()
    sqlContext = pyspark.sql.SQLContext(sc)

    if input_path.endswith(".parquet"):
        df = sqlContext.read.parquet(input_path)
    elif input_path.endswith(".csv"):
        df = sqlContext.read.csv(input_path)
    else:
        ValueError("Data must be either in .parquet or .csv format")

    df = spark_preprocess(df)
    predictor = BatchPredictor.from_serialized(model_path)
    df = predictor.spark_predict(df)
    (
        df.coalesce(4)
          .write.parquet(output_path)
    )

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--output-path", help="S3 Path for the output predictions")
    parser.add_argument("--input-path", help="S3 Path to input data for inference")
    parser.add_argument("--model-path", help="S3 Path to trained model")
    args = vars(parser.parse_args())
    run(**args)