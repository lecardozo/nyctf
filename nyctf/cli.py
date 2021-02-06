import os
import boto3
import click
from .inference.server import create_server
from .training import trainer, evaluator
from . import infra

@click.group(help="Helper CLI for training, evaluating and serving the NYCTF model")
def cli():
    pass

@click.command(help="Train the model")
@click.option("--dataset-path", required=True, type=click.Path())
@click.option("--artifact-path", type=click.Path(), default="model.pkl")
def train(dataset_path, artifact_path):
    train_df, validation_df = trainer.split_dataset(dataset_path)
    trainer.run(train_df, validation_df, artifact_path)

@click.command(help="Evaluate the trained model with test data")
@click.option("--model-path", type=click.Path(), required=True)
@click.option("--test-dataset-path", type=click.Path(), required=True)
def evaluate(model_path, test_dataset_path):
    predictor = BatchPredictor.from_serialized(model_path)
    metric = evaluator.run(predictor, test_dataset_path)
    print(f"RMSE: {metric.round(3)}")

@click.command(help="Start the online predictor server.")
@click.option("--model-path", required=True, type=click.Path())
def serve(model_path):    
    server = create_server(model_path)
    server.run(host="0.0.0.0", port=8080)

@click.command("spark-inference", help="Perform batch inference in a EMR cluster")
@click.option("--bucket", help="S3 bucket where intermediate artifacts will be saved", required=True)
@click.option("--input-path", help="Path to input data for inference", required=True)
@click.option("--output-path", help="S3 path for the output predictions", required=True)
@click.option("--model-path", help="Path to a trained model (e.g. model.pkl)", required=True)
@click.option("--aws-profile", help="AWS profile", default="default")
def inference(bucket, input_path, output_path, model_path, aws_profile):
    bucket = bucket.rstrip("/")
    session = boto3.Session(profile_name=aws_profile)
    emr = session.client("emr")
    s3 = session.client("s3")

    infra.build_package()
    # Model package upload
    infra.upload_to_bucket(s3, "dist/nyctf-0.0.1.tar.gz", bucket, "package")
    # EMR cluster bootstrap actions upload
    infra.upload_to_bucket(s3, "scripts", bucket)
    # Spark job upload
    infra.upload_to_bucket(s3, "spark/inference.py", bucket, "steps")

    # Trained model upload, in case it's a local file
    if not model_path.startswith("s3://"):
        infra.upload_to_bucket(s3, model_path, bucket, "model")
        model_path = f"{bucket}/model/{os.path.basename(model_path)}"
    
    # Data for inference upload, in case it's a local file
    if not input_path.startswith("s3://"):
        infra.upload_to_bucket(s3, input_path, bucket, "inference-data")
        input_path = f"{bucket}/inference-data/{os.path.basename(input_path)}"

    # Generate cluster and jobs config
    cluster_specs = infra.generate_cluster_config("batch-inference", bucket)
    job_specs = infra.generate_job_config(
        "inference", f"{bucket}/steps/inference.py", 
        args=["--output-path", output_path, "--input-path", input_path, "--model-path", model_path]
    )

    # Create cluster and submit job
    cluster_info = emr.run_job_flow(**cluster_specs)
    step_id = emr.add_job_flow_steps(JobFlowId=cluster_info["JobFlowId"], Steps=job_specs)["StepIds"][0]
    waiter = emr.get_waiter("step_complete")
    waiter.wait(
        ClusterId=cluster_info["JobFlowId"],
        StepId=step_id,
        WaiterConfig={
            "Delay": 30,
            "MaxAttempts": 100
        }
    )


cli.add_command(train)
cli.add_command(evaluate)
cli.add_command(serve)
cli.add_command(inference)