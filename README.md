# nyctf

## Installation
```shell
$ pip install .
```

## Usage
This model exposes a CLI (`mlcli`) for easy interaction with different steps of the modeling pipeline.

```shell
$ mlcli --help

Usage: mlcli [OPTIONS] COMMAND [ARGS]...

  Helper CLI for training, evaluating and serving the NYCTF model

Options:
  --help  Show this message and exit.

Commands:
  evaluate         Evaluate the trained model with test data
  serve            Start the online predictor server.
  spark-inference  Perform batch inference in a EMR cluster
  train            Train the model
```

### Training
The `mlcli train` command takes a dataset as input, trains the model and save the serialized
model to the specified output location

```shell
$ mlcli train --dataset-path=data/train.csv --artifact-path=model.pkl
```

### Evaluating
For evaluating the model with unseen data, use the `mlcli evaluate` command.
This will locally predict the `fare_amount` for each instance and then calculate the root mean squared error for the dataset.
```shell
$ mlcli evaluate --model-path=model.pkl \
                 --test-dataset-path=data/test-set.parquet
```

### Inference
Here we provide two inference modes: online and batch.

#### Online
For running your model inference procedure in online mode, use the `mlcli serve` command.
This will start a server on port 8080 that exposes a `/predict` endpoint that expects to receive a POST 
request with the input data in the payload, following the schema defined in `nyctf.predictor.FeatureSchema`.
If the input data fails the schema validation, a response containing all the validation errors
will be returned. Otherwise, the response will contain the predicted `fare_amount`. Here is an example:

```shell
# Build the image
$ docker build -t nyctf .

# Start the service
$ docker run --rm -d -p 8080:8080 nyctf

# POST your data to the /predict endpoint
$ curl --location --request POST 'http://localhost:8080/predict' \
       --header 'Content-Type: application/json' \
       --data-raw '{
           "pickup_latitude": -80,
           "pickup_longitude": -19,
           "dropoff_latitude": 80,
           "dropoff_longitude": 19,
           "pickup_datetime": "2009-01-01 00:00:46+00:00"
       }'
```
#### Batch
For batch inference you can use the `mlcli spark-inference` command. 

**DISCLAIMER:** This command assumes you have a configured AWS account,
a bucket for artifacts and a role with permissions for creating/destroying EMR clusters, reading/writing data for the required buckets.

This command will:
- Package the model and send it somewhere we can access from EMR
- Start an EMR cluster with a bootstrap action that installs our packaged model and its dependencies
- Submit an inference step to the cluster

```shell
$ export BUCKET=s3://testing
$ export AWS_PROFILE=your-profile
$ mlcli spark-inference --input-path=$BUCKET/dataset.parquet \
                        --output-path=$BUCKET/predictions
                        --model-path=model.pkl
                        --bucket=$BUCKET
                        --aws-profile=$AWS_PROFILE
```
