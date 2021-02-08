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

### Evaluating
For evaluating the model with unseen data, use the `mlcli evaluate` command.

### Inference
Here we provide two inference modes: online and batch.

#### Online
For running your model inference procedure in online mode, use the `mlcli serve` command.
This will start a server on port 8080 that exposes a `/predict` endpoint that expects to receive a POST 
request with the input data in the payload, following the schema defined in `nyctf.predictor.FeatureSchema`.
If the input data fails the schema validation, a response containing all the validation errors
will be returned. Otherwise, the response will contain the predicted fare_amount

#### Batch
For batch inference you can use the `mlcli spark-inference` command. 

DISCLAIMER: This command assumes you have a configured AWS account, 
a bucket for artifacts and a role with permissions for creating/destroying EMR clusters,
for reading/writing data for the required buckets.

This command will:
- Package the model and send it somewhere we can access from EMR
- Start an EMR cluster with a bootstrap action that installs our packaged model and its dependencies
- Submit an inference step to the cluster
