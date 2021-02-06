import os
import subprocess
from pathlib import Path


def generate_cluster_config(cluster_name, bucket):
    config = {
        'Name': cluster_name,
        'ReleaseLabel': 'emr-6.0.0',
        'Applications': [{'Name': 'Hadoop'}, {'Name': 'Spark'}],
        'BootstrapActions': [{
            'Name': 'Install Model Package',
            'ScriptBootstrapAction': {
                'Args': [f'{bucket}/package/nyctf-0.0.1.tar.gz'],
                'Path': f'{bucket}/scripts/install-model-package.sh'
            }
        }],
        'Configurations': [
            {
                'Classification': 'spark-env',
                'Configurations': [{
                    'Classification': 'export',
                    'Properties': {
                        'PYSPARK_PYTHON': '/usr/bin/python3',
                    }
                }]
            }
        ],
        'Instances': {
            'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm4.large',
                'InstanceCount': 1,
            },
            {
                'Name': 'Worker node',
                'Market': 'SPOT',
                'InstanceRole': 'CORE',
                'InstanceType': 'm4.large',
                'InstanceCount': 2,
            }
        ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        'VisibleToAllUsers': True,
        'JobFlowRole': 'EMR_EC2_DefaultRole',
        'ServiceRole': 'EMR_DefaultRole',
        'LogUri': f'{bucket}/logs/'
    }

    return config

def generate_job_config(job_name, filepath, args=None):
    if not args:
        args = []
    return [{
        'Name': job_name,
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--deploy-mode', 'cluster', filepath] + args
        }
    }]

def upload_folder_to_s3(s3, path, bucket):
    for root, dirs, files in os.walk(path):
        for f in files:
            path = os.path.join(root, f)
            s3.upload_file(path, bucket.strip("s3://"), path)

def upload_to_bucket(s3, path, bucket, prefix=None):
    if Path(path).is_dir():
        upload_folder_to_s3(s3, path, bucket)
    else:
        s3.upload_file(path, bucket.strip("s3://"), f"{prefix}/{os.path.basename(path)}")

def build_package():
    subprocess.check_call([
        "python", "setup.py", "sdist"
    ], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)