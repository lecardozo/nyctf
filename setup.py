from setuptools import setup, find_packages

SERVER_DEPENDENCIES = [
    "flask"
]

setup(
    name="nyctf",
    version="0.0.1",
    packages=find_packages(exclude=["test*"]),
    install_requires=[
        "pandas==1.1.4",
        "scikit-learn==0.24.1",
        "pyarrow==3.0.0",
        "cloudpickle==1.6.0",
        "xgboost==1.3.3",
        "smart_open",
        "boto3",
        "marshmallow"
    ],
    extras_require={
        "server": SERVER_DEPENDENCIES,
    },
    entry_points={"console_scripts": ["mlcli=nyctf.cli:cli"]}
)
