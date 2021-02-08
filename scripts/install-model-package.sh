#!/bin/bash

aws s3 cp $1 package.tar.gz
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install --upgrade setuptools
sudo python3 -m pip install package.tar.gz
sudo python3 -m pip install --upgrade pyarrow==0.15.1