#!/bin/bash

source bike/bin/activate

pip install -r requirements.txt

#TODO
# virtualenv... source ./env/bin/activate

#todo use ./env/bin/python
python crawler.py
