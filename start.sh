#!/bin/bash

source bike/bin/activate

./bike/bin/pip install -r requirements.txt

./bike/bin/python crawler.py
