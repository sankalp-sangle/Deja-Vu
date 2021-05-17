#!/bin/bash

python load_records.py traces/$1.pcap
python3 preprocess.py $1
python3 app.py $1
