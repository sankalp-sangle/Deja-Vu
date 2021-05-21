#!/bin/bash

./wait-for-it.sh -t 30 mysql-server:3306 -- echo "MySQL is up"
python load_records.py traces/$1.pcap
python3 preprocess.py $1
python3 app.py $1
