#!/bin/bash

file=$1

python3 step1_threshold.py -l error -f $1 -t 0.1 -a 5
python3 step1_threshold.py -l error -f $1 -t 0.1 -a 10

python3 step1_threshold.py -l error -f $1 -t 0.05 -a 5
python3 step1_threshold.py -l error -f $1 -t 0.05 -a 10

python3 step1_threshold.py -l error -f $1 -t 0.01 -a 5
python3 step1_threshold.py -l error -f $1 -t 0.01 -a 10

python3 step1_ganz.py -l error -f $1 -t 0.05
python3 step1_ganz.py -l error -f $1 -t 0.1
python3 step1_ganz.py -l error -f $1 -t 0.2


echo "Threshold methods are done, Calculating properties"

python3 step2_property.py -m threshold -f $1 -t 0.1 -a 5 > threshold.txt
python3 step2_property.py -m threshold -f $1 -t 0.1 -a 10 >> threshold.txt
python3 step2_property.py -m threshold -f $1 -t 0.05 -a 5 >> threshold.txt
python3 step2_property.py -m threshold -f $1 -t 0.05 -a 10 >> threshold.txt
python3 step2_property.py -m threshold -f $1 -t 0.01 -a 5 >> threshold.txt
python3 step2_property.py -m threshold -f $1 -t 0.01 -a 10 >> threshold.txt

python3 step2_property.py -m ganz -f $1 -t 0.05  > ganz.txt
python3 step2_property.py -m ganz -f $1 -t 0.1  >> ganz.txt
python3 step2_property.py -m ganz -f $1 -t 0.2  >> ganz.txt

echo "Finished!"