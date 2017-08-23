#!/bin/bash
nohup python3 -u prm.py 3 cloud_setup.txt &
nohup python3 -u mapper.py 5 cloud_setup.txt &
nohup python3 -u mapper.py 6 cloud_setup.txt &
nohup python3 -u reducer.py 3 cloud_setup.txt &
python3 cli.py 3 cloud_setup.txt 
