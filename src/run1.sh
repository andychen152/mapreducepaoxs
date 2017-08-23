#!/bin/bash
nohup python3 -u prm.py 1 cloud_setup.txt &
nohup python3 -u mapper.py 1 cloud_setup.txt &
nohup python3 -u mapper.py 2 cloud_setup.txt &
nohup python3 -u reducer.py 1 cloud_setup.txt &
python3 cli.py 1 cloud_setup.txt 
