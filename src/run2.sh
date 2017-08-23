#!/bin/bash
nohup python3 -u prm.py 2 cloud_setup.txt &
nohup python3 -u mapper.py 3 cloud_setup.txt &
nohup python3 -u mapper.py 4 cloud_setup.txt &
nohup python3 -u reducer.py 2 cloud_setup.txt &
python3 cli.py 2 cloud_setup.txt 
