#!/bin/bash

#using python virtual env
source $PYTHON_VENV_DIR/bin/activate

#install requirements 
pip install -r requirements.txt

#start services
nohup python3 data-late.py > /dev/null &
sleep 5
nohup python3 source-data-interface.py > /dev/null &
nohup python3 passenger-svc.py > /dev/null &
nohup python3 flight-svc.py > /dev/null &
nohup python3 facial-svc.py > /dev/null &
nohup python3 satellite1.py > /dev/null &
nohup python3 satellite2.py > /dev/null &
nohup python3 satellite3.py > /dev/null &