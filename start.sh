#!/bin/bash

#using python virtual env
source $PYTHON_VENV_DIR/bin/activate

#install requirements 
pip install -r requirements.txt

nohup python3 source-data-interface.py > /dev/null &
nohup python3 passenger-svc.py > /dev/null &
nohup python3 flight-svc.py > /dev/null &