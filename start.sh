#!/bin/bash

#using python virtual env
source $PYTHON_VENV_DIR/bin/activate

#install requirements 
pip install -r requirements.txt

python3 source-data-interface.py &
python3 passenger-svc.py &
