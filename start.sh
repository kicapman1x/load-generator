#!/bin/bash
MODE=$1
SUB_MODE=$2
#using python virtual env
source $PYTHON_VENV_DIR/bin/activate

#install requirements 
pip install -r requirements.txt

#start services
if [ "$MODE" == "all" ]; then
    nohup python3 source-data-interface.py subseq > /dev/null &
    nohup python3 passenger-svc.py > /dev/null &
    nohup python3 flight-svc.py > /dev/null &
    nohup python3 facial-svc.py > /dev/null &
    nohup python3 satellite-interface.py > /dev/null &
    nohup python3 satellite1.py > /dev/null &
    nohup python3 satellite2.py > /dev/null &
    nohup python3 satellite3.py > /dev/null &
    nohup python3 housekeep.py > /dev/null &
elif [ "$MODE" == "silo" ]; then
    case $SUB_MODE in
        "data-lake")
            python3 data-lake.py
            ;;
        "data-interface")
            python3 source-data-interface.py
            ;;
        "passenger")
            python3 passenger-svc.py
            ;;
        "flight")
            python3 flight-svc.py
            ;;
        "facial")
            python3 facial-svc.py
            ;;
        "satellite-interface")
            python3 satellite-interface.py
            ;;
        "s1")
            python3 satellite1.py
            ;;
        "s2")
            python3 satellite2.py
            ;;
        "s3")
            python3 satellite3.py
            ;;
        "housekeep")
            python3 housekeep.py
            ;;
        *)
            echo "Invalid application name. Please provide a valid application to start."
            ;;
    esac
else
    echo "Invalid mode. Please approach Daddy Han for further instructions."
fi