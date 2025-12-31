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
            nohup python3 data-lake.py > /dev/null &
            ;;
        "data-interface")
            nohup python3 source-data-interface.py > /dev/null &
            ;;
        "passenger")
            nohup python3 passenger-svc.py > /dev/null &
            ;;
        "flight")
            nohup python3 flight-svc.py > /dev/null &
            ;;
        "facial")
            nohup python3 facial-svc.py > /dev/null &
            ;;
        "satellite-interface")
            nohup python3 satellite-interface.py > /dev/null &
            ;;
        "s1")
            nohup python3 satellite1.py > /dev/null &
            ;;
        "s2")
            nohup python3 satellite2.py > /dev/null &
            ;;
        "s3")
            nohup python3 satellite3.py > /dev/null &
            ;;
        *)
            echo "Invalid application name. Please provide a valid application to start."
            ;;
    esac
else
    echo "Invalid mode. Please approach Daddy Han for further instructions."
fi