#!/bin/bash

echo "Stopping data-lake service..."
kill -15 $(ps -ef | grep -i py | grep -i 'data-lake' | grep -v grep | awk '{print $2}')

echo "Stopping source-data-interface service..."
kill -15 $(ps -ef | grep -i py | grep -i 'source-data-interface' | grep -v grep | awk '{print $2}')

echo "Stopping passenger-svc service..."
kill -15 $(ps -ef | grep -i py | grep -i 'passenger-svc' | grep -v grep | awk '{print $2}')

echo "Stopping flight-svc service..."
kill -15 $(ps -ef | grep -i py | grep -i 'flight-svc' | grep -v grep | awk '{print $2}')

echo "Stopping facial-svc service..."
kill -15 $(ps -ef | grep -i py | grep -i 'facial-svc' | grep -v grep | awk '{print $2}')

echo "Stopping satellite-interface service..."
kill -15 $(ps -ef | grep -i py | grep -i 'satellite-interface' | grep -v grep | awk '{print $2}')

echo "Stopping satellite1 service..."
kill -15 $(ps -ef | grep -i py | grep -i 'satellite1' | grep -v grep | awk '{print $2}')

echo "Stopping satellite2 service..."
kill -15 $(ps -ef | grep -i py | grep -i 'satellite2' | grep -v grep | awk '{print $2}')

echo "Stopping satellite3 service..."
kill -15 $(ps -ef | grep -i py | grep -i 'satellite3' | grep -v grep | awk '{print $2}')

echo "All services stopped."