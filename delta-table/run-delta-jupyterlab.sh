#!/bin/bash
set -e
DIR="$( cd "$(dirname "$0")" ; pwd -P )"
cd "$DIR"/
chmod -R a+w . || true
cd docker
docker build -t pontus-delta-docker:latest .
cd .. 
docker run --name delta_quickstart --rm -it -p10000:10000 -p 8888-8889:8888-8889 -v ./data:/data -v ./work-dir:/opt/spark/work-dir  pontus-delta-docker:latest
