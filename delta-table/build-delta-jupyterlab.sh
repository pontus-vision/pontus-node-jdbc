#!/bin/bash
set -e
DIR="$( cd "$(dirname "$0")" ; pwd -P )"
cd "$DIR"/
chmod -R a+w . || true
cd docker
docker build -t pontus-delta-docker:latest .
docker push pontus-delta-docker:latest
