#!/bin/bash
set -e
DIR="$( cd "$(dirname "$0")" ; pwd -P )"
cd "$DIR"/

coverage run --source=../udl/ -m behave -e 'features/.ipynb_checkpoints/*' $* 
#coverage run --source=../udl/ -m behave --no-capture
coverage report -m
