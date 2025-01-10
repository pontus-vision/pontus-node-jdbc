#!/bin/bash
set -e
DIR="$( cd "$(dirname "$0")" ; pwd -P )"
cd "$DIR"/

python3 setup.py bdist_wheel
