#!/bin/bash

set -e
DIR="$( cd "$(dirname "$0")" ; pwd -P )"
cd "$DIR"

export CLASSPATH="$(ls $(pwd)/jars/*|xargs|sed -e 's/ /,/g' )"


