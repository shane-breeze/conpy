#!/bin/bash
DIR=$(readlink -f $(dirname ${BASH_SOURCE[0]}))
export PATH=$DIR/conpy:$PATH
export PYTHONPATH=$DIR:$PYTHONPATH
