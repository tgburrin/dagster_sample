#!/usr/bin/env bash

CWD=${PWD}
cd `dirname $0`

docker build . -t dagster-custom

cd $CWD
