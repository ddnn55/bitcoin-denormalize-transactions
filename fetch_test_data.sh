#!/usr/bin/env bash

set -e

mkdir -p "test"

curl -L "https://www.dropbox.com/s/qxkdbkbfx55bqhz/blk00900.dat?dl=1" > test/blk00900.dat
