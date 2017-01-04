#!/bin/sh
cd ${BASH_SOURCE%/*}
source ../src/functions.sh
runstandardbuild "$@" ci-memcached
