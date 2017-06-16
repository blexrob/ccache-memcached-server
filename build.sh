#!/bin/sh
cd ${BASH_SOURCE%/*}
docker build --squash -t webhare/ccache-memcached-server:latest server
