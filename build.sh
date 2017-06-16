#!/bin/sh
cd ${BASH_SOURCE%/*}
docker build -t webhare/ccache-memcached-server:latest .
docker push webhare/ccache-memcached-server:latest
