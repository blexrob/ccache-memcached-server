#!/bin/sh
${BASH_SOURCE%/*}/build.sh &&
docker push webhare/ccache-memcached-server:latest
