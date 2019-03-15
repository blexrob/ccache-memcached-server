# PLEASE NOTE
*THIS PROJECT MAY NOW BE OBSOLETE*

docker's experimental buildkit seems to suffice with an unpatched ccache:

```
RUN --mount=type=cache,target=/tmp/ccache CCACHE_DIR=/tmp/ccache make
```

# ccache-memcached-server
A docker container hosting a ccache over the memcached protocol to
speed up builds

To use ccache persisted between docker builds, you need two things:
- a server (docker container) hosting the ccache server
- a patched ccache in your container to use this memcache

## The ccache server

Setup a network and launch the server:
```
docker network create ci_network
docker run --rm --name ccache --network ci_network --network-alias ccache webhare/ccache-memcached-server:latest
```

We recommend mounting a data volume over `/opt/memcache-data` so that any
cached compilations are persisted when the container is restarted, but this is
not strictly necessary.

For more info about the container, see [server/]

## The patched ccache
Your container needs to build this version of ccache: https://github.com/WebHare/ccache/tree/memcached-scopefix

(this part probably needs to be improved, either by setting up base containers
 with this ccache version preinstalled, or ideally, by getting the changes back
 into core and getting all distributions to ship them)

You will also need to make sure your build actually uses ccache, and invokes it
with the proper parameters to connect to to the memcache, ie prefix all gcc, g++
etc invocations with `ccache --SERVER=ccache`

Finally, your `docker build` instructions will also need to add `--network ci_network`

See the [example/Dockerfile] for an example of how to build and configure it.

## Example/test
```
# Run the example with ccache, forcing memcached, and ensuring the last few steps are always run
docker build --build-arg CCACHE_MEMCACHED_CONF=--SERVER=ccache \
             --build-arg CACHEBUST=$$ \
             --network ci_network \
             example
```
