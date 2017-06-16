# ccache-memcached-server
A docker container hosting a ccache over the memcached protocol to
speed up builds

## How to speed up docker builds
Launch the server:
```
docker run -P 11211 webhare/ccache-memcachedserver:latest
```

We recommend mounting a data volume over `/opt/memcache-data` so that any
cached compilations are persisted when the container is restarted, but this is
not strictly necessary.
