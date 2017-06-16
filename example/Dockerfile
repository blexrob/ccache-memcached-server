FROM fedora:25
MAINTAINER Arnold Hendriks <arnold@webhare.com>

# Build the patched ccache
RUN dnf install -y asciidoc autoconf libmemcached libmemcached-devel git gcc && \
  dnf mark install hostname initscripts iputils && \
  git clone https://github.com/WebHare/ccache.git /tmp/ccache && \
  cd /tmp/ccache && \
  git checkout memcached-scopefix && \
  ./autogen.sh &&  \
  ./configure --enable-memcached && \
  make && \
  make install && \
  rm -rf /tmp/ccache && \
  dnf remove -y asciidoc autoconf libmemcached-devel && \
  dnf clean all && \
  rm -rf /var/cache/dnf/

# Get something to build
RUN curl https://www.openssl.org/source/openssl-1.1.0f.tar.gz | tar zx -C /tmp/

# Force memcache-only use, no point in using local storage ?
ENV CCACHE_MEMCACHED_ONLY=true

# Allow CCACHE buildargs to be specified
ARG CCACHE_MEMCACHED_CONF

# For debugging
ARG CACHEBUST=1
RUN if [ "$CCACHE_MEMCACHED_CONF" == "" ]; then echo 'Missing config!'; exit 1; else /usr/local/bin/ccache -p -s ; fi # $CACHEBUST

# build it!
RUN cd /tmp/openssl*/ && CC="/usr/local/bin/ccache gcc" ./config && time make

# stats?
RUN /usr/local/bin/ccache -p -s
