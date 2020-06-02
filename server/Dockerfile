# WebHare as a docker
FROM       fedora:31

RUN dnf install -y --setopt=install_weak_deps=False nodejs npm && \
  dnf upgrade -y --setopt=install_weak_deps=False && \
  dnf clean all && \
  rm -rf /var/cache/dnf/

EXPOSE 11211
CMD node /opt/server/index.js

COPY package.json package-lock.json /opt/server/
RUN cd /opt/server && npm install && npm cache clean --force

COPY src/*.js /opt/server/
RUN mkdir -p /opt/memcache-data
VOLUME /opt/memcache-data
