# WebHare as a docker
FROM       fedora:25

RUN (curl --silent --location https://rpm.nodesource.com/setup_7.x | bash - ) && \
  (curl --silent https://dl.yarnpkg.com/rpm/yarn.repo -o /etc/yum.repos.d/yarn.repo ) && \
  dnf install -y nodejs yarn && \
  dnf upgrade -y && \
  dnf clean all && \
  rm -rf /var/cache/dnf/

EXPOSE 11211
CMD node /opt/server/index.js

COPY package.json yarn.lock /opt/server/
RUN cd /opt/server && yarn install && yarn cache clean

COPY src/*.js /opt/server/
