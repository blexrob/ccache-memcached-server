# WebHare as a docker
FROM       fedora:25

RUN (curl --silent --location https://rpm.nodesource.com/setup_7.x | bash - ) && \
  dnf install -y nodejs && \
  dnf upgrade -y && \
  dnf clean all && \
  rm -rf /var/cache/dnf/ && \
  npm install -g yarn && \
  npm cache clear

EXPOSE 11211
CMD node /opt/server/index.js

COPY package.json yarn.lock /opt/server/
RUN cd /opt/server && yarn install

COPY src/*.js /opt/server/
