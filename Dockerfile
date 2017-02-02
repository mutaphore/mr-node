FROM node:v6

LABEL maintainer "deweichen47@gmail.com"

USER root

RUN mkdir -p /src/mr-node
COPY . /src/mr-node/

WORKDIR /src/mr-node
RUN npm i
