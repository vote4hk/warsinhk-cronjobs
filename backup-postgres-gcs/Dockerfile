FROM postgres:10-alpine

RUN apk add --update \
  python \
  curl \
  bash \
  gzip

RUN curl -sSL https://sdk.cloud.google.com | bash

ENV PATH $PATH:/root/google-cloud-sdk/bin

WORKDIR /opt

COPY . .

ENTRYPOINT ./run.sh
