FROM postgres:10-alpine

RUN apk add --no-cache --update \
		bash \
		curl \
		gcc \
		gzip \
		libffi-dev \
		linux-headers \
		make \
		musl-dev \
		openssl-dev \
		py3-pip \
		python3 \
		python3-dev

ENV AZURE_CLI_VERSION 2.0.80
RUN pip3 --no-cache-dir install azure-cli==${AZURE_CLI_VERSION}

WORKDIR /opt

COPY . .

ENTRYPOINT ./run.sh
