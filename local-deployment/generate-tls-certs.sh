#!/usr/bin/env bash

set -eux -o pipefail

function init() {
  DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
  CERTS_DIR=$(mktemp -d -t prana)
  mkdir ${CERTS_DIR}/ca
  mkdir ${CERTS_DIR}/server
  mkdir ${CERTS_DIR}/client
}

function cleanup() {
  rm -rf "${CERTS_DIR}"
}
trap cleanup EXIT

function generate_ca_cert() {
    openssl req -newkey rsa:2048 \
    -new -nodes -x509 \
    -days 365 \
    -out ${CERTS_DIR}/ca/ca.crt \
    -keyout ${CERTS_DIR}/ca/ca.key \
    -subj "/C=US/ST=Earth/L=San Francisco/O=PranaDB Inc CA/OU=Security/CN=localhost"
}

function generate_server_cert() {
    openssl genrsa -out ${CERTS_DIR}/server/server.key 2048

    openssl req -new -key ${CERTS_DIR}/server/server.key -days 365 -out ${CERTS_DIR}/server/server.csr \
    -subj "/C=US/ST=Earth/L=San Francisco/O=SQ/OU=PranaDB Inc/CN=localhost"

    #sign it with Root CA
    openssl x509  -req -in ${CERTS_DIR}/server/server.csr \
    -extfile <(printf "subjectAltName=DNS:localhost") \
    -CA ${CERTS_DIR}/ca/ca.crt -CAkey ${CERTS_DIR}/ca/ca.key  \
    -days 365 -sha256 -CAcreateserial \
    -out ${CERTS_DIR}/server/server.crt
}

function generate_client_cert() {
    openssl genrsa -out ${CERTS_DIR}/client/client.key 2048

    openssl req -new -key ${CERTS_DIR}/client/client.key -days 365 -out ${CERTS_DIR}/client/client.csr \
    -subj "/C=US/ST=Earth/L=San Francisco/O=SQ/OU=Client/CN=localhost"

    #sign it with Root CA
    openssl x509  -req -in ${CERTS_DIR}/client/client.csr \
    -extfile <(printf "subjectAltName=DNS:localhost") \
    -CA ${CERTS_DIR}/ca/ca.crt -CAkey ${CERTS_DIR}/ca/ca.key  \
    -days 365 -sha256 -CAcreateserial \
    -out ${CERTS_DIR}/client/client.crt
}

function copy_certs() {
    cp ${CERTS_DIR}/ca/ca.crt ${CERTS_DIR}/server/server.crt ${CERTS_DIR}/server/server.key ${DIR}/certs/server
    cp ${CERTS_DIR}/ca/ca.crt ${CERTS_DIR}/client/client.crt ${CERTS_DIR}/client/client.key ${DIR}/certs/client
}

init
generate_ca_cert
generate_server_cert
generate_client_cert
copy_certs