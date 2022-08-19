ROOT = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
BUILD_DIR = $(ROOT)/out
CHANNEL ?= canary
VERSION ?= $(shell git describe --tags --dirty  --always)
GOOS ?= $(shell ./bin/go version | awk '{print $$NF}' | cut -d/ -f1)
GOARCH ?= $(shell ./bin/go version | awk '{print $$NF}' | cut -d/ -f2)
BIN_SERVER = $(BUILD_DIR)/pranadb-$(GOOS)-$(GOARCH)
BIN_CLI = $(BUILD_DIR)/prana-cli-$(GOOS)-$(GOARCH)

.PHONY: all protos build test docker-image start stop create-topics publish-payments connect status

all: protos build

protos:
	$(MAKE) -C ./protos

build-server: protos
	mkdir -p $(BUILD_DIR)
	go build -tags musl -o $(BIN_SERVER) $(ROOT)/cmd/pranadb
	gzip -9 $(BIN_SERVER)

build-cli: protos
	mkdir -p $(BUILD_DIR)
	go build -o $(BIN_CLI) $(ROOT)/cmd/prana
	gzip -9 $(BIN_CLI)

test: protos
	go test -race -short -timeout 30s ./...

docker-image:
	docker build -f docker-files/Dockerfile -t pranadb:latest .

start:docker-image
	docker-compose -f ./local-deployment/docker-compose.yaml up -d --remove-orphans

stop:
	docker-compose -f ./local-deployment/docker-compose.yaml down

ifeq ($(origin topic), undefined)
  topic = payments
endif
create-topics:
	docker exec -it broker kafka-topics --create --topic ${topic} --partitions 25 --bootstrap-server localhost:9092

ifeq ($(origin delay), undefined)
  delay = 10ms
endif
ifeq ($(origin num_messages), undefined)
  num_messages = 1000
endif
ifeq ($(origin index_start), undefined)
  index_start = 0
endif
publish-payments:
	go run cmd/msggen/main.go --generator-name payments --topic-name payments --partitions 25 --delay ${delay} --num-messages ${num_messages} --index-start ${index_start} --kafka-properties "bootstrap.servers"="localhost:9092"

connect:
	go run ./cmd/prana/ shell --addr=localhost:8443 --ca-cert local-deployment/certs/client/ca.crt --key local-deployment/certs/client/client.key --cert local-deployment/certs/client/client.crt

status:
	docker-compose -f local-deployment/docker-compose.yaml ps -a

