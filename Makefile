.PHONY: all
all: protos

.PHONY: protos
protos:
	make -C protos

.PHONY: clean
clean:
	make -C protos clean
	go clean -testcache

.PHONY: test
test: protos
	go test -race -short -timeout 30s ./...

docker-image:
	docker build -f docker/Dockerfile -t pranadb:latest .

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
	go run ./cmd/prana/ shell --addr=localhost:6584

status:
	docker-compose -f local-deployment/docker-compose.yaml ps -a

