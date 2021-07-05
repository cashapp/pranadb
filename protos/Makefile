PROTOS = $(shell find . -name '*.proto')
GENERATED = $(patsubst %.proto, %.pb.go, $(PROTOS))

.PHONY: all
all: generators $(GENERATED)
	go build ./...

.PHONY: clean
clean:
	rm -f $(GENERATED)

%.pb.go: %.proto Makefile
	protoc --go_out=plugins=grpc,paths=source_relative:. $<

# Ensure the Go protoc generators are installed.
.PHONY: generators
generators: ../.hermit/go/bin/protoc-gen-go

../.hermit/go/bin/protoc-gen-go:
	go install github.com/golang/protobuf/protoc-gen-go@latest
