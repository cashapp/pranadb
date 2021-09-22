FROM golang:1.17-alpine AS build_base

RUN apk add build-base librdkafka-dev

# Set the Current Working Directory inside the container
WORKDIR /tmp/pranadb

# We want to populate the module cache based on the go.{mod,sum} files.
COPY go.mod .

RUN go mod download

COPY . .

# Build the Go app
# `-tags musl` is required by the confluent kafka library. https://github.com/confluentinc/confluent-kafka-go/issues/454
RUN go build -tags confluent -tags musl -o ./out/pranadb ./cmd/pranadb

# Start fresh from a smaller image
FROM alpine:latest
RUN apk add bash

COPY --from=build_base /tmp/pranadb/out/pranadb /usr/local/bin
COPY ./docker/docker-entrypoint.sh /usr/local/bin

EXPOSE 63201 63301 6584

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["pranadb"]
