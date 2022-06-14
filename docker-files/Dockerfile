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
# and we MUST use the Confluent client (this is the default)- the segment client is only for local development as it does not handle
# rebalance well
RUN go build -tags musl -o ./out/pranadb ./cmd/pranadb

# Start fresh from a smaller image
FROM alpine:latest
RUN apk add bash

COPY --from=build_base /tmp/pranadb/out/pranadb /usr/local/bin
COPY ./docker-files/docker-entrypoint.sh /usr/local/bin

EXPOSE 63201 63301 6584

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["pranadb"]
