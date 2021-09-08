// +build !confluent

package source

import "github.com/squareup/pranadb/kafka"

var ClientFactory = kafka.NewSegmentIOMessageProviderFactory
