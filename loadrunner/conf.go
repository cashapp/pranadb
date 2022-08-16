package loadrunner

import (
	"fmt"
	"github.com/squareup/pranadb/errors"
)

// Config represents configuration for the load runner
type Config struct {
	NodeID                   int
	APIServerListenAddresses []string
}

func (c *Config) Validate() error { //nolint:gocyclo
	if c.NodeID < 0 {
		return errors.NewInvalidConfigurationError("NodeID must be >= 0")
	}
	if c.NodeID >= len(c.APIServerListenAddresses) {
		return errors.NewInvalidConfigurationError(fmt.Sprintf("Node id is %d but there are only %d GRPCAPIServerListenAddresses", c.NodeID, len(c.APIServerListenAddresses)))
	}
	return nil
}
