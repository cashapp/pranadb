package common

import (
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/errors"
)

func LogInternalError(err error) errors.PranaError {
	id, err2 := uuid.NewRandom()
	var errRef string
	if err2 != nil {
		log.Errorf("failed to generate uuid %v", err)
		errRef = ""
	} else {
		errRef = id.String()
	}
	// For internal errors we don't return internal error messages to the CLI as this would leak
	// server implementation details. Instead, we generate a random UUID and add that to the message
	// and log the internal error in the server logs with the UUID so it can be looked up
	log.Errorf("error - creating internal error with ref %s", errRef)
	perr := errors.NewInternalError(errRef)
	log.Errorf("internal error occurred with reference %s\n%+v", errRef, err)
	return perr
}
