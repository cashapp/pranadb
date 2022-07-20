// Copyright (C) 2016  Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package common

import "unsafe"

// Make goimports import the unsafe package, which is required to be able
// to use //go:linkname
var _ = unsafe.Sizeof(0) //nolint:gosec

//go:noescape
//go:linkname nanotime runtime.nanotime
func nanotime() int64

// NanoTime returns the current time in nanoseconds from a monotonic clock.
// The time returned is based on some arbitrary platform-specific point in the
// past.  The time returned is guaranteed to increase monotonically at a
// constant rate, unlike time.Now() from the Go standard library, which may
// slow down, speed up, jump forward or backward, due to NTP activity or leap
// seconds.
func NanoTime() uint64 {
	return uint64(nanotime())
}
