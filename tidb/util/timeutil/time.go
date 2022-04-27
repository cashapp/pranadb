//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package timeutil

import (
	"fmt"
	"sync"
	"time"

	"github.com/uber-go/atomic"
)

// init initializes `locCache`.
func init() {
	// We need set systemTZ when it is in testing process.
	if systemTZ.Load() == "" {
		systemTZ.Store("System")
	}
	locCa = &locCache{}
	locCa.locMap = make(map[string]*time.Location)
}

// locCa is a simple cache policy to improve the performance of 'time.LoadLocation'.
var locCa *locCache

// systemTZ is current TiDB's system timezone name.
var systemTZ atomic.String

// locCache is a simple map with lock. It stores all used timezone during the lifetime of tidb instance.
// Talked with Golang team about whether they can have some forms of cache policy available for programmer,
// they suggests that only programmers knows which one is best for their use case.
// For detail, please refer to: https://github.com/golang/go/issues/26106
type locCache struct {
	sync.RWMutex
	// locMap stores locations used in past and can be retrieved by a timezone's name.
	locMap map[string]*time.Location
}

// SystemLocation returns time.SystemLocation's IANA timezone location. It is TiDB's global timezone location.
func SystemLocation() *time.Location {
	loc, err := LoadLocation(systemTZ.Load())
	if err != nil {
		return time.Local
	}
	return loc
}

// getLoc first trying to load location from a cache map. If nothing found in such map, then call
// `time.LoadLocation` to get a timezone location. After trying both way, an error will be returned
//  if valid Location is not found.
func (lm *locCache) getLoc(name string) (*time.Location, error) {
	if name == "System" {
		return time.Local, nil
	}
	lm.RLock()
	v, ok := lm.locMap[name]
	lm.RUnlock()
	if ok {
		return v, nil
	}

	if loc, err := time.LoadLocation(name); err == nil {
		// assign value back to map
		lm.Lock()
		lm.locMap[name] = loc
		lm.Unlock()
		return loc, nil
	}

	return nil, fmt.Errorf("invalid name for timezone %s", name)
}

// LoadLocation loads time.Location by IANA timezone time.
func LoadLocation(name string) (*time.Location, error) {
	return locCa.getLoc(name)
}
