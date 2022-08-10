// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
Package settings is used for managing internal parameters that can be set at
compile time by expert level users. Most of those parameters can also be
overwritten by using the json mechanism described below.
*/
package settings

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/lni/dragonboat/v3/logger"
)

var (
	plog = logger.GetLogger("settings")
)

//
// Parameters in both hard.go and soft.go are _NOT_ a part of the public API.
// There is no guarantee that any of these parameters are going to be available
// in future releases. Change them only when you know what you are doing.
//
// This file contain hard configuration values that should _NEVER_ be changed
// after your system has been deployed. Changing any value here will CORRUPT
// the data in your existing deployment.
//
// We do have an mechanism to overwrite the default values for the hard struct.
// To tune these parameters, place a json file named
// dragonboat-hard-settings.json in the current working directory of your
// dragonboat application, all fields in the json file will be applied to
// overwrite the default setting values. e.g. for a json file with the
// following content -
//
// {
//   "LRUMaxSessionCount": 32,
// }
//
// hard.LRUMaxSessionCount will be set to 32
//
// The application need to be restarted to apply such configuration changes.
// Again - tuning these hard parameters using the above described json file
// will cause your existing data to be corrupted. Decide them in your dev/test
// phase, once your system is deployed in production, _NEVER_ change them.
//

// Hard is the hard settings that can not be changed after the system has been
// deployed.
var Hard = getHardSettings()

type hard struct {
	// LRUMaxSessionCount is the max number of client sessions that can be
	// concurrently held and managed by each raft cluster.
	LRUMaxSessionCount uint64
	// LogDBEntryBatchSize is the max size of each entry batch.
	LogDBEntryBatchSize uint64
}

// BlockFileMagicNumber is the magic number used in block based snapshot files.
var BlockFileMagicNumber = []byte{0x3F, 0x5B, 0xCB, 0xF1, 0xFA, 0xBA, 0x81, 0x9F}

const (
	//
	// RSM
	//

	// SnapshotHeaderSize defines the snapshot header size in number of bytes.
	SnapshotHeaderSize uint64 = 1024

	//
	// transport
	//

	// UnmanagedDeploymentID is the special deployment ID value used when no user
	// deployment ID is specified.
	UnmanagedDeploymentID uint64 = 1
	// MaxMessageBatchSize is the max size for a single message batch sent between
	// nodehosts.
	MaxMessageBatchSize uint64 = LargeEntitySize
	// SnapshotChunkSize is the snapshot chunk size.
	SnapshotChunkSize uint64 = 2 * 1024 * 1024
)

// HardHash returns the hash value of the Hard setting.
func HardHash(execShards uint64,
	logDBShards uint64, sessionCount uint64, batchSize uint64) uint64 {
	hashstr := fmt.Sprintf("%d-%d-%t-%d-%d",
		execShards,
		logDBShards,
		false, // was the UseRangeDelete option
		sessionCount,
		batchSize)
	mh := md5.New()
	if _, err := io.WriteString(mh, hashstr); err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint64(mh.Sum(nil))
}

func getHardSettings() hard {
	org := getDefaultHardSettings()
	overwriteHardSettings(&org)
	return org
}

func getDefaultHardSettings() hard {
	return hard{
		LRUMaxSessionCount:  4096,
		LogDBEntryBatchSize: 48,
	}
}
