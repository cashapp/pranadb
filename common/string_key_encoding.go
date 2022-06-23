// Copyright 2015 PingCAP, Inc.
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
//
// The Key encoding used in this file is adapted from the TiDB binary string encoding code, hence the license header
// above

package common

import "github.com/squareup/pranadb/errors"

/*
We encode a binary string in the following way:
We split it into chunks of 8 bytes, and after each chunk append a byte which has a value from 1-8 depending on how
many significant bytes there were in the previous chunk. Final chunk is right padded out with zeros to 8 bytes.
*/

const (
	encGroupSize      = 8
	encMarker    byte = 255
	encPad       byte = 0
)

var stringKeyEncodingPads = make([]byte, encGroupSize)

func KeyEncodeString(buff []byte, val string) []byte {

	data := StringToByteSliceZeroCopy(val)
	dLen := len(data)

	for idx := 0; idx <= dLen; idx += encGroupSize {
		remain := dLen - idx
		padCount := 0
		if remain >= encGroupSize {
			buff = append(buff, data[idx:idx+encGroupSize]...)
		} else {
			padCount = encGroupSize - remain
			buff = append(buff, data[idx:]...)
			buff = append(buff, stringKeyEncodingPads[:padCount]...)
		}

		marker := encMarker - byte(padCount)
		buff = append(buff, marker)
	}
	return buff
}

func KeyDecodeString(buffer []byte, offset int) (string, int, error) {
	res := make([]byte, 0, len(buffer))

	if offset != 0 {
		buffer = buffer[offset:]
	}

	for {
		if len(buffer) < encGroupSize+1 {
			return "", 0, errors.New("insufficient bytes to decode value")
		}

		groupBytes := buffer[:encGroupSize+1]

		group := groupBytes[:encGroupSize]
		marker := groupBytes[encGroupSize]

		padCount := encMarker - marker

		if padCount > encGroupSize {
			return "", 0, errors.Errorf("invalid marker byte, group bytes %q", groupBytes)
		}

		realGroupSize := encGroupSize - padCount
		res = append(res, group[:realGroupSize]...)
		buffer = buffer[encGroupSize+1:]
		offset += encGroupSize + 1

		if padCount != 0 {
			var padByte = encPad

			// Check validity of padding bytes.
			for _, v := range group[realGroupSize:] {
				if v != padByte {
					return "", 0, errors.Errorf("invalid padding byte, group bytes %q", groupBytes)
				}
			}
			break
		}
	}
	return ByteSliceToStringZeroCopy(res), offset, nil
}
