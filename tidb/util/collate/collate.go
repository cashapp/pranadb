//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2020 PingCAP, Inc.
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

package collate

import (
	"github.com/squareup/pranadb/tidb"
	"sync/atomic"

	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	newCollatorMap      map[string]Collator
	newCollatorIDMap    map[int]Collator
	newCollationEnabled int32

	// binCollatorInstance is a singleton used for all collations when newCollationEnabled is false.
	binCollatorInstance = &binCollator{}
)

const (
	// DefaultLen is set for datum if the string datum don't know its length.
	DefaultLen = 0
	// first byte of a 2-byte encoding starts 110 and carries 5 bits of data
	b2Mask = 0x1F // 0001 1111
	// first byte of a 3-byte encoding starts 1110 and carries 4 bits of data
	b3Mask = 0x0F // 0000 1111
	// first byte of a 4-byte encoding starts 11110 and carries 3 bits of data
	b4Mask = 0x07 // 0000 0111
	// non-first bytes start 10 and carry 6 bits of data
	mbMask = 0x3F // 0011 1111
)

// Collator provides functionality for comparing strings for a given
// collation order.
type Collator interface {
	// Compare returns an integer comparing the two strings. The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
	Compare(a, b string) int
	// Key returns the collate key for str. If the collation is padding, make sure the PadLen >= len(rune[]str) in opt.
	Key(str string) []byte
	// Pattern get a collation-aware WildcardPattern.
	Pattern() WildcardPattern
}

// WildcardPattern is the interface used for wildcard pattern match.
type WildcardPattern interface {
	// Compile compiles the patternStr with specified escape character.
	Compile(patternStr string, escape byte)
	// DoMatch tries to match the str with compiled pattern, `Compile()` must be called before calling it.
	DoMatch(str string) bool
}

// NewCollationEnabled returns if the new collations are enabled.
func NewCollationEnabled() bool {
	return atomic.LoadInt32(&newCollationEnabled) == 1
}

// CompatibleCollate checks whether the two collate are the same.
func CompatibleCollate(collate1, collate2 string) bool {
	if (collate1 == "utf8mb4_general_ci" || collate1 == "utf8_general_ci") && (collate2 == "utf8mb4_general_ci" || collate2 == "utf8_general_ci") {
		return true
	} else if (collate1 == "utf8mb4_bin" || collate1 == "utf8_bin") && (collate2 == "utf8mb4_bin" || collate2 == "utf8_bin") {
		return true
	} else if (collate1 == "utf8mb4_unicode_ci" || collate1 == "utf8_unicode_ci") && (collate2 == "utf8mb4_unicode_ci" || collate2 == "utf8_unicode_ci") {
		return true
	} else {
		return collate1 == collate2
	}
}

// GetCollator get the collator according to collate, it will return the binary collator if the corresponding collator doesn't exist.
func GetCollator(collate string) Collator {
	if atomic.LoadInt32(&newCollationEnabled) == 1 {
		ctor, ok := newCollatorMap[collate]
		if !ok {
			logutil.BgLogger().Warn(
				"Unable to get collator by name, use binCollator instead.",
				zap.String("name", collate),
				zap.Stack("stack"))
			return newCollatorMap["utf8mb4_bin"]
		}
		return ctor
	}
	return binCollatorInstance
}

// CollationName2ID return the collation id by the given name.
// If the name is not found in the map, the default collation id is returned
func CollationName2ID(name string) int {
	if coll, err := charset.GetCollationByName(name); err == nil {
		return coll.ID
	}
	return mysql.DefaultCollationID
}

// GetCollationByName wraps charset.GetCollationByName, it checks the collation.
func GetCollationByName(name string) (coll *charset.Collation, err error) {
	if coll, err = charset.GetCollationByName(name); err != nil {
		return nil, errors.Trace(err)
	}
	if atomic.LoadInt32(&newCollationEnabled) == 1 {
		if _, ok := newCollatorIDMap[coll.ID]; !ok {
			return nil, tidb.ErrUnsupportedCollation.GenWithStackByArgs(name)
		}
	}
	return
}

func truncateTailingSpace(str string) string {
	byteLen := len(str)
	i := byteLen - 1
	for ; i >= 0; i-- {
		if str[i] != ' ' {
			break
		}
	}
	str = str[:i+1]
	return str
}

func sign(i int) int {
	if i < 0 {
		return -1
	} else if i > 0 {
		return 1
	}
	return 0
}

// decode rune by hand
func decodeRune(s string, si int) (r rune, newIndex int) {
	switch b := s[si]; {
	case b < 0x80:
		r = rune(b)
		newIndex = si + 1
	case b < 0xE0:
		r = rune(b&b2Mask)<<6 |
			rune(s[1+si]&mbMask)
		newIndex = si + 2
	case b < 0xF0:
		r = rune(b&b3Mask)<<12 |
			rune(s[si+1]&mbMask)<<6 |
			rune(s[si+2]&mbMask)
		newIndex = si + 3
	default:
		r = rune(b&b4Mask)<<18 |
			rune(s[si+1]&mbMask)<<12 |
			rune(s[si+2]&mbMask)<<6 |
			rune(s[si+3]&mbMask)
		newIndex = si + 4
	}
	return
}

// IsCICollation returns if the collation is case-sensitive
func IsCICollation(collate string) bool {
	return collate == "utf8_general_ci" || collate == "utf8mb4_general_ci" ||
		collate == "utf8_unicode_ci" || collate == "utf8mb4_unicode_ci"
}

func init() {
	newCollatorMap = make(map[string]Collator)
	newCollatorIDMap = make(map[int]Collator)

	newCollatorMap["binary"] = &binCollator{}
	newCollatorIDMap[CollationName2ID("binary")] = &binCollator{}
	newCollatorMap["ascii_bin"] = &binPaddingCollator{}
	newCollatorIDMap[CollationName2ID("ascii_bin")] = &binPaddingCollator{}
	newCollatorMap["latin1_bin"] = &binPaddingCollator{}
	newCollatorIDMap[CollationName2ID("latin1_bin")] = &binPaddingCollator{}
	newCollatorMap["utf8mb4_bin"] = &binPaddingCollator{}
	newCollatorIDMap[CollationName2ID("utf8mb4_bin")] = &binPaddingCollator{}
	newCollatorMap["utf8_bin"] = &binPaddingCollator{}
	newCollatorIDMap[CollationName2ID("utf8_bin")] = &binPaddingCollator{}
	newCollatorMap["utf8mb4_general_ci"] = &generalCICollator{}
	newCollatorIDMap[CollationName2ID("utf8mb4_general_ci")] = &generalCICollator{}
	newCollatorMap["utf8_general_ci"] = &generalCICollator{}
	newCollatorIDMap[CollationName2ID("utf8_general_ci")] = &generalCICollator{}
	newCollatorMap["utf8mb4_unicode_ci"] = &unicodeCICollator{}
	newCollatorIDMap[CollationName2ID("utf8mb4_unicode_ci")] = &unicodeCICollator{}
	newCollatorMap["utf8_unicode_ci"] = &unicodeCICollator{}
	newCollatorIDMap[CollationName2ID("utf8_unicode_ci")] = &unicodeCICollator{}
	newCollatorMap["utf8mb4_zh_pinyin_tidb_as_cs"] = &zhPinyinTiDBASCSCollator{}
	newCollatorIDMap[CollationName2ID("utf8mb4_zh_pinyin_tidb_as_cs")] = &zhPinyinTiDBASCSCollator{}
}
