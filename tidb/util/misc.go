//
// This source code is a modified form of original source from the TiDB project, which has the following copyright header(s):
//

// Copyright 2016 PingCAP, Inc.
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

package util

import (
	"crypto/tls"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	// syntaxErrorPrefix is the common prefix for SQL syntax error in TiDB.
	syntaxErrorPrefix = "You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use"
)

// SyntaxError converts parser error to TiDB's syntax error.
func SyntaxError(err error) error {
	if err == nil {
		return nil
	}
	logutil.BgLogger().Debug("syntax error", zap.Error(err))

	// If the error is already a terror with stack, pass it through.
	if errors.HasStack(err) {
		cause := errors.Cause(err)
		if _, ok := cause.(*terror.Error); ok {
			return err
		}
	}

	return parser.ErrParse.GenWithStackByArgs(syntaxErrorPrefix, err.Error())
}

const (
	// Country is type name for country.
	Country = "C"
	// Organization is type name for organization.
	Organization = "O"
	// OrganizationalUnit is type name for organizational unit.
	OrganizationalUnit = "OU"
	// Locality is type name for locality.
	Locality = "L"
	// Email is type name for email.
	Email = "emailAddress"
	// CommonName is type name for common name.
	CommonName = "CN"
	// Province is type name for province or state.
	Province = "ST"
)

// see go/src/crypto/x509/pkix/pkix.go:attributeTypeNames
var pkixAttributeTypeNames = map[string]string{
	"2.5.4.6":              Country,
	"2.5.4.10":             Organization,
	"2.5.4.11":             OrganizationalUnit,
	"2.5.4.3":              CommonName,
	"2.5.4.5":              "SERIALNUMBER",
	"2.5.4.7":              Locality,
	"2.5.4.8":              Province,
	"2.5.4.9":              "STREET",
	"2.5.4.17":             "POSTALCODE",
	"1.2.840.113549.1.9.1": Email,
}

var pkixTypeNameAttributes = make(map[string]string)

// SANType is enum value for GlobalPrivValue.SANs keys.
type SANType string

var tlsCipherString = map[uint16]string{
	tls.TLS_RSA_WITH_RC4_128_SHA:                "RC4-SHA",
	tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA:           "DES-CBC3-SHA",
	tls.TLS_RSA_WITH_AES_128_CBC_SHA:            "AES128-SHA",
	tls.TLS_RSA_WITH_AES_256_CBC_SHA:            "AES256-SHA",
	tls.TLS_RSA_WITH_AES_128_CBC_SHA256:         "AES128-SHA256",
	tls.TLS_RSA_WITH_AES_128_GCM_SHA256:         "AES128-GCM-SHA256",
	tls.TLS_RSA_WITH_AES_256_GCM_SHA384:         "AES256-GCM-SHA384",
	tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA:        "ECDHE-ECDSA-RC4-SHA",
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA:    "ECDHE-ECDSA-AES128-SHA",
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA:    "ECDHE-ECDSA-AES256-SHA",
	tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA:          "ECDHE-RSA-RC4-SHA",
	tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA:     "ECDHE-RSA-DES-CBC3-SHA",
	tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA:      "ECDHE-RSA-AES128-SHA",
	tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA:      "ECDHE-RSA-AES256-SHA",
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256: "ECDHE-ECDSA-AES128-SHA256",
	tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256:   "ECDHE-RSA-AES128-SHA256",
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:   "ECDHE-RSA-AES128-GCM-SHA256",
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256: "ECDHE-ECDSA-AES128-GCM-SHA256",
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:   "ECDHE-RSA-AES256-GCM-SHA384",
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384: "ECDHE-ECDSA-AES256-GCM-SHA384",
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305:    "ECDHE-RSA-CHACHA20-POLY1305",
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305:  "ECDHE-ECDSA-CHACHA20-POLY1305",
	// TLS 1.3 cipher suites, compatible with mysql using '_'.
	tls.TLS_AES_128_GCM_SHA256:       "TLS_AES_128_GCM_SHA256",
	tls.TLS_AES_256_GCM_SHA384:       "TLS_AES_256_GCM_SHA384",
	tls.TLS_CHACHA20_POLY1305_SHA256: "TLS_CHACHA20_POLY1305_SHA256",
}

// SupportCipher maintains cipher supported by TiDB.
var SupportCipher = make(map[string]struct{}, len(tlsCipherString))

func init() {
	for _, value := range tlsCipherString {
		SupportCipher[value] = struct{}{}
	}
	for key, value := range pkixAttributeTypeNames {
		pkixTypeNameAttributes[value] = key
	}
}

// SequenceSchema is implemented by infoSchema and used by sequence function in expression package.
// Otherwise calling information schema will cause import cycle problem.
type SequenceSchema interface {
	SequenceByName(schema, sequence model.CIStr) (SequenceTable, error)
}

// SequenceTable is implemented by tableCommon,
// and it is specialised in handling sequence operation.
// Otherwise calling table will cause import cycle problem.
type SequenceTable interface {
	GetSequenceID() int64
	GetSequenceNextVal(ctx interface{}, dbName, seqName string) (int64, error)
	SetSequenceVal(ctx interface{}, newVal int64, dbName, seqName string) (int64, bool, error)
}
