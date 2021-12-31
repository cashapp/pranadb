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

package driver

import (
	"github.com/pingcap/parser"
)

// To add new features that needs to be downgrade-compatible,
// 1. Define a featureID below and make sure it is unique.
//    For example, `const FeatureIDMyFea = "my_fea"`.
// 2. Register the new featureID in init().
//    Only the registered parser can parse the comment annotated with `my_fea`.
//    Now, the parser treats `/*T![my_fea] what_ever */` and `what_ever` equivalent.
//    In other word, the parser in old-version TiDB will ignores these comments.
// 3. [optional] Add a pattern into FeatureIDPatterns.
//    This is only required if the new feature is contained in DDL,
//    and we want to comment out this part of SQL in binlog.
func init() {
	parser.SpecialCommentsController.Register(string(FeatureIDAutoRandom))
	parser.SpecialCommentsController.Register(string(FeatureIDAutoIDCache))
	parser.SpecialCommentsController.Register(string(FeatureIDAutoRandomBase))
	parser.SpecialCommentsController.Register(string(FeatureClusteredIndex))
}

type featureID string

const (
	// FeatureIDAutoRandom is the `auto_random` feature.
	FeatureIDAutoRandom featureID = "auto_rand"
	// FeatureIDAutoIDCache is the `auto_id_cache` feature.
	FeatureIDAutoIDCache featureID = "auto_id_cache"
	// FeatureIDAutoRandomBase is the `auto_random_base` feature.
	FeatureIDAutoRandomBase featureID = "auto_rand_base"
	// FeatureClusteredIndex is the `clustered_index` feature.
	FeatureClusteredIndex featureID = "clustered_index"
)
