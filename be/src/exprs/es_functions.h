// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_QUERY_EXPRS_ES_FUNCTIONS_H
#define BDG_PALO_BE_SRC_QUERY_EXPRS_ES_FUNCTIONS_H

#include "udf/udf.h"

namespace palo {

class Expr;
class OpcodeRegistry;
class TupleRow;

class ESFunctions {
public:
    static void init();

    // used to push down query conditions to es.
    static palo_udf::BooleanVal match(
        palo_udf::FunctionContext* ctx, const palo_udf::StringVal& col, 
            const palo_udf::StringVal& condition);

};

}

#endif
