// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

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

#pragma once

#include <atomic>

#include <google/protobuf/stubs/common.h>

#include "service/brpc.h"
 
namespace palo {

template<typename T>
class RefCountClosure : public google::protobuf::Closure {
public:
    RefCountClosure() : _refs(0) { }
    ~RefCountClosure() { }

    void ref() { _refs.fetch_add(1); }

    // If unref() returns true, this object should be delete
    bool unref() { return _refs.fetch_sub(1) == 1; }

    void Run() override {
        if (unref()) {
            delete this;
        }
    }

    void join() {
        brpc::Join(cntl.call_id());
    }

    brpc::Controller cntl;
    T result;
private:
    std::atomic<int> _refs;
};

}
