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

#include "util/json_util.h"

#include <gtest/gtest.h>

#include "common/logging.h"

namespace palo {

class JsonUtilTest : public testing::Test {
public:
    JsonUtilTest() { }
    virtual ~JsonUtilTest() { }
};

TEST_F(JsonUtilTest, success) {
    Status status;

    auto str = to_json(status);

    const char* result = "{\n"
        "    \"status\": \"Success\",\n"
        "    \"msg\": \"OK\"\n}";
    ASSERT_STREQ(result, str.c_str());
}

TEST_F(JsonUtilTest, normal_fail) {
    Status status("so bad");

    auto str = to_json(status);

    const char* result = "{\n"
        "    \"status\": \"Fail\",\n"
        "    \"msg\": \"so bad\"\n}";
    ASSERT_STREQ(result, str.c_str());
}

TEST_F(JsonUtilTest, normal_fail_str) {
    Status status("\"so bad\"");

    auto str = to_json(status);

    // "msg": "\"so bad\""
    const char* result = "{\n"
        "    \"status\": \"Fail\",\n"
        "    \"msg\": \"\\\"so bad\\\"\"\n}";
    LOG(INFO) << "str: " << str;
    ASSERT_STREQ(result, str.c_str());
}

}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
