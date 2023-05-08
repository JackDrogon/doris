// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/action/download_binlog_action.h"

#include <fmt/format.h>

#include <stdexcept>
#include <string_view>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/utils.h"
#include "io/fs/local_file_system.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "runtime/exec_env.h"

namespace doris {

namespace {
const std::string FILE_PARAMETER = "file";
const std::string kTokenParameter = "token";
const std::string kTabletIdParameter = "tablet_id";
const std::string kBinlogVersion = "binlog_version";

// get http param, if no value throw exception
const auto& get_http_param(HttpRequest* req, const std::string& param_name) {
    const auto& param = req->param(param_name);
    if (param.empty()) {
        auto error_msg = fmt::format("parameter {} not specified in url.", param_name);
        throw std::runtime_error(error_msg);
    }
    return param;
}

std::vector<std::string> get_binlog_filepath(std::string_view tablet_id_str,
                                             std::string_view binlog_version) {
    int64_t tablet_id = std::atoll(tablet_id_str.data());
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        auto error = fmt::format("tablet is not exist, tablet_id={}", tablet_id);
        LOG(WARNING) << error;
        throw std::runtime_error(error);
    }

    return tablet->get_binlog_filepath(binlog_version);
}
} // namespace

DownloadBinlogAction::DownloadBinlogAction(ExecEnv* exec_env) : _exec_env(exec_env) {}

void DownloadBinlogAction::handle(HttpRequest* req) {
    VLOG_CRITICAL << "accept one download binlog request " << req->debug_string();

    // Step 1: check token
    Status status;
    if (config::enable_token_check) {
        // FIXME(Drogon): support check token
        // status = _check_token(req);
        if (!status.ok()) {
            HttpChannel::send_reply(req, status.to_string());
            return;
        }
    }

    // Step 2: get download file path
    // Get 'file' parameter, then assembly file absolute path
    std::vector<std::string> binlog_file_path;
    try {
        const auto& tablet_id = get_http_param(req, kTabletIdParameter);
        const auto& binlog_version = get_http_param(req, kBinlogVersion);

        binlog_file_path = get_binlog_filepath(tablet_id, binlog_version);
    } catch (const std::exception& e) {
        HttpChannel::send_reply(req, e.what());
        LOG(WARNING) << "get download file path failed, error: " << e.what();
        return;
    }

    // Step 3: handle download
    // check file exists
    // bool exists = false;
    // status = io::global_local_filesystem()->exists(binlog_file_path, &exists);
    // if (!status.ok()) {
    //     HttpChannel::send_reply(req, status.to_string());
    //     LOG(WARNING) << "check file exists failed, error: " << status.to_string();
    //     return;
    // }
    // if (!exists) {
    //     HttpChannel::send_reply(req, "file not exist.");
    //     LOG(WARNING) << "file not exist, file path: " << binlog_file_path;
    //     return;
    // }
    do_file_response(binlog_file_path[0], req);

    VLOG_CRITICAL << "deal with download binlog file request finished! ";
}

Status DownloadBinlogAction::_check_token(HttpRequest* req) {
    const std::string& token_str = req->param(kTokenParameter);
    if (token_str.empty()) {
        return Status::InternalError("token is not specified.");
    }

    if (token_str != _exec_env->token()) {
        return Status::InternalError("invalid token.");
    }

    return Status::OK();
}

} // end namespace doris
