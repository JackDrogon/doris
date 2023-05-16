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

package org.apache.doris.binlog;

import org.apache.doris.thrift.TBinlog;

import java.util.TreeSet;

public class TableBinlog {
    private long tableId;
    private TreeSet<TBinlog> binlogs;

    public TableBinlog(long tableId) {
        this.tableId = tableId;
        // binlogs treeset order by commitSeq
        binlogs = new TreeSet<TBinlog>((o1, o2) -> {
            if (o1.getCommitSeq() < o2.getCommitSeq()) {
                return -1;
            } else if (o1.getCommitSeq() > o2.getCommitSeq()) {
                return 1;
            } else {
                return 0;
            }
        });
    }

    public long getTableId() {
        return tableId;
    }

    public void addBinlog(TBinlog binlog) {
        binlogs.add(binlog);
    }

    public TBinlog getBinlog(long commitSeq) {
        // return first binlog whose commitSeq > commitSeq
        TBinlog guard = new TBinlog();
        guard.setCommitSeq(commitSeq);
        return binlogs.higher(guard);
    }    
}
