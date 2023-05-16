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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class DBBinlog {
    private long dbId;
    // all binlogs contain table binlogs && create table binlog etc ...
    private TreeSet<TBinlog> allBinlogs;
    // table binlogs
    private Map<Long, TableBinlog> tableBinlogMap;

    public DBBinlog(long dbId) {
        this.dbId = dbId;
        // allBinlogs treeset order by commitSeq
        allBinlogs = new TreeSet<TBinlog>((o1, o2) -> {
            if (o1.getCommitSeq() < o2.getCommitSeq()) {
                return -1;
            } else if (o1.getCommitSeq() > o2.getCommitSeq()) {
                return 1;
            } else {
                return 0;
            }
        });
        tableBinlogMap = new HashMap<Long, TableBinlog>();
    }

    public void addBinlog(List<Long> tableIds, TBinlog binlog) {
        allBinlogs.add(binlog);
        if (tableIds != null) {
            for (long tableId : tableIds) {
                TableBinlog tableBinlog = tableBinlogMap.get(tableId);
                if (tableBinlog == null) {
                    tableBinlog = new TableBinlog(tableId);
                    tableBinlogMap.put(tableId, tableBinlog);
                }
                tableBinlog.addBinlog(binlog);
            }
        }
    }

    public long getDbId() {
        return dbId;
    }

    public TBinlog getBinlog(long tableId, long commitSeq) {
        if (tableId >= 0) {
            TableBinlog tableBinlog = tableBinlogMap.get(tableId);
            if (tableBinlog == null) {
                return null;
            }
            return tableBinlog.getBinlog(commitSeq);
        }

        // get first binlog from internal allBinlogs whose commitSeq  > commitSeq
        TBinlog guard = new TBinlog();
        guard.setCommitSeq(commitSeq);
        return allBinlogs.higher(guard);
    }
}
